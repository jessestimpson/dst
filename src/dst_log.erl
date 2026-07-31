-module(dst_log).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
A deterministic event log, and the thing that makes a failing run readable.

A trace is a *scheduler* artifact. `{step, 8}` records which process was chosen
and says nothing about what that process did, what it read, or what state
anything was in — and the ids are anonymous positions assigned in registration
order. Turning one into an explanation means reconstructing every mailbox state
by hand, which is a decoder ring rather than a repro.

The schedule tells you what the scheduler did. This tells you what your system
did. You need both, and `dst_run` writes its own decisions into the same log
your system writes to, so there is one ordered timeline rather than two that
have to be reconciled:

```
  12  $dst        {step,8}
  13  reader-6    query_sent
  14  $dst        {step,1}
  15  r2          {applied_write,{1,1}}
  16  r2          {answered_read,{1,1}}
  17  $dst        {step,8}
  18  reader-6    {quorum_max,{1,1}}
```

## What a system under test calls

Not these functions directly. Include the header and use the macros:

```erlang
-include_lib("dst/include/dst.hrl").

init(Index) ->
    ok = ?DST_ROLE({replica, Index}),
    {ok, #st{index = Index}}.

handle_cast({read, Ref, From}, St = #st{ts = Ts}) ->
    _ = ?DST_LOG({answered_read, Ts}),
    ...
```

`dst` is a test-only dependency, so a shipped module holding a bare
`dst_log:log/1` call is an `undef` waiting for the first production run. Under a
release build the macros expand to nothing and the module has no relationship to
this library at all, which is what makes it safe to leave instrumentation in the
source of something that ships. Same shape as EUnit's `TEST`.

The functions below are the *driver* side — collection, correlation and
presentation — and are called from tests and shells, never from a module that
ships.

## Three phases, like `fprof`

```erlang
dst_run:run(my_harness, #{seed => 3}),   %% 1. collect
dst_log:profile(),                       %% 2. correlate
dst_log:analyze().                       %% 3. present
```

Collection is on by default during a run (`log => false` disables it) and can be
driven by hand with `trace/1` outside one.

The middle phase is not ceremony. Process labels **cannot** be resolved while a
run is in progress: ids are handed out as processes register, and only the
harness can name them, with its own state in hand. So the raw log stays dumb —
ids and terms, no callbacks on the hot path — and `profile/0` does the
correlation afterwards, attaching each event to the step it happened inside and
finding the boundary where the scheduler handed the system back.

## The data outlives the run

Deliberately, and this is the mistake worth not repeating. A log deleted during
teardown is unreadable exactly when you want to read it. Like `fprof`'s trace
file, what you collect survives until the next collection starts or you call
`stop/0`.

## Ordering, and why the counter is shared

Every event takes its sequence number from one `ets:update_counter`, and
anything the harness stamps must take its timestamps from the same place. Two
counters mean a violation reporting `earlier: {3, 4, ...}` that indexes into
nothing. One counter means the violation **points into the narrative** — read
the numbers, jump to those lines.

## It cannot perturb the schedule

`dst_run`'s own calls happen in the driver process, which the scheduler does not
own, so they are safe by construction. Calls from inside a scheduled process are
safe too, for a reason worth stating: a step ends when a process **blocks in a
receive**, and an ETS operation never blocks. Observability that changed the
schedule would be worse than none, since every failure you then investigated
would be a different failure from the one you set out to investigate.
""".
-endif.

-export([
    trace/1,
    running/0,
    stop/0,
    role/1,
    log/1,
    seq/0,
    events/0,
    labels/1,
    profile/0,
    analyze/0,
    analyze/1
]).

-export_type([event/0, entry/0]).

-define(TAB, dst_log_events).
-define(ROLE, '$dst_log_role').

%% Reserved role for entries written by the framework rather than by the system
%% under test. Rendered as `$dst` so a reader can tell the driver's decisions
%% from the system's behaviour at a glance.
-define(DRIVER, '$dst').

-type event() :: {non_neg_integer(), term(), term()}.
-type entry() :: #{
    seq := non_neg_integer(),
    role := term(),
    name := binary(),
    what := term(),
    step := non_neg_integer() | undefined,
    simulated := boolean()
}.

%% ---------------------------------------------------------------------------
%% Collection
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc """
Starts or stops collection. `dst_run` calls this for you unless you pass
`log => false`; use it directly when driving `dst_sched` by hand.

Starting discards whatever was collected before, the same way
`dst_time:start/0` resets a running clock.
""".
-endif.
-spec trace(start | stop) -> ok.
trace(start) ->
    stop(),
    ?TAB = ets:new(?TAB, [named_table, public, ordered_set]),
    true = ets:insert(?TAB, {'$seq', 0}),
    ok;
trace(stop) ->
    stop().

-if(?DOCATTRS).
-doc "Whether collection is active. When false, `log/1` is a no-op returning 0.".
-endif.
-spec running() -> boolean().
running() ->
    ets:info(?TAB, name) =/= undefined.

-if(?DOCATTRS).
-doc "Discards the collected log. Safe to call when nothing is running.".
-endif.
-spec stop() -> ok.
stop() ->
    try
        ets:delete(?TAB)
    catch
        error:badarg -> ok
    end,
    ok.

-if(?DOCATTRS).
-doc """
Names the calling process for the log.

Call `?DST_ROLE(Role)` rather than this, so a release build strips it. See the
module doc.

Kept in the process dictionary, so logging costs nothing at the call sites and
no API has to carry a label around. Call it once, wherever the process starts:
in a `gen_server`'s `init/1`, or at the top of whatever an operation spawns.

This is the *self-reported* name. `dst_harness`'s optional `label/2` is the
harness naming a process from outside, and it wins when both exist.
""".
-endif.
-spec role(term()) -> ok.
role(Role) ->
    _ = put(?ROLE, Role),
    ok.

-if(?DOCATTRS).
-doc """
Records an event and returns its sequence number.

Call `?DST_LOG(Event)` rather than this from a module that ships; see the module
doc. This returns 0 when collection is not running, and the macro returns 0 when
the module was not built for simulation at all, so the 2 are consistent.

**Stamp your harness's own timestamps from this return value.** An invariant
that reports "read A finished at 11, read B started at 12" is only useful if 11
and 12 name lines in this log.
""".
-endif.
-spec log(term()) -> non_neg_integer().
log(What) ->
    case ets:whereis(?TAB) of
        undefined ->
            0;
        _ ->
            Seq = ets:update_counter(?TAB, '$seq', 1),
            true = ets:insert(?TAB, {Seq, get(?ROLE), What}),
            Seq
    end.

-if(?DOCATTRS).
-doc "The sequence number of the last event recorded, or 0.".
-endif.
-spec seq() -> non_neg_integer().
seq() ->
    try ets:lookup_element(?TAB, '$seq', 2) of
        N -> N
    catch
        error:badarg -> 0
    end.

-if(?DOCATTRS).
-doc "The raw log, oldest first. `profile/0` is usually what you want.".
-endif.
-spec events() -> [event()].
events() ->
    try
        [E || E = {Seq, _Role, _What} <- ets:tab2list(?TAB), is_integer(Seq)]
    catch
        error:badarg -> []
    end.

-if(?DOCATTRS).
-doc """
Records the id-to-name mapping for this run. Called by `dst_run` at teardown,
once the harness can finally name the processes it created.

Stored beside the events rather than resolved into them, so the raw log stays a
record of facts and naming stays a presentation concern.
""".
-endif.
-spec labels(#{non_neg_integer() => term()}) -> ok.
labels(Map) ->
    case running() of
        false ->
            ok;
        true ->
            true = ets:insert(?TAB, {'$labels', Map}),
            ok
    end.

%% ---------------------------------------------------------------------------
%% Correlation
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc """
Correlates the raw log into a timeline: resolves ids to names, attaches every
event to the step it happened inside, and marks where the simulation ended.

The last part matters more than it sounds. `dst_run` releases every suspended
process before tearing the system down, so anything logged after that point ran
on the *real* scheduler and is not part of the run. It is real, it is
nondeterministic, and a reader who does not know that will try to make sense of
it.
""".
-endif.
-spec profile() -> [entry()].
profile() ->
    Labels = stored_labels(),
    {Entries, _Step, _Sim} =
        lists:foldl(
            fun({Seq, Role, What}, {Acc, Step, Sim}) ->
                {Step1, Sim1} = advance(Role, What, Step, Sim),
                Entry = #{
                    seq => Seq,
                    role => Role,
                    name => name(Role, What, Labels),
                    what => What,
                    step => Step1,
                    simulated => Sim1
                },
                {[Entry | Acc], Step1, Sim1}
            end,
            {[], undefined, true},
            events()
        ),
    lists:reverse(Entries).

%% A driver `step` entry opens a new step; the release marker closes the
%% simulation and everything after it is teardown. Dispatching on the role
%% rather than the payload means a system that logs its own `{step, _}` term
%% cannot be mistaken for the driver.
advance(?DRIVER, {step, Id}, _Step, Sim) -> {Id, Sim};
advance(?DRIVER, released, Step, _Sim) -> {Step, false};
advance(_Role, _What, Step, Sim) -> {Step, Sim}.

%% A driver step is rendered under the name of the process it chose, which is
%% what makes the timeline read as one story rather than two interleaved ones.
name(?DRIVER, {step, Id}, Labels) -> label_for(Id, Labels);
name(?DRIVER, _What, _Labels) -> <<"$dst">>;
name(undefined, _What, _Labels) -> <<"?">>;
name(Role, _What, _Labels) -> format(Role).

label_for(Id, Labels) ->
    case maps:get(Id, Labels, undefined) of
        undefined -> <<"p", (integer_to_binary(Id))/binary>>;
        Name -> format(Name)
    end.

%% Names are ordinary terms, from `role/1` or from a harness's `label/2`, and
%% rendering them is this module's job rather than the caller's. A harness
%% returning `{participant, 2}` is saying something clearer than one returning
%% a preformatted string, so the tuple form gets first-class treatment and
%% everything else falls back to `~p`.
%%
%% Always a binary out. A charlist would be indistinguishable from a list of
%% integers to anything downstream, and every caller here is either Elixir or
%% modern Erlang.
-spec format(term()) -> binary().
format(Atom) when is_atom(Atom) ->
    atom_to_binary(Atom, utf8);
format(Bin) when is_binary(Bin) ->
    Bin;
format({Kind, N}) when is_atom(Kind), is_integer(N) ->
    <<(atom_to_binary(Kind, utf8))/binary, "-", (integer_to_binary(N))/binary>>;
format(List) when is_list(List) ->
    case io_lib:printable_list(List) of
        true -> unicode:characters_to_binary(List);
        false -> iolist_to_binary(io_lib:format("~p", [List]))
    end;
format(Other) ->
    iolist_to_binary(io_lib:format("~p", [Other])).

stored_labels() ->
    try ets:lookup(?TAB, '$labels') of
        [{'$labels', Map}] -> Map;
        [] -> #{}
    catch
        error:badarg -> #{}
    end.

%% ---------------------------------------------------------------------------
%% Presentation
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc "Renders the profiled timeline to the terminal. See `analyze/1`.".
-endif.
-spec analyze() -> ok.
analyze() ->
    analyze(#{}).

-if(?DOCATTRS).
-doc """
Renders the profiled timeline.

Options:

- `dest` (`[]`) — `[]` for the terminal, or a filename.
- `until` (`infinity`) — stop at this sequence number. **Pass the finish stamp
  from a violation** and the output ends exactly where the story does:

  ```erlang
  #{outcome := {violation, #{later := {_Start, Finish, _}}}} = Result,
  dst_log:analyze(#{until => Finish}).
  ```

- `teardown` (`false`) — include events logged after the scheduler released the
  system. Off by default because they ran on the real scheduler and are not part
  of the run.
- `driver` (`true`) — include `dst_run`'s own `step`, `op` and `clock` entries.
  Turn it off for a pure account of what the system did.
""".
-endif.
-spec analyze(map()) -> ok.
analyze(Opts) ->
    Until = maps:get(until, Opts, infinity),
    Teardown = maps:get(teardown, Opts, false),
    Driver = maps:get(driver, Opts, true),
    Selected = [
        E
     || E = #{seq := Seq, role := Role, simulated := Sim} <- profile(),
        Until =:= infinity orelse Seq =< Until,
        Teardown orelse Sim,
        Driver orelse Role =/= ?DRIVER
    ],
    Text = [render(E) || E <- Selected],
    case maps:get(dest, Opts, []) of
        [] ->
            io:put_chars(Text);
        File ->
            ok = file:write_file(File, Text)
    end.

render(#{seq := Seq, name := Name, what := What}) ->
    iolist_to_binary(io_lib:format("~5w  ~-14ts ~tp~n", [Seq, Name, What])).
