-module(eta_log).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
A deterministic event log, and the thing that makes a failing run readable.

A trace is a *scheduler* artifact. `{step, 8}` records which process was chosen
and says nothing about what that process did, what it read, or what state
anything was in — and the ids are anonymous positions assigned in registration
order. Turning one into an explanation means reconstructing every mailbox state
by hand, which is a decoder ring rather than a repro.

The schedule tells you what the scheduler did. This is where you **define the
narrative of your system** — nothing here generates a story, you choose what is
worth recording and where, and the quality of a failure report follows from
those choices. `eta_run` writes its own decisions into the same log, so there is
one ordered timeline rather than two that have to be reconciled:

```
  12  $eta        {step,8}
  13  reader-6    query_sent
  14  $eta        {step,1}
  15  r2          {applied_write,{1,1}}
  16  r2          {answered_read,{1,1}}
  17  $eta        {step,8}
  18  reader-6    {quorum_max,{1,1}}
```

## What a system under test calls

Not these functions directly. Include the header and use the macros:

```erlang
-include_lib("eta/include/eta.hrl").

init(Index) ->
    ok = ?ETA_LABEL({replica, Index}),
    {ok, #st{index = Index}}.

handle_cast({read, Ref, From}, St = #st{ts = Ts}) ->
    _ = ?ETA_LOG({answered_read, Ts}),
    ...
```

Under a release build the macros expand to nothing at all, so the module has no
relationship to this library: no call, no table lookup, nothing to pay for and
nothing for a release to carry. `log/1` below is inert when no collection is
running, but inert is not free and still requires `eta` to be present. The
macros are what make it safe to leave instrumentation in the source of something
that ships. Same shape as EUnit's `TEST`.

The functions below are the *driver* side — collection, correlation and
presentation — and are called from tests and shells rather than from a module
that ships.

A harness may use either. It never ships, so the macros buy it nothing, and
calling `label/1` and `log/1` directly means a harness can be written in Elixir:
the macros are Erlang, and so is `eta_transform`, so the harness is the one part
of a simulated system with that freedom.

## Three phases, like `fprof`

```erlang
eta_run:run(my_harness, #{seed => 3}),   %% 1. collect
eta_log:profile(),                       %% 2. correlate
eta_log:analyze().                       %% 3. present
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

`eta_run`'s own calls happen in the driver process, which the scheduler does not
own, so they are safe by construction. Calls from inside a scheduled process are
safe too, for a reason worth stating: a step ends when a process **blocks in a
receive**, and an ETS operation never blocks. Observability that changed the
schedule would be worse than none, since every failure you then investigated
would be a different failure from the one you set out to investigate.

*This documentation is LLM-generated. See the AI disclosure in `README.md`.*
""".
-endif.

-export([
    trace/1,
    running/0,
    stop/0,
    label/1,
    self_labels/0,
    log/1,
    seq/0,
    events/0,
    register_labels/1,
    profile/0,
    analyze/0,
    analyze/1
]).

-export_type([event/0, entry/0]).

-define(TAB, eta_log_events).
-define(LABEL, '$eta_log_label').

%% Key prefix for the pid-to-label rows `label/1` writes. A tuple, so it cannot
%% collide with the integer sequence numbers the events are keyed on.
-define(SELF, '$self').

%% Reserved label for entries written by the framework rather than by the system
%% under test. Rendered as `$eta` so a reader can tell the driver's decisions
%% from the system's behaviour at a glance.
-define(DRIVER, '$eta').

-type event() :: {non_neg_integer(), term(), term()}.
-type entry() :: #{
    seq := non_neg_integer(),
    label := term(),
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
Starts or stops collection. `eta_run` calls this for you; use it directly when
driving `eta_sched` by hand.

`stamps` is collection turned off **without turning the clock off**: `log/1`
still returns a real, increasing sequence number, it just records nothing. That
is what `eta_run`'s `log => false` uses, and the distinction matters more than it
sounds.

A harness is told to stamp its operations from `log/1`, so that a violation
reporting `earlier: {8, 11, ...}` indexes into the narrative. If switching
collection off made those stamps 0, every one of them would compare equal, and
an invariant phrased as "this finished before that started" would quietly stop
holding — a run that checks nothing, reported as a clean pass. Keeping the
counter costs one ETS row.

Starting either way discards whatever was collected before, the same way
`eta_time:start/0` resets a running clock.
""".
-endif.
-spec trace(start | stamps | stop) -> ok.
trace(start) ->
    fresh(true);
trace(stamps) ->
    fresh(false);
trace(stop) ->
    stop().

fresh(Record) ->
    %% See `eta_time`'s `claim/3`: the table is the lock, and taking it from a
    %% live owner would silently discard a run in progress.
    case ets:info(?TAB, owner) of
        undefined ->
            ok;
        Self when Self =:= self() ->
            ok;
        Other ->
            error(
                {eta_log,
                    {log_in_use, Other, <<
                        "one per VM; keep runs serial (`async: false`) and do "
                        "not drive two at once"
                    >>}}
            )
    end,
    stop(),
    ?TAB = ets:new(?TAB, [named_table, public, ordered_set]),
    true = ets:insert(?TAB, [{'$seq', 0}, {'$record', Record}]),
    ok.

-if(?DOCATTRS).
-doc """
Whether a log exists, whether or not it is recording. `log/1` returns 0 only
when this is false.
""".
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

Call `?ETA_LABEL(Label)` rather than this, so a release build strips it. See the
module doc.

Kept in the process dictionary, so logging costs nothing at the call sites and
no API has to carry a label around. Call it once, wherever the process starts:
in a `gen_server`'s `init/1`, or at the top of whatever an operation spawns.

## A label is permanent

The first call wins. A second call with a different label is ignored and
warned about, because a label is a property of a *process*, not of its current
state. A replica that becomes leader has not become a different process, and
saying so would rewrite the name on every step it took beforehand. Log the
transition as an event instead — that is what events are for, and it keeps the
before and the after both readable.

## It also names the step lines

Registering here is what lets a `{step, Id}` line carry the same name as the
events underneath it. The process cannot know its own scheduler id — it is
labelled in `init/1`, before `eta_sched` has assigned one — so the pid is
recorded and `eta_run` joins it against the scheduler's id-to-pid map at
teardown.

**That join is why the self-reported name wins** over `eta_harness`'s optional
`labels/1`. Two sources naming one process is how the same process ends up under
2 names in one report, and one of those sources is the process itself. Use
`labels/1` for the processes that never labelled themselves: something from a
library you do not own, or a module you kept the header out of.
""".
-endif.
-spec label(term()) -> ok.
label(Label) ->
    case get(?LABEL) of
        undefined ->
            _ = put(?LABEL, Label),
            remember(Label);
        Label ->
            %% Idempotent, and re-registering matters: a process that outlives a
            %% `trace/1` reset would otherwise be missing from the new table.
            remember(Label);
        Existing ->
            logger:warning(
                "eta_log: ~p is already labelled ~p, ignoring the relabel to ~p. "
                "A label is a permanent property of a process; represent a change "
                "of state as a logged event instead.",
                [self(), Existing, Label]
            ),
            ok
    end.

remember(Label) ->
    case ets:whereis(?TAB) of
        undefined ->
            ok;
        _ ->
            true = ets:insert(?TAB, {{?SELF, self()}, Label}),
            ok
    end.

-if(?DOCATTRS).
-doc """
Every label a process gave itself this run, as `#{pid() => term()}`.

`eta_run` joins this against the scheduler's id-to-pid map; a harness has no
reason to call it.
""".
-endif.
-spec self_labels() -> #{pid() => term()}.
self_labels() ->
    try ets:match_object(?TAB, {{?SELF, '_'}, '_'}) of
        Rows -> maps:from_list([{Pid, Label} || {{_, Pid}, Label} <- Rows])
    catch
        error:badarg -> #{}
    end.

-if(?DOCATTRS).
-doc """
Records an event and returns its sequence number.

Call `?ETA_LOG(Event)` rather than this from a module that ships; see the module
doc. This returns 0 when there is no log at all, and the macro returns 0 when the
module was not built for simulation, so the 2 are consistent. Under
`trace(stamps)` the number is real and only the recording is skipped.

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
            case ets:lookup_element(?TAB, '$record', 2) of
                true -> true = ets:insert(?TAB, {Seq, get(?LABEL), What});
                false -> ok
            end,
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
        [E || E = {Seq, _Label, _What} <- ets:tab2list(?TAB), is_integer(Seq)]
    catch
        error:badarg -> []
    end.

-if(?DOCATTRS).
-doc """
Records the id-to-name mapping for this run. Called by `eta_run` at teardown,
once ids can finally be joined to names.

The plural of `label/1` in subject, not in object: `label/1` is a process naming
*itself*, this is the finished map for the whole run — every self-reported label
plus whatever `c:eta_harness:labels/1` added for the processes that never reported
one.

Stored beside the events rather than resolved into them, so the raw log stays a
record of facts and naming stays a presentation concern.
""".
-endif.
-spec register_labels(#{non_neg_integer() => term()}) -> ok.
register_labels(Map) ->
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

The last part matters more than it sounds. `eta_run` releases every suspended
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
            fun({Seq, Label, What}, {Acc, Step, Sim}) ->
                {Step1, Sim1} = advance(Label, What, Step, Sim),
                Entry = #{
                    seq => Seq,
                    label => Label,
                    name => name(Label, What, Labels),
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
%% simulation and everything after it is teardown. Dispatching on the label
%% rather than the payload means a system that logs its own `{step, _}` term
%% cannot be mistaken for the driver.
advance(?DRIVER, {step, Id}, _Step, Sim) -> {Id, Sim};
advance(?DRIVER, released, Step, _Sim) -> {Step, false};
advance(_Label, _What, Step, Sim) -> {Step, Sim}.

%% A driver step is rendered under the name of the process it chose, which is
%% what makes the timeline read as one story rather than two interleaved ones.
name(?DRIVER, {step, Id}, Labels) -> label_for(Id, Labels);
name(?DRIVER, _What, _Labels) -> <<"$eta">>;
name(undefined, _What, _Labels) -> <<"?">>;
name(Label, _What, _Labels) -> format(Label).

label_for(Id, Labels) ->
    case maps:get(Id, Labels, undefined) of
        undefined -> <<"p", (integer_to_binary(Id))/binary>>;
        Name -> format(Name)
    end.

%% Names are ordinary terms, from `label/1` or from a harness's `labels/1`, and
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
  eta_log:analyze(#{until => Finish}).
  ```

- `teardown` (`false`) — include events logged after the scheduler released the
  system. Off by default because they ran on the real scheduler and are not part
  of the run.
- `driver` (`true`) — include `eta_run`'s own `step`, `op` and `clock` entries.
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
     || E = #{seq := Seq, label := Label, simulated := Sim} <- profile(),
        Until =:= infinity orelse Seq =< Until,
        Teardown orelse Sim,
        Driver orelse Label =/= ?DRIVER
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
