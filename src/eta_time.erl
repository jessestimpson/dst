-module(eta_time).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
A virtual clock and timer wheel — Phase 1 of the DST framework
(design: `docs/design.md`).

Drop-in replacements for the `erlang` timer and clock BIFs, backed by a clock that
only moves when the driver moves it. `eta_transform` rewrites a module's calls to
point here; nothing else about the module changes.

Two things follow, and the second is the one that pays for the phase:

- **Determinism.** A timeout fires at a point the schedule chooses, not whenever
  the machine happens to get there, so a run replays identically.
- **Speed.** Waiting out a one-second timeout costs nothing: with no process
  runnable, the driver jumps the clock straight to the next deadline. A run that
  spends most of its wall clock waiting collapses to the work it actually does.

## No process of its own

State lives in ETS, and every operation is a direct table access from whichever
process is calling. That is deliberate. A `gen_server` here would make every
`send_after/3` a blocking call, and a blocking call *is* quiescence as far as
`eta_sched` is concerned — so each timer set would end a step, cost a scheduling
choice, and pollute the recorded schedule with the framework's own traffic.

Concurrent access is safe because `eta_sched` guarantees only one process under
test runs at a time, with the driver the only other participant.

## Inert unless started

Every function falls back to its real `erlang` counterpart when no clock is
running. A module compiled with the transform therefore behaves exactly as it did
before outside a simulation, which is what makes the transform safe to leave
enabled in a build that also runs ordinary tests.

## The event loop

The driver alternates between stepping processes and advancing time:

```erlang
eta_sched:run(Sched, MaxSteps, fun eta_time:advance_to_next/0)
```

`advance_to_next/0` is called only when nothing is runnable, and moves the clock
to the earliest pending deadline — so time advances in jumps between events, never
in ticks, and never while work remains to be done at the current instant.

## Single instance

The tables are named, so one clock exists per VM. Simulations must not run
concurrently in the same node.

*This documentation is LLM-generated. See the AI disclosure in `README.md`.*
""".
-endif.

-export([
    start/0,
    start/1,
    stop/0,
    running/0
]).

%% Clock reads — the erlang/3 counterparts eta_transform rewrites to.
-export([
    monotonic_time/0,
    monotonic_time/1,
    system_time/0,
    system_time/1,
    timestamp/0
]).

%% Timers.
-export([
    send_after/3,
    send_after/4,
    start_timer/3,
    start_timer/4,
    cancel_timer/1,
    cancel_timer/2,
    read_timer/1
]).

%% Receive timeouts — what eta_transform's rewritten `after` blocks call.
-export([
    arm_after/2,
    disarm_after/2
]).

%% Driving.
-export([
    now_ms/0,
    next_deadline/0,
    next_deadline/1,
    advance_to_next/0,
    advance_to_next/1,
    advance/1,
    pending/0,
    strays/0,
    set_faults/1,
    stats/0
]).

-export_type([fault_opts/0]).

-define(STATE, eta_time_state).
-define(TIMERS, eta_time_timers).
-define(REFS, eta_time_refs).

%% Refs of timers `next_deadline/1` has stepped over — see `advance_to_next/1`. A
%% set, so a stray skipped on 20 successive idle calls counts once.
-define(STRAYS, eta_time_strays).

-type fault_opts() :: #{
    drop_p => float(),
    skew_ms => non_neg_integer()
}.

%% ---------------------------------------------------------------------------
%% Lifecycle
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc "Starts a clock with default options. See `start/1`.".
-endif.
-spec start() -> ok.
start() ->
    start(#{}).

-if(?DOCATTRS).
-doc """
Starts the virtual clock.

Options:

- `start_ms` — the clock's initial value (default 0).
- `seed` — seeds the fault RNG, so an injected timer fault schedule replays.
- `faults` — see `set_faults/1`.

Idempotent in the sense that starting over a running clock resets it.
""".
-endif.
-spec start(#{start_ms => integer(), seed => integer(), faults => fault_opts()}) -> ok.
start(Opts) ->
    ok = claim(?STATE, eta_time, clock_in_use),
    stop(),
    ?STATE = ets:new(?STATE, [named_table, public, set]),
    ?TIMERS = ets:new(?TIMERS, [named_table, public, ordered_set]),
    ?REFS = ets:new(?REFS, [named_table, public, set]),
    ?STRAYS = ets:new(?STRAYS, [named_table, public, set]),
    StartMs = maps:get(start_ms, Opts, 0),
    Seed = maps:get(seed, Opts, 0),
    ets:insert(?STATE, [
        {clock, StartMs},
        {seq, 0},
        {fired, 0},
        {dropped, 0},
        %% Offset chosen so system_time/1 returns a plausible wall clock rather
        %% than a number near zero, which tends to confuse code that formats it.
        {system_offset, erlang:system_time(millisecond) - StartMs},
        {faults, maps:get(faults, Opts, #{})},
        {rand, rand:seed_s(exsss, {Seed, Seed + 7, Seed + 13})}
    ]),
    ok.

-if(?DOCATTRS).
-doc "Stops the clock and discards pending timers. Safe to call when not running.".
-endif.
-spec stop() -> ok.
%% Refuse to take over a global that another live process is using.
%%
%% Every one of these tables is named, so there is exactly one clock, one log and
%% one scheduler per VM, and `start/1` used to begin with an unconditional
%% `stop()`. Two concurrent runs therefore ended with the second silently
%% destroying the first's tables, and the first dying on its next clock read with
%% an ETS `badarg` in a stack that said nothing about what had happened.
%%
%% A named table's owner is alive by construction — the VM destroys the table
%% when the owner exits — so the existence of the table is the lock, and no
%% liveness check is needed.
%%
%% `stop/0` is deliberately **not** guarded. It is called from `on_exit` cleanup,
%% which ExUnit runs in a process that is not the owner, and refusing there would
%% turn a harmless no-op into a leak. Losing a race to `stop/0` costs you a
%% teardown; losing one to `start/1` costs you a run.
claim(Tab, Module, Reason) ->
    case ets:info(Tab, owner) of
        undefined ->
            ok;
        Self when Self =:= self() ->
            ok;
        Other ->
            error(
                {Module,
                    {Reason, Other, <<
                        "one per VM; keep runs serial (`async: false`) and do "
                        "not drive two at once"
                    >>}}
            )
    end.

stop() ->
    lists:foreach(
        fun(Tab) ->
            %% Tables are owned by whichever process started the clock, so the VM
            %% destroys them when it exits. A checked delete would race that — the
            %% table can vanish between the check and the delete, which is what an
            %% ExUnit `on_exit` running after the test process is gone hits.
            try
                ets:delete(Tab)
            catch
                error:badarg -> ok
            end
        end,
        [?STATE, ?TIMERS, ?REFS, ?STRAYS]
    ),
    ok.

-if(?DOCATTRS).
-doc "Whether a virtual clock is running. When false, every call here delegates to `erlang`.".
-endif.
-spec running() -> boolean().
running() ->
    ets:info(?STATE, name) =/= undefined.

%% ---------------------------------------------------------------------------
%% Clock reads
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc "The virtual clock in milliseconds. Raises if no clock is running.".
-endif.
-spec now_ms() -> integer().
now_ms() ->
    ets:lookup_element(?STATE, clock, 2).

-spec monotonic_time() -> integer().
monotonic_time() ->
    monotonic_time(native).

-spec monotonic_time(erlang:time_unit()) -> integer().
monotonic_time(Unit) ->
    case running() of
        false -> erlang:monotonic_time(Unit);
        true -> erlang:convert_time_unit(now_ms(), millisecond, Unit)
    end.

-spec system_time() -> integer().
system_time() ->
    system_time(native).

-spec system_time(erlang:time_unit()) -> integer().
system_time(Unit) ->
    case running() of
        false ->
            erlang:system_time(Unit);
        true ->
            Offset = ets:lookup_element(?STATE, system_offset, 2),
            erlang:convert_time_unit(now_ms() + Offset, millisecond, Unit)
    end.

-spec timestamp() -> erlang:timestamp().
timestamp() ->
    case running() of
        false ->
            erlang:timestamp();
        true ->
            Micro = system_time(microsecond),
            {Micro div 1000000000000, (Micro div 1000000) rem 1000000, Micro rem 1000000}
    end.

%% ---------------------------------------------------------------------------
%% Timers
%% ---------------------------------------------------------------------------

-spec send_after(non_neg_integer(), pid() | atom(), term()) -> reference().
send_after(Time, Dest, Msg) ->
    send_after(Time, Dest, Msg, []).

-spec send_after(non_neg_integer(), pid() | atom(), term(), list()) -> reference().
send_after(Time, Dest, Msg, Opts) ->
    case running() of
        false -> erlang:send_after(Time, Dest, Msg, Opts);
        true -> insert_timer(Time, Dest, Msg, send)
    end.

-spec start_timer(non_neg_integer(), pid() | atom(), term()) -> reference().
start_timer(Time, Dest, Msg) ->
    start_timer(Time, Dest, Msg, []).

-spec start_timer(non_neg_integer(), pid() | atom(), term(), list()) -> reference().
start_timer(Time, Dest, Msg, Opts) ->
    case running() of
        false -> erlang:start_timer(Time, Dest, Msg, Opts);
        true -> insert_timer(Time, Dest, Msg, timeout)
    end.

-if(?DOCATTRS).
-doc """
Cancels a timer, returning the milliseconds that were left, or `false` if it had
already fired or never existed — the same contract as `erlang:cancel_timer/1`.
""".
-endif.
-spec cancel_timer(reference()) -> non_neg_integer() | false.
cancel_timer(Ref) ->
    case running() of
        false ->
            erlang:cancel_timer(Ref);
        true ->
            case ets:take(?REFS, Ref) of
                [] ->
                    false;
                [{Ref, Key = {Deadline, _Seq}}] ->
                    ets:delete(?TIMERS, Key),
                    max(0, Deadline - now_ms())
            end
    end.

-spec cancel_timer(reference(), list()) -> non_neg_integer() | false | ok.
cancel_timer(Ref, Opts) ->
    case running() of
        false ->
            erlang:cancel_timer(Ref, Opts);
        true ->
            Result = cancel_timer(Ref),
            %% `{async, true}` means the result comes back as a message rather
            %% than a return value; `{info, false}` suppresses it entirely.
            case {proplists:get_value(async, Opts, false), proplists:get_value(info, Opts, true)} of
                {true, true} ->
                    self() ! {cancel_timer, Ref, Result},
                    ok;
                {true, false} ->
                    ok;
                _ ->
                    Result
            end
    end.

-spec read_timer(reference()) -> non_neg_integer() | false.
read_timer(Ref) ->
    case running() of
        false ->
            erlang:read_timer(Ref);
        true ->
            case ets:lookup(?REFS, Ref) of
                [] -> false;
                [{Ref, {Deadline, _Seq}}] -> max(0, Deadline - now_ms())
            end
    end.

%% Create a timer at `now + Time`, subject to the fault policy.  Ordering among
%% timers sharing a deadline is by creation sequence, so firing order is
%% deterministic.
-spec insert_timer(non_neg_integer(), pid() | atom(), term(), send | timeout) -> reference().
insert_timer(Time, Dest, Msg, Kind) ->
    Ref = make_ref(),
    Seq = ets:update_counter(?STATE, seq, 1),
    Deadline = now_ms() + skew(Time),
    Key = {Deadline, Seq},
    Drop = roll_drop(),
    ets:insert(?TIMERS, {Key, Ref, Dest, Msg, Kind, Drop}),
    ets:insert(?REFS, {Ref, Key}),
    Ref.

%% ---------------------------------------------------------------------------
%% Receive-timeout support (for eta_transform's `-eta_after(true)` pass)
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc """
Arms the virtual timeout behind a rewritten `receive ... after`.

`eta_transform` turns the `after` block into an ordinary receive clause
waiting on `{'$eta_after', Ref}`, and this is what makes that message arrive. The
handle it returns is opaque and belongs to `disarm_after/2`.

`infinity` arms nothing — the generated clause then simply cannot match, which is
exactly `receive ... end`.

A zero timeout is delivered immediately rather than through a timer. `after 0` is
a mailbox poll, not a wait: routing it through the timer wheel would block until
something advanced the clock, turning a non-blocking poll into a wait. Appending to
our own mailbox also keeps the ordering right, since a selective receive scans in
arrival order and therefore still prefers anything already queued.
""".
-endif.
-spec arm_after(timeout(), reference()) -> term().
arm_after(infinity, _Ref) ->
    undefined;
arm_after(0, Ref) ->
    self() ! {'$eta_after', Ref},
    flush_only;
arm_after(Timeout, Ref) when is_integer(Timeout), Timeout > 0 ->
    send_after(Timeout, self(), {'$eta_after', Ref}).

-if(?DOCATTRS).
-doc """
Cancels an armed timeout and removes its message if it already landed.

The transform calls this at the *head* of every ordinary receive clause rather
than after the receive, which is what keeps the clause bodies in tail position —
see `eta_transform`. Flushing is not optional: a timer that fired before its
receive matched something else leaves a `{'$eta_after', Ref}` nothing will ever
match again.
""".
-endif.
-spec disarm_after(term(), reference()) -> ok.
disarm_after(undefined, _Ref) ->
    ok;
disarm_after(flush_only, Ref) ->
    %% `arm_after(0, Ref)` delivered the message straight to our own mailbox, so
    %% there is definitely one to remove.
    flush_after(Ref);
disarm_after(TRef, Ref) ->
    case cancel_timer(TRef) of
        false ->
            %% Already fired, so a message may be queued for a receive that has
            %% moved on. This is the only case that needs the flush.
            flush_after(Ref);
        _Remaining ->
            %% Cancelled with time to spare, which means it never sent anything.
            %%
            %% Worth the extra clause: this runs on **every** successful receive
            %% in a rewritten module, and `flush_after/1` is a selective receive
            %% on a ref the compiler cannot prove is fresh — so it walks the
            %% whole mailbox before giving up. Skipping it when there is provably
            %% nothing to find takes an O(mailbox) scan off the common path.
            ok
    end.

%% This module never declares `-eta_after(true)` — if it did,
%% the `after 0` below would rewrite into a call to `arm_after/2`, which calls this.
flush_after(Ref) ->
    receive
        {'$eta_after', Ref} -> ok
    after 0 -> ok
    end.

%% ---------------------------------------------------------------------------
%% Fault injection
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc """
Sets the timer fault policy.

- `drop_p` — probability that a timer, once set, never fires. It remains
  cancellable and readable; it simply produces no message. Models a timer lost
  with the process or connection it belonged to.
- `skew_ms` — timers are created with their deadline perturbed uniformly by up to
  this many milliseconds either way (never earlier than now). Models a timeout
  firing in an unlucky window relative to other work.

Skew is applied at creation rather than at firing, so the deadline remains
meaningful to `read_timer/1` and `cancel_timer/1`.
""".
-endif.
-spec set_faults(fault_opts()) -> ok.
set_faults(Faults) ->
    ets:insert(?STATE, {faults, Faults}),
    ok.

-spec skew(non_neg_integer()) -> non_neg_integer().
skew(Time) ->
    case maps:get(skew_ms, faults(), 0) of
        0 ->
            Time;
        Skew ->
            %% uniform(2*Skew+1) - 1 - Skew gives -Skew..+Skew inclusive.
            Delta = roll_uniform(2 * Skew + 1) - 1 - Skew,
            max(0, Time + Delta)
    end.

-spec roll_drop() -> boolean().
roll_drop() ->
    case maps:get(drop_p, faults(), 0.0) of
        P when P =< 0.0 -> false;
        P -> roll_float() < P
    end.

-spec faults() -> fault_opts().
faults() ->
    ets:lookup_element(?STATE, faults, 2).

-spec roll_float() -> float().
roll_float() ->
    {V, Rand} = rand:uniform_s(ets:lookup_element(?STATE, rand, 2)),
    ets:insert(?STATE, {rand, Rand}),
    V.

-spec roll_uniform(pos_integer()) -> pos_integer().
roll_uniform(N) ->
    {V, Rand} = rand:uniform_s(N, ets:lookup_element(?STATE, rand, 2)),
    ets:insert(?STATE, {rand, Rand}),
    V.

%% ---------------------------------------------------------------------------
%% Driving
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc "The earliest pending deadline, or `infinity` if no timers are set.".
-endif.
-spec next_deadline() -> integer() | infinity.
next_deadline() ->
    next_deadline(fun(_Dest) -> true end).

-if(?DOCATTRS).
-doc """
The earliest pending deadline whose destination `Schedulable` accepts.

Timers it rejects are stepped over and **left in the wheel**. They no longer
decide *when* the clock moves, but if it reaches them for some other reason they
still fire. See `advance_to_next/1` for why that distinction matters.
""".
-endif.
-spec next_deadline(fun((pid() | atom()) -> boolean())) -> integer() | infinity.
next_deadline(Schedulable) ->
    scan(ets:first(?TIMERS), Schedulable).

scan('$end_of_table', _Schedulable) ->
    infinity;
scan(Key = {Deadline, _Seq}, Schedulable) ->
    case ets:lookup(?TIMERS, Key) of
        [{Key, Ref, Dest, _Msg, _Kind, _Drop}] ->
            case Schedulable(Dest) of
                true ->
                    Deadline;
                false ->
                    %% This deadline would have moved the clock on behalf of a
                    %% process that cannot take a step. Record it once and keep
                    %% looking; `strays/0` is what reports it.
                    _ = ets:insert(?STRAYS, {Ref}),
                    scan(ets:next(?TIMERS, Key), Schedulable)
            end;
        [] ->
            scan(ets:next(?TIMERS, Key), Schedulable)
    end.

-if(?DOCATTRS).
-doc """
How many distinct timers the clock has refused to advance to — see
`advance_to_next/1`.

Counted at the moment one is stepped over rather than by scanning what is still
pending, because a stray is usually gone by the end: some other timer advances
the clock past it and `fire_due/1` delivers it on the way. The damage is done
when it is *considered*, not when it fires.

`eta_run` reports this as `stray_timers`, and `audit/1` treats a non-zero count
as a system holding a timer the scheduler does not own.
""".
-endif.
-spec strays() -> non_neg_integer().
strays() ->
    case ets:info(?STRAYS, size) of
        undefined -> 0;
        N -> N
    end.

-if(?DOCATTRS).
-doc """
Advances the clock to the earliest pending deadline and fires every timer due at
it, in creation order.

Returns `true` if the clock moved, `false` if nothing was pending — which is the
signal the event loop uses to decide a run is over. Suitable as `eta_sched:run/3`'s
idle callback.
""".
-endif.
-spec advance_to_next() -> boolean().
advance_to_next() ->
    advance_to_next(fun(_Dest) -> true end).

-if(?DOCATTRS).
-doc """
Advances to the earliest deadline whose destination `Schedulable` accepts.

**A timer is owned by a process, and the scheduler is not obliged to own that
process.** A deadline belonging to one it does not own cannot make anything
runnable, so advancing to it moves the clock and records an entry on behalf of
something that can never take a step. Two runs of one seed then differ by an
inserted `{clock, Ms}` with no step behind it, which is a schedule decided by
something other than the seed.

There are 2 ordinary ways to get such a timer. A process killed while blocked in
a rewritten `receive ... after` never reaches the clause head that disarms it, so
its deadline outlives it. And a process spawned before the scheduler existed —
anything a system starts during `init/2` — is outside the schedule by
construction.

Rejected timers are **skipped, not cancelled**. Cancelling would silently
disarm a live process's timer, and the goal is only that a stray stops deciding
when the clock moves. If time passes far enough for another reason, `fire_due/1`
delivers it as before.

`advance_to_next/0` accepts everything, which is what you want when driving the
clock by hand with no scheduler to ask.
""".
-endif.
-spec advance_to_next(fun((pid() | atom()) -> boolean())) -> boolean().
advance_to_next(Schedulable) ->
    case next_deadline(Schedulable) of
        infinity ->
            false;
        Deadline ->
            ets:insert(?STATE, {clock, max(now_ms(), Deadline)}),
            fire_due(Deadline),
            true
    end.

-if(?DOCATTRS).
-doc """
Advances the clock by `Ms`, firing every timer that becomes due. Returns how many
fired.

`advance_to_next/0` is usually what a run wants; this is for a test that needs to
say "and then a second passed" regardless of what is pending.
""".
-endif.
-spec advance(non_neg_integer()) -> non_neg_integer().
advance(Ms) ->
    Target = now_ms() + Ms,
    Before = fired_count(),
    ets:insert(?STATE, {clock, Target}),
    fire_due(Target),
    fired_count() - Before.

%% Fire every timer with a deadline at or before `Upto`, earliest first, then by
%% creation sequence.  Re-reads `ets:first/1` each time because firing a timer can
%% cause the receiving process to set another — though under eta_sched the
%% receiver is suspended, so in practice this only matters when driving by hand.
-spec fire_due(integer()) -> ok.
fire_due(Upto) ->
    case ets:first(?TIMERS) of
        '$end_of_table' ->
            ok;
        Key = {Deadline, _Seq} when Deadline =< Upto ->
            case ets:take(?TIMERS, Key) of
                [{Key, Ref, Dest, Msg, Kind, Drop}] ->
                    ets:delete(?REFS, Ref),
                    deliver(Ref, Dest, Msg, Kind, Drop);
                [] ->
                    ok
            end,
            fire_due(Upto);
        _ ->
            ok
    end.

-spec deliver(reference(), pid() | atom(), term(), send | timeout, boolean()) -> ok.
deliver(_Ref, _Dest, _Msg, _Kind, true) ->
    ets:update_counter(?STATE, dropped, 1),
    ok;
deliver(Ref, Dest, Msg, Kind, false) ->
    Payload =
        case Kind of
            send -> Msg;
            timeout -> {timeout, Ref, Msg}
        end,
    %% A registered name that no longer exists is not an error here, the same way
    %% a real timer firing at a dead process is not.
    try
        Dest ! Payload
    catch
        _:_ -> ok
    end,
    ets:update_counter(?STATE, fired, 1),
    ok.

-if(?DOCATTRS).
-doc "How many timers are currently pending.".
-endif.
-spec pending() -> non_neg_integer().
pending() ->
    ets:info(?TIMERS, size).

-if(?DOCATTRS).
-doc "Clock value, pending timer count, and how many timers have fired or been dropped.".
-endif.
-spec stats() -> #{atom() => term()}.
stats() ->
    #{
        now_ms => now_ms(),
        pending => pending(),
        fired => fired_count(),
        dropped => ets:lookup_element(?STATE, dropped, 2),
        strays => strays()
    }.

-spec fired_count() -> non_neg_integer().
fired_count() ->
    ets:lookup_element(?STATE, fired, 2).
