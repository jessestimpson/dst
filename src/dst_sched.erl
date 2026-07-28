-module(dst_sched).
-behaviour(gen_server).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

%% `register/2` is deliberately the API name; it shadows the auto-imported BIF,
%% which is never used here.
-compile({no_auto_import, [register/2, spawn/1, spawn/3, spawn_link/1, spawn_link/3]}).

-if(?DOCATTRS).
-moduledoc """
A serializing scheduler for BEAM processes — Phase 0 of the DST framework
(design: `docs/design.md`).

Runs exactly one process at a time, to quiescence, so the interleaving of a
concurrent system is determined entirely by this scheduler's sequence of choices.
A seeded choice sequence therefore replays bit-for-bit.

This module is system-agnostic: it knows about pids, mailboxes, and scheduling,
and nothing about what the processes do. A replicated process registry was its
first consumer, not its subject.

## Why controlling *who runs* is enough

It is tempting to think a deterministic scheduler must intercept every message to
control delivery order. It does not. Erlang already guarantees FIFO between an
ordered pair of processes, so once only one process runs at a time, the global
order of message *delivery* follows from the order in which senders were stepped.
Controlling scheduling alone determinises the whole system, which is what makes
this approach tractable without rewriting the code under test.

## Quiescence, and why the naive check is wrong

A process has finished its step when it is blocked on a receive it cannot
satisfy. The obvious test — status `waiting` and an empty mailbox — is wrong, and
wrong in a way that matters: a process blocked in

```erlang
receive {reply, Ref, V} -> V end
```

with a non-matching message queued is `waiting` with a *non-empty* mailbox. That
is not an edge case; it is what every `gen_server:call/3` does. Treating it as
runnable makes the scheduler spin on a process that can never progress, and makes
"no runnable process" — the termination condition — unreachable.

The two questions are separated properly:

- **Quiescent?** `waiting` is definitive: the process is blocked in a receive.
  Detected from `running` trace events, not by polling. Checking status straight
  after `erlang:resume_process/1` is a trap — a blocked process still reads
  `waiting` *before it has been scheduled in*, so the step would conclude without
  the process ever running. `in` is awaited first, as proof it got the CPU.
- **Runnable?** Has messages, and is not known to be blocked at this exact mailbox
  state. A `receive` blocks only after failing to match *every* queued message, so
  after any step the remaining mailbox is known not to match; the process is
  recorded as blocked at that queue length and skipped until the queue grows.
  Selective receive is then handled by construction, and the skip is
  self-correcting: a new non-matching message costs one wasted step and re-arms
  the block.

## Measuring progress

Consumption is computed from the queue-length delta, corrected for messages the
process sent to itself (nothing else runs, so those are the only arrivals).

The `'receive'` trace flag cannot be used for this, which is worth stating because
the opposite is the natural assumption: it fires when a receive *scans* a message,
matching or not, so a non-matching message produces a `'receive'` event and stays
in the queue.

## The scheduler owns its own process and mailbox

It runs as a `gen_server` and is the tracer for every process it owns, so trace
events land in *its* mailbox rather than the driver's.

That was not always so, and the reason it changed is worth keeping. Phase 0 ran
the scheduler in the calling process, on the argument that a scheduler process
would itself need scheduling — which is false: nothing under test ever calls the
scheduler, so it is never part of the system being serialized. The cost of the
original arrangement was a whole class of bug that only discipline prevented. An
early version used a catch-all `receive` while awaiting trace events, which
consumed and discarded whatever the system under test had sent the driver — a
`gen_server` reply, vanishing, with its caller exiting `:normal`. Every receive
here still matches trace-shaped messages only, but that is now a tidiness
property rather than the only thing standing between the framework and a
mystifying failure.

It also matters for the driver. A `dst_run`-style helper that waits on a
predicate has to interleave stepping with checks on the system under test, so it
needs a mailbox the scheduler is not also reading.

## `sched()` is a handle, not a value

The API threads a `sched()` through, and every function that took one still
returns one, so call sites read as they always did. It now names a process rather
than carrying state, and the value returned is the same handle rather than an
updated copy.

This removes a false affordance rather than adding one. The old record was never
usable as a value: the scheduler's real state is the suspend/resume status of live
OS processes, so holding on to an earlier `sched()` and stepping it again was
never going to replay anything — the processes had already moved.

`release/1` ends the scheduler's life, so inspect (`choices/1`, `stats/1`) before
releasing, not after.

## Determinism boundary

- **Timers.** A process blocked in `receive ... after N` wakes on the real clock
  unless its module is compiled with `dst_after_transform`, which puts the timeout
  on the virtual clock. Untransformed, a system under test must avoid
  real-time-dependent receives (use `infinity` in simulation).
- **Spawn races.** A child spawned with a plain `erlang:spawn` is adopted via
  `set_on_spawn` tracing, which reports the spawn *after* the child exists — so it
  runs briefly, on the real scheduler, before it is suspended. `stats/1`'s
  `adopted_late` counts these. `spawn/1` and friends close the window by starting
  the child blocked; `dst_transform` points a transformed module's spawns there, so
  a system under simulation should show `adopted_late` at or near zero. A run with
  a high count is a run whose interleaving is partly wall-clock.
""".
-endif.

-export([
    new/0,
    new/1,
    register/2,
    release/1,
    ids/1,
    runnable/1,
    step/2,
    run/1,
    run/2,
    run/3,
    replay/2,
    choices/1,
    stats/1,
    pid/1
]).

%% Scheduler-owned spawning — what `dst_transform` points a system's `spawn` calls
%% at. `gated/1,3` are exported because they are spawned by MFA, which is what
%% makes a gated child distinguishable in the spawn trace event.
-export([active/0, current/0, spawn/1, spawn/3, spawn_link/1, spawn_link/3, gated/1, gated/3]).

%% Gated `gen_server` starts — see `start_monitor/3`. `gated/4` is spawned by MFA
%% for the same reason `gated/1,3` are: the spawn trace event has to name it.
-export([start_monitor/3, start_link/3, start/3, gated/4]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-export_type([sched/0, id/0, outcome/0]).

%% `running` gives the in/out scheduling events quiescence is detected from.
%% `send` lets a step count self-sends, the correction term consumption needs.
%% `procs` + `set_on_spawn` adopt processes the system under test creates.
-define(TRACE_FLAGS, [running, 'receive', send, procs, set_on_spawn]).

%% A step should never take this many scheduling events; bailing out beats hanging
%% a test suite on a process that never blocks.
-define(MAX_EVENTS_PER_STEP, 100000).

%% How long to wait for a scheduling event before concluding none is coming. Only
%% paid when a process passed the awaited point before we started listening, which
%% is rare; the common path returns as soon as the event lands.
-define(EVENT_TIMEOUT, 5).

%% Bound on waiting for erlang:trace_delivered/1 to report the trace stream drained.
-define(DELIVERED_TIMEOUT, 5000).

%% Presence of this table means a scheduler is running, so `spawn/1` and friends
%% gate their children. Created by the scheduler process and destroyed with it, so
%% the inertness is automatic — the same arrangement `dst_time` uses.
-define(ACTIVE, dst_sched_active).

%% What releases a gated child once the scheduler owns it.
-define(GO, '$dst_sched_go').

-type id() :: non_neg_integer().
-type outcome() :: progress | no_progress | exited.
-opaque sched() :: {dst_sched, pid()}.

-record(st, {
    rand :: rand:state(),
    %% id => pid, and the reverse. Ids are stable across runs (assignment order is
    %% a function of the schedule), pids are not — so choices are recorded as ids.
    procs = #{} :: #{id() => pid()},
    ids = #{} :: #{pid() => id()},
    next_id = 0 :: id(),
    %% id => queue length at which this process was found blocked.
    blocked_at = #{} :: #{id() => non_neg_integer()},
    exited = #{} :: #{id() => true},
    %% Children spawned through `spawn/1` and friends, which start blocked. Tracked
    %% so `release/1` can let go of any that never got their token.
    gated = #{} :: #{pid() => true},
    choices = [] :: [id()],
    adopted_late = 0 :: non_neg_integer(),
    steps = 0 :: non_neg_integer()
}).

%% A step is internally bounded (?MAX_EVENTS_PER_STEP, ?EVENT_TIMEOUT,
%% ?DELIVERED_TIMEOUT) and a run is bounded by MaxSteps, so the call cannot hang
%% for reasons a caller-side timeout would diagnose better than the bounds do.
-define(CALL_TIMEOUT, infinity).

%% ---------------------------------------------------------------------------
%% Lifecycle
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc "A fresh scheduler with the default seed. See `new/1`.".
-endif.
-spec new() -> sched().
new() ->
    new(#{}).

-if(?DOCATTRS).
-doc """
Starts a scheduler and returns a handle to it. `seed` fixes the choice sequence.

Linked to the caller, so a driver that dies takes the scheduler with it — and with
it every suspend it holds, which lets the system under test run free rather than
leaving it frozen for the rest of the VM's life.
""".
-endif.
-spec new(#{seed => integer()}) -> sched().
new(Opts) ->
    {ok, Pid} = gen_server:start_link(?MODULE, Opts, []),
    {dst_sched, Pid}.

-if(?DOCATTRS).
-doc "The scheduler's pid. For tests that need to assert on the process itself.".
-endif.
-spec pid(sched()) -> pid().
pid({dst_sched, Pid}) ->
    Pid.

-if(?DOCATTRS).
-doc """
Takes ownership of a pid (or a list of them): suspends it and begins tracing it.

Register every process before any of them is allowed to run, or the run starts
from a state this scheduler did not choose.
""".
-endif.
-spec register(sched(), pid() | [pid()]) -> sched().
register(S, Pids) ->
    call(S, {register, Pids}),
    S.

-if(?DOCATTRS).
-doc """
Resumes every process, stops tracing, and shuts the scheduler down.

The system runs freely again. Inspect (`choices/1`, `stats/1`) *before* calling
this — the handle is dead afterwards.
""".
-endif.
-spec release(sched()) -> ok.
release(S = {dst_sched, Pid}) ->
    call(S, release),
    gen_server:stop(Pid),
    ok.

%% ---------------------------------------------------------------------------
%% Scheduler-owned spawning
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc """
Whether a scheduler is running in this VM. When false, `spawn/1` and friends are
plain `erlang` spawns.
""".
-endif.
-spec active() -> boolean().
active() ->
    ets:info(?ACTIVE, name) =/= undefined.

-if(?DOCATTRS).
-doc """
The scheduler running in this VM, or `undefined`.

For code that is handed no `sched()` and still has to ask the scheduler something —
in practice, an invariant asking whether the system is **quiescent**. A property
that only holds once the system has stopped moving (a registry's replicas agree
only after speculation settles) is otherwise unstateable: `check/1` receives
the system under test, not the schedule, so it cannot tell "diverged" from "still
in flight" on its own.

Safe to call from an invariant. The scheduler is never part of the system it
serializes, so it is not suspended, and `runnable/1` is a read.
""".
-endif.
-spec current() -> sched() | undefined.
current() ->
    try ets:lookup(?ACTIVE, sched) of
        [{sched, S}] -> S;
        [] -> undefined
    catch
        error:badarg -> undefined
    end.

-if(?DOCATTRS).
-doc """
`erlang:spawn/1` for a system under simulation: the child starts **blocked** and
runs only once the scheduler owns it.

`set_on_spawn` tracing adopts a child after the fact, which is not the same thing
and is not enough. Between the spawn and the scheduler handling the trace event the
child runs on the *real* scheduler, so anything it does in that window is ordered by
wall clock rather than by the schedule. Porting a distributed registry measured the
cost: 1212 of a run's 1233 processes were adopted late, because that system spawns a
short-lived helper for almost every operation — and a seed consequently did not
reproduce its own schedule.

Gating closes it. The child blocks on a token before running anything, so there is
no window; the scheduler suspends it, adopts it, and sends the token, after which it
is runnable and stepped like anything else. `stats/1`'s `adopted_late` then counts
only genuinely ungated children.

The child is spawned by MFA rather than as a fun so that the spawn trace event
carries `{dst_sched, gated, _}` — that is how the scheduler tells a gated child from
one it must chase.

Inert with no scheduler running, exactly as `dst_time` is with no clock, so a
transformed module behaves normally outside a simulation.
""".
-endif.
-spec spawn(fun(() -> term())) -> pid().
spawn(Fun) ->
    case active() of
        false -> erlang:spawn(Fun);
        true -> erlang:spawn(?MODULE, gated, [Fun])
    end.

-spec spawn(module(), atom(), [term()]) -> pid().
spawn(M, F, A) ->
    case active() of
        false -> erlang:spawn(M, F, A);
        true -> erlang:spawn(?MODULE, gated, [M, F, A])
    end.

-spec spawn_link(fun(() -> term())) -> pid().
spawn_link(Fun) ->
    case active() of
        false -> erlang:spawn_link(Fun);
        true -> erlang:spawn_link(?MODULE, gated, [Fun])
    end.

-spec spawn_link(module(), atom(), [term()]) -> pid().
spawn_link(M, F, A) ->
    case active() of
        false -> erlang:spawn_link(M, F, A);
        true -> erlang:spawn_link(?MODULE, gated, [M, F, A])
    end.

-if(?DOCATTRS).
-doc false.
-endif.
-spec gated(fun(() -> term())) -> term().
gated(Fun) ->
    _ = wait_for_token(),
    Fun().

-if(?DOCATTRS).
-doc false.
-endif.
-spec gated(module(), atom(), [term()]) -> term().
gated(M, F, A) ->
    _ = wait_for_token(),
    apply(M, F, A).

%% An empty mailbox and a selective receive, so the child is quiescent from birth
%% and cannot act before the scheduler has it.
wait_for_token() ->
    receive
        ?GO -> ok
    end.

-if(?DOCATTRS).
-doc """
`gen_server:start_monitor/3` for a system under simulation: the child is gated, so
the scheduler owns it before it runs a line of `init/1`.

## Why this exists rather than a shadowed `proc_lib`

The child of a `gen_server:start_*` is spawned inside OTP —
`gen:do_spawn/5` calls `proc_lib:start_monitor/5`, which calls
`erlang:spawn_opt(proc_lib, init_p, …)`. No transform of the system under test
reaches that, so its children are adopted late and the run stops being replayable.
On that registry it was one child per group commit, and that alone was enough to
make a seed produce a different schedule each time.

The obvious fix is to shadow `proc_lib` with a transformed copy. It is worse than
it sounds:

- **stdlib is sticky.** Loading a replacement fails with `sticky_directory`; it
  takes a deliberate `code:unstick_dir/1` on stdlib's ebin first.
- **The blast radius is the VM.** Every OTP process start goes through `proc_lib`,
  including ExUnit's and Logger's. With a deliberately broken copy loaded,
  `Agent.start_link/1` fails immediately with `undef`.
- **It is 1600 lines of copied OTP** that must track releases, and a mismatch is a
  subtle breakage rather than a loud one.

So the child is built rather than intercepted. A gated process sets up the two
process-dictionary entries `init_p/3` in `proc_lib` would have set, runs `init/1`
itself, acknowledges, and then *becomes* the `gen_server` through
`gen_server:enter_loop/3`. Verified to produce a genuine OTP process: `sys:get_state/1`,
`sys:get_status/1` and `sys:suspend/1` all work on it, and `$initial_call` reads
as it should.

## What it covers, and what it refuses

`init/1` returning `{ok, State}`, `{ok, State, {continue, C}}` — including a
continue chaining into another, which `enter_loop/3` does not do for you —
`{stop, Reason}` and `ignore`. Anything else raises rather than guessing: this is
a stand-in for a start path, and a stand-in that silently mishandles a shape is
worse than one that stops.

A start *timeout* in `Options` is ignored. Waiting on one would be a real-clock
dependency in the middle of a simulated run, which is the thing the framework
exists to remove, and no caller seen so far passes one.

Inert with no scheduler running: delegates straight to `gen_server`.
""".
-endif.
-spec start_monitor(module(), term(), list()) ->
    {ok, {pid(), reference()}} | ignore | {error, term()}.
start_monitor(Mod, Args, Options) ->
    case active() of
        false -> gen_server:start_monitor(Mod, Args, Options);
        true -> gated_start(Mod, Args, monitor)
    end.

-spec start_link(module(), term(), list()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Mod, Args, Options) ->
    case active() of
        false -> gen_server:start_link(Mod, Args, Options);
        true -> gated_start(Mod, Args, link)
    end.

-spec start(module(), term(), list()) -> {ok, pid()} | ignore | {error, term()}.
start(Mod, Args, Options) ->
    case active() of
        false -> gen_server:start(Mod, Args, Options);
        true -> gated_start(Mod, Args, plain)
    end.

gated_start(Mod, Args, Kind) ->
    Parent = self(),
    Ancestors = [Parent | own_ancestors()],
    MFA = [Parent, Ancestors, Mod, Args],
    {Pid, Ref} =
        case Kind of
            monitor -> erlang:spawn_monitor(?MODULE, gated, MFA);
            link -> {erlang:spawn_link(?MODULE, gated, MFA), undefined};
            plain -> {erlang:spawn(?MODULE, gated, MFA), undefined}
        end,
    await_start(Pid, Ref, Kind).

%% `infinity`, deliberately: see the note on start timeouts above. The child cannot
%% run until the scheduler adopts it, and the scheduler is not waiting on us.
await_start(Pid, Ref, Kind) ->
    receive
        {'$dst_started', Pid, {ok, Pid}} ->
            case Kind of
                monitor -> {ok, {Pid, Ref}};
                _ -> {ok, Pid}
            end;
        {'$dst_started', Pid, Other} ->
            _ = drop_monitor(Ref),
            Other;
        {'DOWN', Ref, process, Pid, Reason} ->
            {error, Reason}
    end.

drop_monitor(undefined) -> ok;
drop_monitor(Ref) -> erlang:demonitor(Ref, [flush]).

-if(?DOCATTRS).
-doc false.
-endif.
gated(Parent, Ancestors, Mod, Args) ->
    _ = wait_for_token(),
    %% What proc_lib:init_p/3 would have set. Without these the process is not an
    %% OTP process: no crash-report formatting, and `sys` cannot introspect it.
    put('$ancestors', Ancestors),
    put('$initial_call', {Mod, init, 1}),
    case Mod:init(Args) of
        {ok, State} ->
            Parent ! {'$dst_started', self(), {ok, self()}},
            gen_server:enter_loop(Mod, [], State);
        {ok, State, {continue, C}} ->
            Parent ! {'$dst_started', self(), {ok, self()}},
            gated_continue(Mod, C, State);
        {stop, Reason} ->
            Parent ! {'$dst_started', self(), {error, Reason}},
            exit(Reason);
        ignore ->
            Parent ! {'$dst_started', self(), ignore},
            exit(normal);
        Other ->
            %% `{ok, State, Timeout}` and `{ok, State, hibernate}` land here.
            %% Both are real-clock behaviours that would need simulating, and
            %% guessing at them would be a stand-in that lies.
            Parent ! {'$dst_started', self(), {error, {dst_sched, unsupported_init, Other}}},
            exit({dst_sched, unsupported_init, Other})
    end.

%% `gen_server` runs handle_continue after the init acknowledgement and before any
%% message, and a continue may chain into another. `enter_loop/3` takes a state,
%% not a continue, so this is done here.
gated_continue(Mod, C, State) ->
    case Mod:handle_continue(C, State) of
        {noreply, State1} -> gen_server:enter_loop(Mod, [], State1);
        {noreply, State1, {continue, C2}} -> gated_continue(Mod, C2, State1);
        {stop, Reason, _State1} -> exit(Reason)
    end.

own_ancestors() ->
    case get('$ancestors') of
        undefined -> [];
        A -> A
    end.

%% ---------------------------------------------------------------------------
%% Inspection
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc "Every known process id, ascending. Ids are assigned in registration order.".
-endif.
-spec ids(sched()) -> [id()].
ids(S) ->
    call(S, ids).

-if(?DOCATTRS).
-doc """
Ids that have work to do: a non-empty mailbox they are not known to be blocked
against. See the module doc on selective receive.
""".
-endif.
-spec runnable(sched()) -> [id()].
runnable(S) ->
    call(S, runnable).

-if(?DOCATTRS).
-doc """
Run statistics: steps taken, processes known, how many have exited, and how many
children were adopted after they had already started running (see the module
doc's determinism boundary).
""".
-endif.
-spec stats(sched()) -> #{atom() => non_neg_integer()}.
stats(S) ->
    call(S, stats).

-if(?DOCATTRS).
-doc "The choice sequence, oldest first. Feed to `replay/2`.".
-endif.
-spec choices(sched()) -> [id()].
choices(S) ->
    call(S, choices).

%% ---------------------------------------------------------------------------
%% Stepping and driving
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc """
Runs one process until it blocks, then suspends it again.

Returns `progress` (it consumed at least one message), `no_progress` (it blocked
without consuming — a selective receive that matched nothing), or `exited`.
""".
-endif.
-spec step(sched(), id()) -> {outcome(), sched()}.
step(S, Id) ->
    {call(S, {step, Id}), S}.

-if(?DOCATTRS).
-doc "Steps up to 10000 times. See `run/2`.".
-endif.
-spec run(sched()) -> sched().
run(S) ->
    run(S, 10000).

-if(?DOCATTRS).
-doc """
Steps processes, choosing from the seeded RNG, until nothing is runnable or
`MaxSteps` is reached.

`choices/1` then gives the sequence to replay.
""".
-endif.
-spec run(sched(), non_neg_integer()) -> sched().
run(S, MaxSteps) ->
    run(S, MaxSteps, fun() -> false end).

-if(?DOCATTRS).
-doc """
`run/2` with an idle callback — the discrete-event loop.

`OnIdle` is invoked only when **nothing is runnable**, and returns whether it
created new work. That is the hook virtual time plugs into:

```erlang
dst_sched:run(Sched, MaxSteps, fun dst_time:advance_to_next/0)
```

so the clock jumps straight to the next deadline once the current instant has
nothing left to do, and the run ends when neither processes nor timers have
anything pending. Time therefore advances in jumps between events rather than in
ticks, and never while work remains at the current instant — which is both what
makes the ordering deterministic and why waiting out a long timeout is free.

The scheduler deliberately knows nothing about time: it asks "is there more work?"
and the caller decides what that means. An idle callback that returns `true`
counts against `MaxSteps`, so a callback that always says yes terminates rather
than spinning.

`OnIdle` runs **in the scheduler's process**, not the caller's. `dst_time` is ETS
and callable from anywhere, so the usual callback is unaffected; a callback that
wants the driver's mailbox is not, and should not be one.
""".
-endif.
-spec run(sched(), non_neg_integer(), fun(() -> boolean())) -> sched().
run(S, MaxSteps, OnIdle) ->
    call(S, {run, MaxSteps, OnIdle}),
    S.

-if(?DOCATTRS).
-doc """
Replays a recorded choice sequence exactly, ignoring the RNG.

A choice naming a process that is not currently runnable is a divergence — the
run did not follow the same path — and is reported rather than skipped, because
silently continuing would produce a "replay" that is not one.
""".
-endif.
-spec replay(sched(), [id()]) ->
    {ok | {error, {diverged, id(), [id()]}}, sched()}.
replay(S, Choices) ->
    {call(S, {replay, Choices}), S}.

call({dst_sched, Pid}, Request) ->
    gen_server:call(Pid, Request, ?CALL_TIMEOUT).

%% ---------------------------------------------------------------------------
%% Server
%% ---------------------------------------------------------------------------

init(Opts) ->
    Seed = maps:get(seed, Opts, 0),
    _ = ets:new(?ACTIVE, [named_table, public, set]),
    true = ets:insert(?ACTIVE, {sched, {?MODULE, self()}}),
    {ok, #st{rand = rand:seed_s(exsss, {Seed, Seed + 1, Seed + 2})}}.

handle_call({register, Pids}, _From, St) ->
    {reply, ok, do_register(St, Pids)};
handle_call(release, _From, St) ->
    {reply, ok, do_release(St)};
handle_call(ids, _From, St = #st{procs = Procs}) ->
    {reply, lists:sort(maps:keys(Procs)), St};
handle_call(runnable, _From, St) ->
    {reply, runnable_ids(St), St};
handle_call(choices, _From, St = #st{choices = Choices}) ->
    {reply, lists:reverse(Choices), St};
handle_call(stats, _From, St) ->
    #st{steps = Steps, procs = Procs, exited = Exited, adopted_late = Late} = St,
    Reply = #{
        steps => Steps,
        processes => maps:size(Procs),
        exited => maps:size(Exited),
        adopted_late => Late
    },
    {reply, Reply, St};
handle_call({step, Id}, _From, St) ->
    {Outcome, St1} = do_step(St, Id),
    {reply, Outcome, St1};
handle_call({run, MaxSteps, OnIdle}, _From, St) ->
    {reply, ok, do_run(St, MaxSteps, OnIdle)};
handle_call({replay, Choices}, _From, St) ->
    {Result, St1} = do_replay(St, Choices),
    {reply, Result, St1}.

handle_cast(_Msg, St) ->
    {noreply, St}.

%% Trace events that arrive between calls: the same handling a step would give
%% them. Adopting a child here rather than at the next step is what keeps a
%% process spawned during an idle window from running unsupervised.
handle_info({trace, _, _, _} = T, St) ->
    {noreply, handle_trace(St, T)};
handle_info({trace, _, _, _, _} = T, St) ->
    {noreply, handle_trace(St, T)};
%% A drain that timed out can leave this behind; it is not an error.
handle_info({trace_delivered, _Pid, _Ref}, St) ->
    {noreply, St};
handle_info(_Msg, St) ->
    {noreply, St}.

%% Suspends belong to this process, so they lift when it exits — but only after
%% the VM has cleaned up, and an untraced-but-suspended process in the meantime
%% is a confusing thing to leave behind. Release explicitly.
terminate(_Reason, St) ->
    _ = do_release(St),
    try
        ets:delete(?ACTIVE)
    catch
        error:badarg -> ok
    end,
    ok.

%% ---------------------------------------------------------------------------
%% Registration
%% ---------------------------------------------------------------------------

do_register(St, Pids) when is_list(Pids) ->
    lists:foldl(fun(Pid, Acc) -> do_register(Acc, Pid) end, St, Pids);
do_register(St = #st{ids = Ids}, Pid) when is_pid(Pid) ->
    case maps:is_key(Pid, Ids) of
        true ->
            St;
        false ->
            %% A dead pid is not an error: a system under test declares its
            %% processes through `processes/1`, which is re-consulted after every
            %% operation, and short-lived client processes are routinely finished
            %% by then. `erlang:trace/3` raises `badarg` on a dead pid, so this
            %% would otherwise take the run down for something that is nothing.
            try
                erlang:trace(Pid, true, ?TRACE_FLAGS),
                erlang:suspend_process(Pid),
                adopt(St, Pid)
            catch
                error:badarg -> St
            end
    end.

%% A child that died before it could be suspended is still adopted, as exited.
%%
%% Skipping it looks harmless — there is nothing left to schedule — and is not: ids
%% are assigned in adoption order, so whether a child is skipped decides whether
%% every process after it keeps its id. A registry's OTP-spawned commit workers
%% are short enough to lose that race, and were observed adopted in one run
%% and gone in the next. Adopting either way makes id assignment independent of
%% liveness, which is the property; a dead process is never runnable, so the id
%% costs nothing else.
%%
%% Honest about the effect: this changed **nothing measurable** on the registry —
%% 8 distinct traces across 20 runs, with and without. The dominant cause is what
%% that same late-adopted child *does* before it is suspended, and it masks this
%% entirely. Kept because it is strictly more deterministic and will matter once
%% the dominant cause is gone, not because it was seen to help.
adopt_exited(St, Child) ->
    St1 = adopt(St, Child),
    Id = maps:get(Child, St1#st.ids),
    St1#st{exited = (St1#st.exited)#{Id => true}}.

-spec adopt(#st{}, pid()) -> #st{}.
adopt(St = #st{procs = Procs, ids = Ids, next_id = Id}, Pid) ->
    St#st{
        procs = Procs#{Id => Pid},
        ids = Ids#{Pid => Id},
        next_id = Id + 1
    }.

do_release(St = #st{procs = Procs, gated = Gated}) ->
    %% Anything still waiting for its token would block forever otherwise.
    maps:foreach(fun(Pid, true) -> catch Pid ! ?GO end, Gated),
    maps:foreach(
        fun(_Id, Pid) ->
            case erlang:is_process_alive(Pid) of
                true ->
                    erlang:trace(Pid, false, ?TRACE_FLAGS),
                    safe_resume(Pid);
                false ->
                    ok
            end
        end,
        Procs
    ),
    St#st{procs = #{}, ids = #{}, gated = #{}}.

%% ---------------------------------------------------------------------------
%% Stepping
%% ---------------------------------------------------------------------------

-spec runnable_ids(#st{}) -> [id()].
runnable_ids(St = #st{procs = Procs}) ->
    lists:sort([Id || {Id, Pid} <- maps:to_list(Procs), is_runnable(St, Id, Pid)]).

-spec is_runnable(#st{}, id(), pid()) -> boolean().
is_runnable(#st{exited = Exited, blocked_at = Blocked}, Id, Pid) ->
    case maps:is_key(Id, Exited) orelse not erlang:is_process_alive(Pid) of
        true ->
            false;
        false ->
            %% Default 0: an empty mailbox is nothing to do. Stepping such a
            %% process is harmless but wastes a choice and pollutes the schedule.
            queue_len(Pid) > maps:get(Id, Blocked, 0)
    end.

-spec do_step(#st{}, id()) -> {outcome(), #st{}}.
do_step(St0 = #st{procs = Procs}, Id) ->
    Pid = maps:get(Id, Procs),
    QueueBefore = queue_len(Pid),
    safe_resume(Pid),
    St1 = await_quiescent(St0, Pid),
    St2 = St1#st{steps = St1#st.steps + 1, choices = [Id | St1#st.choices]},
    case status(Pid) of
        dead ->
            {exited, St2#st{exited = (St2#st.exited)#{Id => true}}};
        _ ->
            erlang:suspend_process(Pid),
            {SelfSends, St3} = drain_trace(St2, Pid),
            QueueAfter = queue_len(Pid),
            Consumed = QueueBefore + SelfSends - QueueAfter,
            %% The step ended with the process blocked, and a receive blocks only
            %% after failing to match *every* queued message — so whatever is left
            %% is known not to match, whether or not the step consumed anything
            %% first. Record the block unconditionally. (Recording it only on an
            %% unproductive step would leave a process that consumed one message
            %% and blocked on the rest looking runnable, costing a wasted step
            %% every time round.)
            St4 = St3#st{blocked_at = (St3#st.blocked_at)#{Id => QueueAfter}},
            Outcome =
                case Consumed > 0 of
                    true -> progress;
                    false -> no_progress
                end,
            {Outcome, St4}
    end.

%% Wait for the process to run and then block again.  See the module doc on why
%% the `in` event must be awaited before status is trusted.
-spec await_quiescent(#st{}, pid()) -> #st{}.
await_quiescent(St, Pid) ->
    case await_event(St, Pid, in, ?MAX_EVENTS_PER_STEP) of
        {exited, St1} -> St1;
        {timeout, St1} -> St1;
        {ok, St1} -> await_block(St1, Pid, ?MAX_EVENTS_PER_STEP)
    end.

-spec await_block(#st{}, pid(), non_neg_integer()) -> #st{}.
await_block(St, Pid, Budget) when Budget > 0 ->
    case await_event(St, Pid, out, Budget) of
        {exited, St1} ->
            St1;
        {timeout, St1} ->
            St1;
        {ok, St1} ->
            %% Scheduled out — but that also happens on preemption, so only a
            %% `waiting` status means it is genuinely blocked in a receive.
            case status(Pid) of
                waiting -> St1;
                dead -> St1;
                _ -> await_block(St1, Pid, Budget - 1)
            end
    end;
await_block(St, Pid, _Budget) ->
    logger:warning("dst_sched: ~p never blocked; suspending it mid-flight", [Pid]),
    St.

%% Consume trace messages until `Want` (`in` or `out`) arrives for this pid.
%%
%% Every clause matches a trace-shaped message and nothing else. The scheduler now
%% has its own mailbox, so the only other traffic here is its own API calls — but
%% eating one of those would be just as confusing as eating a reply used to be.
-spec await_event(#st{}, pid(), in | out, non_neg_integer()) ->
    {ok | exited | timeout, #st{}}.
await_event(St, Pid, Want, Budget) when Budget > 0 ->
    receive
        {trace, Pid, Want, _} ->
            {ok, St};
        {trace, Pid, out_exited, _} ->
            {exited, St};
        {trace, Pid, exit, _Reason} ->
            {exited, handle_trace(St, {trace, Pid, exit, normal})};
        {trace, _, _, _} = T ->
            await_event(handle_trace(St, T), Pid, Want, Budget - 1);
        {trace, _, _, _, _} = T ->
            await_event(handle_trace(St, T), Pid, Want, Budget - 1)
    after ?EVENT_TIMEOUT ->
        %% No event is coming. Either the process already passed this point
        %% before we started listening, or it is dead.
        case status(Pid) of
            dead -> {exited, St};
            _ -> {timeout, St}
        end
    end;
await_event(St, _Pid, _Want, _Budget) ->
    {timeout, St}.

%% Collect this tracee's pending trace messages, synchronised with
%% erlang:trace_delivered/1 so a step never races the trace stream.  Returns how
%% many messages the process sent to *itself* during the step — the only way its
%% mailbox can grow while it holds the CPU alone, and so the correction term the
%% consumption count needs.
-spec drain_trace(#st{}, pid()) -> {non_neg_integer(), #st{}}.
drain_trace(St, Pid) ->
    Ref = erlang:trace_delivered(Pid),
    do_drain(St, Pid, Ref, 0).

-spec do_drain(#st{}, pid(), reference(), non_neg_integer()) ->
    {non_neg_integer(), #st{}}.
do_drain(St, Pid, Ref, SelfSends) ->
    receive
        {trace_delivered, Pid, Ref} ->
            {SelfSends, St};
        {trace, Pid, send, _Msg, Pid} ->
            do_drain(St, Pid, Ref, SelfSends + 1);
        {trace, _, _, _} = T ->
            do_drain(handle_trace(St, T), Pid, Ref, SelfSends);
        {trace, _, _, _, _} = T ->
            do_drain(handle_trace(St, T), Pid, Ref, SelfSends)
    after ?DELIVERED_TIMEOUT ->
        logger:warning("dst_sched: trace_delivered timed out for ~p", [Pid]),
        {SelfSends, St}
    end.

%% Trace events that are not about the step in progress: adopt spawned children,
%% note exits, ignore the rest.
-spec handle_trace(#st{}, tuple()) -> #st{}.
%% A gated child (see `spawn/1`) is already blocked, so there is no race to lose:
%% adopt it and hand it its token. It does not count as adopted late, because it
%% has not run.
handle_trace(
    St = #st{ids = Ids, gated = Gated}, {trace, _Parent, spawn, Child, {?MODULE, gated, _}}
) ->
    case maps:is_key(Child, Ids) of
        true ->
            St;
        false ->
            try
                erlang:suspend_process(Child),
                St1 = adopt(St, Child),
                Child ! ?GO,
                St1#st{gated = Gated#{Child => true}}
            catch
                error:badarg -> St
            end
    end;
handle_trace(St = #st{ids = Ids}, {trace, _Parent, spawn, Child, _MFA}) ->
    case maps:is_key(Child, Ids) of
        true ->
            St;
        false ->
            %% set_on_spawn already applied our trace flags, so the child is
            %% traced but running. Suspend it as fast as we can and record that
            %% the window existed.
            %%
            %% It may already be gone: a system that spawns short-lived helpers —
            %% a registry member's gather and snapshot workers, a generic server's
            %% transaction workers — routinely finishes one before the spawn event
            %% has even been handled. `suspend_process/1` raises `badarg` on a dead
            %% pid, which would take the scheduler down for something that is
            %% nothing. There is nothing left to schedule, so it is not adopted.
            try
                erlang:suspend_process(Child),
                St1 = adopt(St, Child),
                St1#st{adopted_late = St#st.adopted_late + 1}
            catch
                error:badarg ->
                    adopt_exited(St, Child)
            end
    end;
handle_trace(St = #st{ids = Ids, exited = Exited}, {trace, Pid, exit, _Reason}) ->
    case maps:find(Pid, Ids) of
        {ok, Id} -> St#st{exited = Exited#{Id => true}};
        error -> St
    end;
%% Every other trace event (in, out, 'receive', send elsewhere, link, …) is not
%% scheduling-relevant.
handle_trace(St, {trace, _Pid, _Event, _A}) ->
    St;
handle_trace(St, {trace, _Pid, _Event, _A, _B}) ->
    St.

%% ---------------------------------------------------------------------------
%% Driving
%% ---------------------------------------------------------------------------

-spec do_run(#st{}, non_neg_integer(), fun(() -> boolean())) -> #st{}.
do_run(St, 0, _OnIdle) ->
    St;
do_run(St, MaxSteps, OnIdle) ->
    case runnable_ids(St) of
        [] ->
            case OnIdle() of
                true -> do_run(St, MaxSteps - 1, OnIdle);
                false -> St
            end;
        Ids ->
            {I, Rand} = rand:uniform_s(length(Ids), St#st.rand),
            Id = lists:nth(I, Ids),
            {_Outcome, St1} = do_step(St#st{rand = Rand}, Id),
            do_run(St1, MaxSteps - 1, OnIdle)
    end.

-spec do_replay(#st{}, [id()]) -> {ok | {error, {diverged, id(), [id()]}}, #st{}}.
do_replay(St, []) ->
    {ok, St};
do_replay(St, [Id | Rest]) ->
    case lists:member(Id, runnable_ids(St)) of
        true ->
            {_Outcome, St1} = do_step(St, Id),
            do_replay(St1, Rest);
        false ->
            {{error, {diverged, Id, runnable_ids(St)}}, St}
    end.

%% ---------------------------------------------------------------------------
%% Process introspection
%% ---------------------------------------------------------------------------

-spec queue_len(pid()) -> non_neg_integer().
queue_len(Pid) ->
    case erlang:process_info(Pid, message_queue_len) of
        {message_queue_len, N} -> N;
        undefined -> 0
    end.

-spec status(pid()) -> atom().
status(Pid) ->
    case erlang:process_info(Pid, status) of
        {status, Status} -> Status;
        undefined -> dead
    end.

%% A process may already be dead, or not suspended at all if something outside
%% the scheduler touched it.
-spec safe_resume(pid()) -> ok.
safe_resume(Pid) ->
    try
        erlang:resume_process(Pid),
        ok
    catch
        _:_ -> ok
    end.
