-module(eta_sched).
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

It also matters for the driver. A `eta_run`-style helper that waits on a
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
  unless its module declares `-eta_after(true)` alongside `eta_transform`, which
  puts the timeout on the virtual clock. Without it, a system under test must
  avoid real-time-dependent receives (use `infinity` in simulation).
- **Spawn races.** A child spawned with a plain `erlang:spawn` is adopted via
  `set_on_spawn` tracing, which reports the spawn *after* the child exists — so it
  runs briefly, on the real scheduler, before it is suspended. `stats/1`'s
  `adopted_late` counts these. `spawn/1` and friends close the window by starting
  the child blocked; `eta_transform` points a transformed module's spawns there, so
  a system under simulation should show `adopted_late` at or near zero. A run with
  a high count is a run whose interleaving is partly wall-clock.
- **Steps that never finished.** A step ends when the process blocks in a receive.
  If no scheduling event arrives for it at all, the scheduler eventually suspends
  it wherever it happens to be, and that suspension point was chosen by wall clock
  rather than by the schedule. `stats/1`'s `timeouts` counts these and a warning is
  logged for each. It should be 0; anything else means the run is not replayable.

  The wait is deliberately generous and condition-based rather than a deadline —
  the scheduler keeps waiting while the process is alive and not suspended, since
  the event is then in flight. Only a process that is dead, or that did not resume
  at all, ends a step this way in practice.

*This documentation is LLM-generated. See the AI disclosure in `README.md`.*
""".
-endif.

-export([
    new/0,
    new/1,
    register/2,
    release/1,
    ids/1,
    procs/1,
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

%% Scheduler-owned spawning — what `eta_transform` points a system's `spawn` calls
%% at. `gated/1,3` are exported because they are spawned by MFA, which is what
%% makes a gated child distinguishable in the spawn trace event.
-export([
    active/0,
    current/0,
    stepping/0,
    spawn/1, spawn/3,
    spawn_link/1, spawn_link/3,
    spawn_monitor/1, spawn_monitor/3,
    spawn_opt/2, spawn_opt/4,
    gated/1, gated/3
]).

%% The distributed forms, which raise while a run is active. See `no_dist/3`.
-export([
    spawn/2, spawn/4,
    spawn_link/2, spawn_link/4,
    spawn_monitor/2, spawn_monitor/4,
    spawn_opt/3, spawn_opt/5,
    plib_spawn/2, plib_spawn/4,
    plib_spawn_link/2, plib_spawn_link/4,
    plib_spawn_opt/3, plib_spawn_opt/5
]).

%% The `proc_lib` flavours, which additionally give the child the OTP process
%% dictionary entries. See `plib_spawn/1`.
-export([
    plib_spawn/1, plib_spawn/3,
    plib_spawn_link/1, plib_spawn_link/3,
    plib_spawn_opt/2, plib_spawn_opt/4,
    gated_plib/2, gated_plib/4
]).

%% Gated `gen_server` starts — see `start_monitor/3`. `gated/5` is spawned by MFA
%% for the same reason `gated/1,3` are: the spawn trace event has to name it.
-export([
    start_monitor/3, start_monitor/4,
    start_link/3, start_link/4,
    start/3, start/4,
    gated/5
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-export_type([sched/0, id/0, outcome/0]).

%% `running` gives the in/out scheduling events quiescence is detected from.
%% `send` lets a step count self-sends, the correction term consumption needs.
%% `procs` + `set_on_spawn` adopt processes the system under test creates.
-define(TRACE_FLAGS, [running, 'receive', send, procs, set_on_spawn]).

%% A step should never take this many scheduling events; bailing out beats hanging
%% a test suite on a process that never blocks.
-define(MAX_EVENTS_PER_STEP, 100000).

%% How long to wait for a scheduling event before looking at the process to see
%% whether one is still coming. Only paid when the event has not landed yet; the
%% common path returns the moment it does.
-define(EVENT_POLL, 5).

%% How many of those polls before giving up on an event entirely.
%%
%% This used to be a single 5ms deadline, and that was a determinism hole in the
%% middle of the machinery that exists to remove them. A trace event delayed past
%% 5ms by a loaded machine or a GC pause made the scheduler conclude the process
%% would never run, suspend it wherever it happened to be, and carry on — so the
%% suspension point was chosen by wall clock. Nothing reported it.
%%
%% Polling instead makes the wait a question about the process rather than about
%% the clock: keep waiting while it is alive and not suspended, since the event
%% is then either in flight or imminent. The bound survives only to stop a
%% genuinely wedged process hanging a test suite, which is why it is generous.
-define(EVENT_PATIENCE, 200).

%% Bound on waiting for erlang:trace_delivered/1 to report the trace stream drained.
-define(DELIVERED_TIMEOUT, 5000).

%% Presence of this table means a scheduler is running, so `spawn/1` and friends
%% gate their children. Created by the scheduler process and destroyed with it, so
%% the inertness is automatic — the same arrangement `eta_time` uses.
-define(ACTIVE, eta_sched_active).

%% What releases a gated child once the scheduler owns it.
-define(GO, '$eta_sched_go').

-type id() :: non_neg_integer().
-type outcome() :: progress | no_progress | exited.
-opaque sched() :: {eta_sched, pid()}.

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
    %% Steps that ended without the process reaching a receive. See
    %% `note_quiescence/5`; any nonzero value means the run is not replayable.
    timeouts = 0 :: non_neg_integer(),
    steps = 0 :: non_neg_integer()
}).

%% A step is internally bounded (?MAX_EVENTS_PER_STEP, ?EVENT_PATIENCE,
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
    {eta_sched, Pid}.

-if(?DOCATTRS).
-doc "The scheduler's pid. For tests that need to assert on the process itself.".
-endif.
-spec pid(sched()) -> pid().
pid({eta_sched, Pid}) ->
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
release(S = {eta_sched, Pid}) ->
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
The process the scheduler is stepping right now, or `undefined` between steps.

Exactly one process runs during a step — every other one the scheduler owns is
suspended — so this is the sharpest available statement of "on the schedule". A
side effect caused by any other process while this is set happened at a moment
wall-clock timing chose, not one the seed did.

`undefined` is not the opposite of that. Between steps the driver itself is
running, and a harness injecting an operation there is doing something the trace
records; nothing is off-schedule merely because no step is in progress.

Read out of band, from ETS, because the answer is wanted by processes that could
not ask for it: the scheduler is a `gen_server` and a call into it from inside a
step would deadlock against the step.
""".
-endif.
-spec stepping() -> pid() | undefined.
stepping() ->
    try ets:lookup(?ACTIVE, stepping) of
        [{stepping, Pid}] -> Pid;
        [] -> undefined
    catch
        error:badarg -> undefined
    end.

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
carries `{eta_sched, gated, _}` — that is how the scheduler tells a gated child from
one it must chase.

Inert with no scheduler running, exactly as `eta_time` is with no clock, so a
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

-spec spawn_monitor(fun(() -> term())) -> {pid(), reference()}.
spawn_monitor(Fun) ->
    case active() of
        false -> erlang:spawn_monitor(Fun);
        true -> erlang:spawn_monitor(?MODULE, gated, [Fun])
    end.

-spec spawn_monitor(module(), atom(), [term()]) -> {pid(), reference()}.
spawn_monitor(M, F, A) ->
    case active() of
        false -> erlang:spawn_monitor(M, F, A);
        true -> erlang:spawn_monitor(?MODULE, gated, [M, F, A])
    end.

%% The options are passed straight through, so `link`, `monitor`, heap sizes and
%% the rest keep their meanings — including the return shape, which is a bare pid
%% unless `monitor` is asked for.
-spec spawn_opt(fun(() -> term()), list()) -> pid() | {pid(), reference()}.
spawn_opt(Fun, Opts) ->
    case active() of
        false -> erlang:spawn_opt(Fun, Opts);
        true -> erlang:spawn_opt(?MODULE, gated, [Fun], Opts)
    end.

-spec spawn_opt(module(), atom(), [term()], list()) -> pid() | {pid(), reference()}.
spawn_opt(M, F, A, Opts) ->
    case active() of
        false -> erlang:spawn_opt(M, F, A, Opts);
        true -> erlang:spawn_opt(?MODULE, gated, [M, F, A], Opts)
    end.

%% ---------------------------------------------------------------------------
%% proc_lib flavours
%% ---------------------------------------------------------------------------

%% `proc_lib`'s spawns differ from `erlang`'s in one way that matters: the child
%% gets `$ancestors` and `$initial_call`, without which it is not an OTP process
%% and `sys` cannot introspect it. Routing them through the plain gated spawn
%% would drop that, so these carry it, and the inert path stays inside `proc_lib`
%% rather than falling through to `erlang`.
-spec plib_spawn(fun(() -> term())) -> pid().
plib_spawn(Fun) ->
    case active() of
        false -> proc_lib:spawn(Fun);
        true -> erlang:spawn(?MODULE, gated_plib, [Fun, plib_ctx(Fun)])
    end.

-spec plib_spawn(module(), atom(), [term()]) -> pid().
plib_spawn(M, F, A) ->
    case active() of
        false -> proc_lib:spawn(M, F, A);
        true -> erlang:spawn(?MODULE, gated_plib, [M, F, A, plib_ctx(M, F, A)])
    end.

-spec plib_spawn_link(fun(() -> term())) -> pid().
plib_spawn_link(Fun) ->
    case active() of
        false -> proc_lib:spawn_link(Fun);
        true -> erlang:spawn_link(?MODULE, gated_plib, [Fun, plib_ctx(Fun)])
    end.

-spec plib_spawn_link(module(), atom(), [term()]) -> pid().
plib_spawn_link(M, F, A) ->
    case active() of
        false -> proc_lib:spawn_link(M, F, A);
        true -> erlang:spawn_link(?MODULE, gated_plib, [M, F, A, plib_ctx(M, F, A)])
    end.

-spec plib_spawn_opt(fun(() -> term()), list()) -> pid() | {pid(), reference()}.
plib_spawn_opt(Fun, Opts) ->
    case active() of
        false -> proc_lib:spawn_opt(Fun, Opts);
        true -> erlang:spawn_opt(?MODULE, gated_plib, [Fun, plib_ctx(Fun)], Opts)
    end.

-spec plib_spawn_opt(module(), atom(), [term()], list()) -> pid() | {pid(), reference()}.
plib_spawn_opt(M, F, A, Opts) ->
    case active() of
        false -> proc_lib:spawn_opt(M, F, A, Opts);
        true -> erlang:spawn_opt(?MODULE, gated_plib, [M, F, A, plib_ctx(M, F, A)], Opts)
    end.

%% ---------------------------------------------------------------------------
%% The distributed forms
%% ---------------------------------------------------------------------------

%% `spawn(Node, Fun)` and its relatives ask for a process on another node, and
%% `eta` does not simulate distribution. There is no honest rewrite: running the
%% child here would silently put it on the wrong node, and letting it through
%% would put it outside the schedule entirely. So while a run is active these
%% raise, which turns a system that cannot be simulated into a loud failure at
%% the call rather than a quiet loss of determinism 200 steps later.
%%
%% With no run in progress they delegate, so a module built with the transform
%% still works normally outside a simulation.
spawn(N, F) -> no_dist(active(), {erlang, spawn, 2}, fun() -> erlang:spawn(N, F) end).
spawn(N, M, F, A) -> no_dist(active(), {erlang, spawn, 4}, fun() -> erlang:spawn(N, M, F, A) end).
spawn_link(N, F) ->
    no_dist(active(), {erlang, spawn_link, 2}, fun() -> erlang:spawn_link(N, F) end).

spawn_link(N, M, F, A) ->
    no_dist(active(), {erlang, spawn_link, 4}, fun() -> erlang:spawn_link(N, M, F, A) end).

spawn_monitor(N, F) ->
    no_dist(active(), {erlang, spawn_monitor, 2}, fun() -> erlang:spawn_monitor(N, F) end).

spawn_monitor(N, M, F, A) ->
    no_dist(active(), {erlang, spawn_monitor, 4}, fun() -> erlang:spawn_monitor(N, M, F, A) end).

spawn_opt(N, F, O) ->
    no_dist(active(), {erlang, spawn_opt, 3}, fun() -> erlang:spawn_opt(N, F, O) end).

spawn_opt(N, M, F, A, O) ->
    no_dist(active(), {erlang, spawn_opt, 5}, fun() -> erlang:spawn_opt(N, M, F, A, O) end).

plib_spawn(N, F) -> no_dist(active(), {proc_lib, spawn, 2}, fun() -> proc_lib:spawn(N, F) end).

plib_spawn(N, M, F, A) ->
    no_dist(active(), {proc_lib, spawn, 4}, fun() -> proc_lib:spawn(N, M, F, A) end).

plib_spawn_link(N, F) ->
    no_dist(active(), {proc_lib, spawn_link, 2}, fun() -> proc_lib:spawn_link(N, F) end).

plib_spawn_link(N, M, F, A) ->
    no_dist(active(), {proc_lib, spawn_link, 4}, fun() -> proc_lib:spawn_link(N, M, F, A) end).

plib_spawn_opt(N, F, O) ->
    no_dist(active(), {proc_lib, spawn_opt, 3}, fun() -> proc_lib:spawn_opt(N, F, O) end).

plib_spawn_opt(N, M, F, A, O) ->
    no_dist(active(), {proc_lib, spawn_opt, 5}, fun() -> proc_lib:spawn_opt(N, M, F, A, O) end).

no_dist(false, _MFA, Delegate) ->
    Delegate();
no_dist(true, MFA, _Delegate) ->
    error(
        {eta_sched,
            {no_distribution, MFA,
                <<"eta does not simulate distribution; run the nodes as processes in one VM">>}}
    ).

%% Computed in the *parent*, because `$ancestors` is the parent's list with the
%% parent on the front and the child cannot see it.
plib_ctx(Fun) ->
    {module, M} = erlang:fun_info(Fun, module),
    {name, N} = erlang:fun_info(Fun, name),
    {arity, A} = erlang:fun_info(Fun, arity),
    {[self() | own_ancestors()], {M, N, A}}.

plib_ctx(M, F, A) ->
    {[self() | own_ancestors()], {M, F, length(A)}}.

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

-if(?DOCATTRS).
-doc false.
-endif.
gated_plib(Fun, {Ancestors, InitialCall}) ->
    _ = wait_for_token(),
    put('$ancestors', Ancestors),
    put('$initial_call', InitialCall),
    Fun().

-if(?DOCATTRS).
-doc false.
-endif.
gated_plib(M, F, A, {Ancestors, InitialCall}) ->
    _ = wait_for_token(),
    put('$ancestors', Ancestors),
    put('$initial_call', InitialCall),
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
        true -> gated_start(undefined, Mod, Args, monitor)
    end.

-spec start_link(module(), term(), list()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Mod, Args, Options) ->
    case active() of
        false -> gen_server:start_link(Mod, Args, Options);
        true -> gated_start(undefined, Mod, Args, link)
    end.

-spec start(module(), term(), list()) -> {ok, pid()} | ignore | {error, term()}.
start(Mod, Args, Options) ->
    case active() of
        false -> gen_server:start(Mod, Args, Options);
        true -> gated_start(undefined, Mod, Args, plain)
    end.

%% The 4-arity forms, which name the server. Registration happens in the *child*,
%% before `init/1`, which is where `gen` does it and is what makes
%% `{error, {already_started, Pid}}` mean what it means.
-spec start_monitor(gen_server:server_name(), module(), term(), list()) ->
    {ok, {pid(), reference()}} | ignore | {error, term()}.
start_monitor(Name, Mod, Args, Options) ->
    case active() of
        false -> gen_server:start_monitor(Name, Mod, Args, Options);
        true -> gated_start(Name, Mod, Args, monitor)
    end.

-spec start_link(gen_server:server_name(), module(), term(), list()) ->
    {ok, pid()} | ignore | {error, term()}.
start_link(Name, Mod, Args, Options) ->
    case active() of
        false -> gen_server:start_link(Name, Mod, Args, Options);
        true -> gated_start(Name, Mod, Args, link)
    end.

-spec start(gen_server:server_name(), module(), term(), list()) ->
    {ok, pid()} | ignore | {error, term()}.
start(Name, Mod, Args, Options) ->
    case active() of
        false -> gen_server:start(Name, Mod, Args, Options);
        true -> gated_start(Name, Mod, Args, plain)
    end.

gated_start(Name, Mod, Args, Kind) ->
    Parent = self(),
    Ancestors = [Parent | own_ancestors()],
    MFA = [Parent, Ancestors, Name, Mod, Args],
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
        {'$eta_started', Pid, {ok, Pid}} ->
            case Kind of
                monitor -> {ok, {Pid, Ref}};
                _ -> {ok, Pid}
            end;
        {'$eta_started', Pid, Other} ->
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
gated(Parent, Ancestors, Name, Mod, Args) ->
    _ = wait_for_token(),
    %% What proc_lib:init_p/3 would have set. Without these the process is not an
    %% OTP process: no crash-report formatting, and `sys` cannot introspect it.
    put('$ancestors', Ancestors),
    put('$initial_call', {Mod, init, 1}),
    case register_name(Name) of
        ok ->
            gated_init(Parent, Name, Mod, Args);
        {error, _} = Err ->
            Parent ! {'$eta_started', self(), Err},
            exit(normal)
    end.

%% `{local, N}` only. `{global, _}` and `{via, _, _}` both register through a
%% service process the scheduler does not own, so a start would block on
%% something outside the schedule — the same class of real-time dependency as
%% `code_server`. Refused rather than delegated, because delegating would produce
%% an ungated child and a run that quietly stopped being reproducible.
register_name(undefined) ->
    ok;
register_name({local, N}) ->
    try
        true = erlang:register(N, self()),
        ok
    catch
        error:badarg -> {error, {already_started, whereis(N)}}
    end;
register_name(Other) ->
    {error, {eta_sched, unsupported_server_name, Other}}.

gated_init(Parent, Name, Mod, Args) ->
    case Mod:init(Args) of
        {ok, State} ->
            Parent ! {'$eta_started', self(), {ok, self()}},
            enter_loop(Mod, Name, State);
        {ok, State, {continue, C}} ->
            Parent ! {'$eta_started', self(), {ok, self()}},
            gated_continue(Mod, Name, C, State);
        {stop, Reason} ->
            Parent ! {'$eta_started', self(), {error, Reason}},
            exit(Reason);
        ignore ->
            Parent ! {'$eta_started', self(), ignore},
            exit(normal);
        Other ->
            %% `{ok, State, Timeout}` and `{ok, State, hibernate}` land here.
            %% Both are real-clock behaviours that would need simulating, and
            %% guessing at them would be a stand-in that lies.
            Parent ! {'$eta_started', self(), {error, {eta_sched, unsupported_init, Other}}},
            exit({eta_sched, unsupported_init, Other})
    end.

%% A named server has to enter its loop knowing its name, or `gen_server` will not
%% answer calls addressed to the name.
enter_loop(Mod, undefined, State) -> gen_server:enter_loop(Mod, [], State);
enter_loop(Mod, Name, State) -> gen_server:enter_loop(Mod, [], State, Name).

%% `gen_server` runs handle_continue after the init acknowledgement and before any
%% message, and a continue may chain into another. `enter_loop/3` takes a state,
%% not a continue, so this is done here.
gated_continue(Mod, Name, C, State) ->
    case Mod:handle_continue(C, State) of
        {noreply, State1} -> enter_loop(Mod, Name, State1);
        {noreply, State1, {continue, C2}} -> gated_continue(Mod, Name, C2, State1);
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
Every known process, by id.

Ids are what a trace records and pids are what a caller recognises, so anything
that wants to say something about a recorded step needs this mapping. `eta_run`
uses it to turn `{step, 7}` into a name at teardown.

Includes processes that have exited, because a trace can name one.
""".
-endif.
-spec procs(sched()) -> #{id() => pid()}.
procs(S) ->
    call(S, procs).

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
Run statistics: steps taken, processes known, how many have exited, how many
children were adopted after they had already started running, and how many steps
ended without the process reaching a receive.

`adopted_late` and `timeouts` are both determinism failures and both should be
0. See the module doc's determinism boundary.
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
eta_sched:run(Sched, MaxSteps, fun eta_time:advance_to_next/0)
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

`OnIdle` runs **in the scheduler's process**, not the caller's. `eta_time` is ETS
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

call({eta_sched, Pid}, Request) ->
    gen_server:call(Pid, Request, ?CALL_TIMEOUT).

%% ---------------------------------------------------------------------------
%% Server
%% ---------------------------------------------------------------------------

init(Opts) ->
    Seed = maps:get(seed, Opts, 0),
    %% See `eta_time`'s `claim/3`. Unguarded, `ets:new/2` on an existing named
    %% table raises a bare `badarg` from inside this `init/1`, which is loud but
    %% says nothing about the cause.
    case ets:info(?ACTIVE, owner) of
        undefined ->
            ok;
        Other ->
            error(
                {eta_sched,
                    {scheduler_in_use, Other, <<
                        "one per VM; keep runs serial (`async: false`) and do "
                        "not drive two at once"
                    >>}}
            )
    end,
    _ = ets:new(?ACTIVE, [named_table, public, set]),
    true = ets:insert(?ACTIVE, {sched, {?MODULE, self()}}),
    {ok, #st{rand = rand:seed_s(exsss, {Seed, Seed + 1, Seed + 2})}}.

handle_call({register, Pids}, _From, St) ->
    {reply, ok, do_register(St, Pids)};
handle_call(release, _From, St) ->
    {reply, ok, do_release(St)};
handle_call(ids, _From, St = #st{procs = Procs}) ->
    {reply, lists:sort(maps:keys(Procs)), St};
handle_call(procs, _From, St = #st{procs = Procs}) ->
    {reply, Procs, St};
handle_call(runnable, _From, St) ->
    {reply, runnable_ids(St), St};
handle_call(choices, _From, St = #st{choices = Choices}) ->
    {reply, lists:reverse(Choices), St};
handle_call(stats, _From, St) ->
    #st{
        steps = Steps,
        procs = Procs,
        exited = Exited,
        adopted_late = Late,
        timeouts = Timeouts
    } = St,
    Reply = #{
        steps => Steps,
        processes => maps:size(Procs),
        exited => maps:size(Exited),
        adopted_late => Late,
        timeouts => Timeouts
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
do_register(St = #st{ids = Ids, gated = Gated}, Pid) when is_pid(Pid) ->
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
                St1 = adopt(St, Pid),
                %% A gated child is waiting for a token only the scheduler can
                %% send. When its parent is traced, the spawn event delivers it;
                %% when the parent is the driver — or anything else the scheduler
                %% does not own — there is no spawn event, and this is the only
                %% route. Deliberately the same three steps as `handle_trace`,
                %% reached the other way round.
                case is_gated(Pid) andalso not maps:is_key(Pid, Gated) of
                    true ->
                        Pid ! ?GO,
                        St1#st{gated = Gated#{Pid => true}};
                    false ->
                        St1
                end
            catch
                error:badarg -> St
            end
    end.

%% The VM's own record of how the process was started, not the `$initial_call`
%% in its dictionary — `proc_lib` overwrites the latter, and this has to survive
%% that.
is_gated(Pid) ->
    case erlang:process_info(Pid, initial_call) of
        {initial_call, {?MODULE, gated, _}} -> true;
        {initial_call, {?MODULE, gated_plib, _}} -> true;
        _ -> false
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
            %% `process_info/2` rather than `is_process_alive/1`, for the reason
            %% in `is_runnable/3`: these are still suspended when this runs.
            case status(Pid) =/= dead of
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

%% Liveness is read with `process_info/2` rather than `is_process_alive/1`, and it
%% matters more here than anywhere else: this runs for every process at every
%% choice point. `is_process_alive/1` is signal-based — it asks the target and
%% waits for a reply the target produces by *running* — and every process here is
%% one this scheduler owns and has suspended, several of which it has already sent
%% a `?GO` token. Asking them would make the scheduler's own progress depend on
%% when it next resumed them. `process_info/2` reads the target directly, which is
%% how `queue_len/1` two lines down has always done it.
-spec is_runnable(#st{}, id(), pid()) -> boolean().
is_runnable(#st{exited = Exited, blocked_at = Blocked}, Id, Pid) ->
    case maps:is_key(Id, Exited) orelse status(Pid) =:= dead of
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
    %% Published for the duration of the step, and erased after it. While a step
    %% is in progress this is the *only* process that is supposed to be running,
    %% so anything else that acts during it is acting outside the schedule.
    %% `eta_net` uses it to tell an on-schedule send from an off-schedule one; see
    %% `stepping/0`.
    true = ets:insert(?ACTIVE, {stepping, Pid}),
    safe_resume(Pid),
    {Quiescence, St1} = await_quiescent(St0, Pid),
    true = ets:delete(?ACTIVE, stepping),
    St2 = St1#st{steps = St1#st.steps + 1, choices = [Id | St1#st.choices]},
    %% Reading the status and then acting on it is a read followed by an action on
    %% something that can change in between, so the two are taken as one: hold the
    %% process first, and let a failure to hold it *be* the death.
    %%
    %% Splitting them was a latent crash. A step ends when the process stops being
    %% runnable, which is usually a receive but is equally the last thing it does
    %% before returning — so a short-lived worker (a registry member's gather
    %% helper, a transaction worker) routinely dies in the window between the two
    %% calls. `suspend_process/1` raises `badarg` on a dead pid, which took the
    %% scheduler down for something that is nothing. Every other call site in this
    %% module already guarded it; this one did not, and a system spawning such
    %% workers under an injected node fault hit it about one run in three.
    %%
    %% The trace is drained either way. It was not before, and that was the more
    %% consequential half: `trace_delivered/1` is the only thing that says the
    %% tracer has every event, so a process that spawned a child and then exited
    %% in the same step left the spawn event sitting in the mailbox, to be adopted
    %% whenever `handle_info/2` next ran rather than at a point the schedule
    %% fixes.
    case hold(Pid) of
        false ->
            {_SelfSends, StDead} = drain_trace(St2, Pid),
            {exited, StDead#st{exited = (StDead#st.exited)#{Id => true}}};
        true ->
            {SelfSends, St3} = drain_trace(St2, Pid),
            QueueAfter = queue_len(Pid),
            Consumed = QueueBefore + SelfSends - QueueAfter,
            St4 = note_quiescence(St3, Id, Pid, Quiescence, QueueAfter),
            Outcome =
                case Consumed > 0 of
                    true -> progress;
                    false -> no_progress
                end,
            {Outcome, St4}
    end.

%% Suspend a process, answering whether there was one to suspend. `false` means it
%% has exited, which is the only reason `suspend_process/1` raises here: the pid is
%% one this scheduler owns, so it is local, and nothing else suspends it.
-spec hold(pid()) -> boolean().
hold(Pid) ->
    try
        erlang:suspend_process(Pid),
        true
    catch
        error:badarg -> false
    end.

%% Where a process blocked, but only when it actually blocked.
%%
%% On a completed step the queue length is meaningful: a receive blocks only
%% after failing to match *every* queued message, so whatever is left is known
%% not to match, whether or not the step consumed anything first. Recording it
%% unconditionally is what makes selective receive work — recording it only on
%% an unproductive step would leave a process that consumed one message and
%% blocked on the rest looking runnable, costing a wasted step every time round.
%%
%% On a step that timed out, none of that holds. The process was suspended
%% wherever it happened to be rather than at a receive, so its mailbox says
%% nothing about what it has scanned. Recording a block it never reached would
%% mark it blocked at a queue length it may never grow past, stranding it for
%% the rest of the run. Leaving `blocked_at` alone keeps it runnable, so the
%% next step re-runs it and the schedule usually repairs itself.
-spec note_quiescence(#st{}, id(), pid(), blocked | timeout, non_neg_integer()) -> #st{}.
note_quiescence(St = #st{blocked_at = Blocked}, Id, _Pid, blocked, QueueAfter) ->
    St#st{blocked_at = Blocked#{Id => QueueAfter}};
note_quiescence(St = #st{timeouts = Timeouts}, _Id, Pid, timeout, _QueueAfter) ->
    logger:warning(
        "eta_sched: no scheduling event for ~p within ~wms, so it was suspended "
        "mid-step. This run's interleaving is partly wall clock and its seed will "
        "not reproduce. See stats/1's `timeouts`.",
        [Pid, ?EVENT_POLL * ?EVENT_PATIENCE]
    ),
    St#st{timeouts = Timeouts + 1}.

%% Wait for the process to run and then block again.  See the module doc on why
%% the `in` event must be awaited before status is trusted.
%%
%% `blocked` means the process reached a receive it cannot satisfy, which is what
%% a completed step is. `timeout` means it did not, and the caller must treat
%% everything it would otherwise conclude from the mailbox as unreliable.
-spec await_quiescent(#st{}, pid()) -> {blocked | timeout, #st{}}.
await_quiescent(St, Pid) ->
    case await_event(St, Pid, in, ?MAX_EVENTS_PER_STEP) of
        %% A process that exited did finish; there is nothing left to strand.
        {exited, St1} -> {blocked, St1};
        {timeout, St1} -> {timeout, St1};
        {ok, St1} -> await_block(St1, Pid, ?MAX_EVENTS_PER_STEP)
    end.

-spec await_block(#st{}, pid(), non_neg_integer()) -> {blocked | timeout, #st{}}.
await_block(St, Pid, Budget) when Budget > 0 ->
    case await_event(St, Pid, out, Budget) of
        {exited, St1} ->
            {blocked, St1};
        {timeout, St1} ->
            {timeout, St1};
        {ok, St1} ->
            %% Scheduled out — but that also happens on preemption, so only a
            %% `waiting` status means it is genuinely blocked in a receive.
            case status(Pid) of
                waiting -> {blocked, St1};
                dead -> {blocked, St1};
                _ -> await_block(St1, Pid, Budget - 1)
            end
    end;
await_block(St, Pid, _Budget) ->
    logger:warning("eta_sched: ~p never blocked; suspending it mid-flight", [Pid]),
    {timeout, St}.

%% Consume trace messages until `Want` (`in` or `out`) arrives for this pid.
%%
%% Every clause matches a trace-shaped message and nothing else. The scheduler now
%% has its own mailbox, so the only other traffic here is its own API calls — but
%% eating one of those would be just as confusing as eating a reply used to be.
-spec await_event(#st{}, pid(), in | out, non_neg_integer()) ->
    {ok | exited | timeout, #st{}}.
await_event(St, Pid, Want, Budget) ->
    await_event(St, Pid, Want, Budget, ?EVENT_PATIENCE).

-spec await_event(#st{}, pid(), in | out, non_neg_integer(), non_neg_integer()) ->
    {ok | exited | timeout, #st{}}.
await_event(St, Pid, Want, Budget, Patience) when Budget > 0, Patience > 0 ->
    receive
        {trace, Pid, Want, _} ->
            {ok, St};
        {trace, Pid, out_exited, _} ->
            {exited, St};
        {trace, Pid, exit, Reason} ->
            %% Reason is passed through rather than flattened to `normal`: nothing
            %% here reads it, but `eta_net` does — a simulated monitor's DOWN
            %% carries the real exit reason. See `eta_net:notify_exit/2`.
            {exited, handle_trace(St, {trace, Pid, exit, Reason})};
        {trace, _, _, _} = T ->
            await_event(handle_trace(St, T), Pid, Want, Budget - 1, Patience);
        {trace, _, _, _, _} = T ->
            await_event(handle_trace(St, T), Pid, Want, Budget - 1, Patience)
    after ?EVENT_POLL ->
        %% Nothing yet. Whether to keep waiting is a question about the process,
        %% not about how long we have waited.
        case status(Pid) of
            dead ->
                {exited, St};
            suspended ->
                %% The resume did not take, so this process is not going to run
                %% and no event is coming. The one case where giving up is right.
                {timeout, St};
            _ ->
                %% Alive and not suspended, so it is running, runnable, or about
                %% to be scheduled in. The event is in flight.
                await_event(St, Pid, Want, Budget, Patience - 1)
        end
    end;
await_event(St, _Pid, _Want, _Budget, _Patience) ->
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
        logger:warning("eta_sched: trace_delivered timed out for ~p", [Pid]),
        {SelfSends, St}
    end.

%% Trace events that are not about the step in progress: adopt spawned children,
%% note exits, ignore the rest.
-spec handle_trace(#st{}, tuple()) -> #st{}.
%% A gated child (see `spawn/1`) is already blocked, so there is no race to lose:
%% adopt it and hand it its token. It does not count as adopted late, because it
%% has not run.
handle_trace(
    St = #st{ids = Ids, gated = Gated}, {trace, Parent, spawn, Child, {?MODULE, F, _}}
) when F =:= gated; F =:= gated_plib ->
    %% A child belongs wherever its parent does. `eta_net` is inert if no network
    %% is running, and a no-op if the parent was never placed — see
    %% `eta_net:place/2` for why a topology that does not follow spawning is a
    %% topology that goes stale the first time the system creates a worker.
    ok = eta_net:inherit(Parent, Child),
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
handle_trace(St = #st{ids = Ids}, {trace, Parent, spawn, Child, _MFA}) ->
    ok = eta_net:inherit(Parent, Child),
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
handle_trace(St = #st{ids = Ids, exited = Exited}, {trace, Pid, exit, Reason}) ->
    %% The scheduler is the tracer for every process in a run, so this is the one
    %% place an exit is observed at a point the schedule fixes rather than
    %% whenever the BEAM happens to run a watcher. `eta_net` has no other source
    %% for it, and is inert if no network is running. See
    %% `eta_net:notify_exit/2`.
    ok = eta_net:notify_exit(Pid, Reason),
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
