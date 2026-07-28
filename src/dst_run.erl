-module(dst_run).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
The run driver — Phase 3 of the DST framework
(design: `docs/design.md`).

Turns a seed into a run: start the clock, start the system, then alternate between
injecting client operations, stepping one process at a time, and letting virtual
time pass — checking the invariants throughout, and recording enough to replay the
whole thing exactly.

The system under test supplies six callbacks; see `dst_sut`.

```erlang
#{outcome := ok} = dst_run:run(my_sut, #{seed => 7, max_ops => 40}).
```

## The three things a run can do, and why the choice matters

At every iteration the driver picks one of:

- **step** a runnable process, chosen from the seeded RNG;
- **inject** the next generated operation;
- **advance the clock** to the next timer deadline.

The interesting knob is what to do when *nothing is runnable*. Always injecting
there is the obvious choice and it is wrong: it means the system is never left
alone with only its own timers for company, and a whole class of defect lives
exactly there. A replicated registry's traffic-triggered resync gap only shows when
nothing else is happening — a follower that lost the tail of the replication
stream detects it on the next batch, so with a client always poking the system the
gap is papered over before it can be observed. `quiet_p` is the probability of
letting time pass instead, and it is the reason that class of bug is reachable.

## Determinism

Everything that varies is drawn from one seeded `rand` state: the scheduling
choices, the decision between stepping and injecting, and whatever `generate/2`
draws. Given the same seed and the same system, a run is reproducible.

Replay does not depend on that reproducibility. `replay/3` follows a recorded
trace — `{step, Id}`, `{op, Op}` and `{clock, Ms}` entries, in order — so a trace
that has been edited or shrunk replays as faithfully as one straight from a seed,
which is what Phase 4 needs. A recorded step naming a process that is not runnable
is reported as a divergence rather than skipped, because a "replay" that quietly
does something else is worse than no replay.

**All three of the driver's actions are entries**, letting time pass included. A
trace missing the clock advances is not a trace of a system that has timers: replay
walks only what it is given, so the clock never moves, nothing ever becomes
runnable, and every recorded step is refused. See `idle/1`.

## Ordering at startup

The clock starts before the system, always. A timer armed while `dst_time` is
inert goes to the real clock and stays there until it fires and re-arms, so
long-period timers — the interesting ones — would silently never be virtual. This
is why `init/2` is called by the driver rather than by the caller beforehand.
""".
-endif.

-export([run/2, replay/3, spawn_op/1]).

-export_type([entry/0, outcome/0, result/0]).

-type entry() :: {op, dst_sut:op()} | {step, dst_sched:id()} | {clock, integer()}.
-type outcome() ::
    ok
    | {violation, dst_sut:violation()}
    | {error, term()}.
-type result() :: #{
    outcome := outcome(),
    seed := integer(),
    trace := [entry()],
    steps := non_neg_integer(),
    ops := non_neg_integer(),
    clock_ms := integer(),
    %% Recorded steps a lenient replay had to skip; always 0 for a generated run
    %% or a strict replay. See `replay/3`.
    skipped := non_neg_integer(),
    sched := #{atom() => non_neg_integer()}
}.

%% An invariant reads a frozen system, so it should return immediately. Anything
%% slower is a `check/1` that called into a suspended process — see `dst_sut`.
-define(CHECK_TIMEOUT, 5000).

%% Where `spawn_op/1` parks pids until the driver has registered them. The driver's
%% own process dictionary, which is safe because `execute/2` runs there and nowhere
%% else — and private, because nothing outside this module reads the key.
-define(PENDING_OPS, '$dst_run_pending_ops').

-record(r, {
    mod :: module(),
    sut :: dst_sut:sut(),
    sched :: dst_sched:sched(),
    rand :: rand:state(),
    seed :: integer(),
    budget :: non_neg_integer(),
    ops_left :: non_neg_integer(),
    settle_left :: non_neg_integer(),
    op_p :: float(),
    quiet_p :: float(),
    check_every :: pos_integer(),
    lenient = false :: boolean(),
    skipped = 0 :: non_neg_integer(),
    since_check = 0 :: non_neg_integer(),
    trace = [] :: [entry()],
    steps = 0 :: non_neg_integer(),
    ops = 0 :: non_neg_integer()
}).

%% ---------------------------------------------------------------------------
%% API
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc """
Runs `Mod` under a seed and returns what happened.

Options, all with defaults:

- `seed` (0) — fixes the whole run.
- `config` (`#{}`) — passed through to `init/2`.
- `max_ops` (50) — how many operations to inject before letting the system settle.
- `max_steps` (10000) — the safety bound on steps *and* clock advances together;
  hitting it is reported as `{error, step_budget_exhausted}`.
- `settle_steps` (2000) — how much longer to run once every operation has been
  injected. A system whose timers never stop — anything with a heartbeat — has no
  natural quiescence to wait for, and without this the only
  way such a run can end is the safety bound. Reaching the end of the settle phase
  is a normal `ok`.
- `op_p` (0.3) — chance of injecting rather than stepping when both are possible.
- `quiet_p` (0.3) — chance of letting time pass rather than injecting when nothing
  is runnable. See the module doc; this is not a tuning knob so much as the thing
  that makes quiet-period bugs reachable.
- `check_every` (1) — run the invariants every N actions.
""".
-endif.
-spec run(module(), map()) -> result().
run(Mod, Opts) ->
    drive(Mod, Opts, generate).

-if(?DOCATTRS).
-doc """
Replays a recorded trace instead of generating one.

The seed still seeds the system, but no choices are drawn from it: every step and
every operation comes from `Trace`.

`lenient => true` skips a recorded step whose process is not runnable instead of
reporting a divergence, and counts them in the result's `skipped`. That is what
shrinking needs and what verification must not use: removing an operation strands
the steps that belonged to it, so a strict replay answers `diverged` for nearly
every candidate and the shrinker learns nothing. A lenient replay's own `trace` is
the schedule that actually ran, which is how `dst_shrink` recovers a trace that
replays strictly again.
""".
-endif.
-spec replay(module(), [entry()], map()) -> result().
replay(Mod, Trace, Opts) ->
    drive(Mod, Opts, {replay, Trace}).

-if(?DOCATTRS).
-doc """
Spawns a process to carry out an operation. Call this from `execute/2` rather than
`spawn/1`.

**A plain `spawn` is a determinism hole**, and a quiet one. The driver is not
traced, so a process it creates is not adopted by `dst_sched` until the driver
registers it — and in that window the new process runs on the real scheduler. One
op process racing alone is usually harmless; two of them, from operations injected
close together, race each other to deliver their first message, and the order they
arrive in is real-world timing rather than anything the seed controls. It shows up
as a seed producing two different traces, which is the framework failing at the one
thing it exists to do.

The process spawned here blocks before running `Fun`, so it cannot act before it is
owned. `dst_run` releases it once registration is done, and from that point the
scheduler decides when it runs, like every other process.
""".
-endif.
-spec spawn_op(fun(() -> term())) -> pid().
spawn_op(Fun) ->
    Pid = spawn(fun() ->
        receive
            {?MODULE, go} -> Fun()
        end
    end),
    put(?PENDING_OPS, [Pid | pending_ops()]),
    Pid.

pending_ops() ->
    case get(?PENDING_OPS) of
        undefined -> [];
        Pids -> Pids
    end.

%% Let the operation processes go, now that the scheduler owns them. Oldest first,
%% so the order matches the order `execute/2` created them in rather than the order
%% they happen to be stacked in.
release_ops() ->
    Pids = lists:reverse(pending_ops()),
    erase(?PENDING_OPS),
    [P ! {?MODULE, go} || P <- Pids],
    ok.

%% ---------------------------------------------------------------------------
%% Driving
%% ---------------------------------------------------------------------------

drive(Mod, Opts, Source) ->
    Seed = maps:get(seed, Opts, 0),
    %% Before the system, always — see the module doc.
    ok = dst_time:start(#{seed => Seed}),
    try
        {ok, Sut} = Mod:init(Seed, maps:get(config, Opts, #{})),
        Sched = dst_sched:new(#{seed => Seed}),
        R = #r{
            mod = Mod,
            sut = Sut,
            sched = dst_sched:register(Sched, Mod:processes(Sut)),
            rand = rand:seed_s(exsss, {Seed + 11, Seed + 23, Seed + 37}),
            seed = Seed,
            budget = maps:get(max_steps, Opts, 10000),
            ops_left = maps:get(max_ops, Opts, 50),
            settle_left = maps:get(settle_steps, Opts, 2000),
            op_p = maps:get(op_p, Opts, 0.3),
            quiet_p = maps:get(quiet_p, Opts, 0.3),
            check_every = maps:get(check_every, Opts, 1),
            lenient = maps:get(lenient, Opts, false)
        },
        {Outcome, R1} =
            case Source of
                generate -> loop(R);
                {replay, Trace} -> replay_loop(R, Trace)
            end,
        finish(Outcome, R1)
    after
        dst_time:stop()
    end.

%% The generated run. One action per iteration, invariants after each.
loop(R = #r{budget = 0}) ->
    {{error, step_budget_exhausted}, R};
%% Every operation is in and the settle budget is spent: a normal end.
%%
%% A system with periodic timers never reaches "nothing runnable and nothing
%% pending" — a replication heartbeat and a prune timer see to that, so an idle
%% cluster produces work forever and the only other terminal state is
%% the safety bound. Ending on a bounded settle phase makes quiescence-by-timer a
%% success rather than a budget error, and the phase is not dead time: it is where
%% the invariants get checked with no client traffic, which is the only way the
%% quiet-period defects are reachable at all.
loop(R = #r{ops_left = 0, settle_left = 0}) ->
    {ok, R};
loop(R = #r{sched = Sched, ops_left = OpsLeft}) ->
    case choose(R, dst_sched:runnable(Sched), OpsLeft > 0) of
        {step, Id, R1} -> after_action(do_step(R1, Id));
        {op, R1} -> after_action(do_op(R1));
        {idle, R1} -> idle(R1)
    end.

%% What to do next. See the module doc on why the nothing-runnable case is the
%% interesting one.
choose(R, [], false) ->
    {idle, R};
choose(R, [], true) ->
    case roll(R) of
        {P, R1} when P < R1#r.quiet_p -> {idle, R1};
        {_, R1} -> {op, R1}
    end;
choose(R, Runnable, false) ->
    pick_step(R, Runnable);
choose(R, Runnable, true) ->
    case roll(R) of
        {P, R1} when P < R1#r.op_p -> {op, R1};
        {_, R1} -> pick_step(R1, Runnable)
    end.

pick_step(R = #r{rand = Rand}, Runnable) ->
    {I, Rand1} = rand:uniform_s(length(Runnable), Rand),
    {step, lists:nth(I, Runnable), R#r{rand = Rand1}}.

roll(R = #r{rand = Rand}) ->
    {P, Rand1} = rand:uniform_s(Rand),
    {P, R#r{rand = Rand1}}.

%% Nothing is runnable: only the clock can produce more work. If it cannot, and
%% there are no operations left to inject, the run is over — that is quiescence,
%% and it is the normal way a run ends.
%%
%% **Letting time pass is an action, and it goes in the trace.** It was left out at
%% first on the reasoning that the clock is a function of the timers and the timers
%% are a function of the schedule, so a replay would advance in the same places by
%% itself. It does not: `replay_loop/2` only walks the entries it is given, so a
%% recorded run of a system with timers replayed against a clock that never moved.
%% Nothing became runnable, every recorded step was refused, and a strict replay
%% failed on entry 0 — which reads as a scheduling divergence and is nothing of the
%% sort. On a timer-driven system it made `dst_shrink` a no-op: the shrinker replays
%% original trace to classify it, got a clean run, and reported "nothing to shrink"
%% for a failure that had just happened.
idle(R = #r{ops_left = OpsLeft, trace = Trace}) ->
    case dst_time:advance_to_next() of
        true ->
            loop(spend(R#r{trace = [{clock, dst_time:now_ms()} | Trace]}));
        false when OpsLeft > 0 ->
            %% `quiet_p` chose to let time pass and there was no time to pass.
            after_action(do_op(R));
        false ->
            {ok, R}
    end.

do_step(R = #r{sched = Sched, trace = Trace, steps = Steps}, Id) ->
    {_Outcome, _} = dst_sched:step(Sched, Id),
    spend(R#r{trace = [{step, Id} | Trace], steps = Steps + 1}).

do_op(R) ->
    #r{mod = Mod, sut = Sut, rand = Rand} = R,
    {Op, Rand1} = Mod:generate(Sut, Rand),
    apply_op(R#r{rand = Rand1}, Op).

apply_op(R, Op) ->
    #r{mod = Mod, sut = Sut, sched = Sched, trace = Trace, ops = Ops} = R,
    Sut1 = Mod:execute(Op, Sut),
    %% An operation is issued by spawning; pick up whatever it created. Already
    %% known pids are ignored, so re-registering the whole set is correct.
    Sched1 = dst_sched:register(Sched, Mod:processes(Sut1)),
    %% Only now may those processes run — see `spawn_op/1`.
    ok = release_ops(),
    spend(R#r{
        sut = Sut1,
        sched = Sched1,
        trace = [{op, Op} | Trace],
        ops = Ops + 1,
        ops_left = R#r.ops_left - 1
    }).

spend(R = #r{budget = B, since_check = N, ops_left = 0, settle_left = S}) ->
    R#r{budget = B - 1, since_check = N + 1, settle_left = max(0, S - 1)};
spend(R = #r{budget = B, since_check = N}) ->
    R#r{budget = B - 1, since_check = N + 1}.

%% Invariants, every `check_every` actions.
after_action(R = #r{check_every = Every, since_check = N}) when N < Every ->
    loop(R);
after_action(R = #r{mod = Mod, sut = Sut}) ->
    case safe_check(Mod, Sut) of
        ok -> loop(R#r{since_check = 0});
        {violation, _} = V -> {V, R};
        {error, _} = E -> {E, R}
    end.

%% ---------------------------------------------------------------------------
%% Replay
%% ---------------------------------------------------------------------------

replay_loop(R, []) ->
    {ok, R};
replay_loop(R = #r{budget = 0}, _) ->
    {{error, step_budget_exhausted}, R};
replay_loop(R = #r{sched = Sched, lenient = Lenient, skipped = Skipped}, [{step, Id} | Rest]) ->
    case lists:member(Id, dst_sched:runnable(Sched)) of
        true ->
            replay_after(do_step(R, Id), Rest);
        false when Lenient ->
            %% Shrinking asks a different question: not "does this trace replay?"
            %% but "does what is left of it still fail?". Removing an operation
            %% necessarily strands the steps that belonged to it, so a strict
            %% replay would answer `diverged` for almost every candidate and the
            %% shrinker would learn nothing. Skipping lets the rest of the
            %% schedule apply.
            replay_loop(R#r{skipped = Skipped + 1}, Rest);
        false ->
            %% The recorded schedule and this run have parted company. Reporting
            %% it beats continuing, which would produce a "replay" that is not one.
            {{error, {diverged, Id, dst_sched:runnable(Sched)}}, R}
    end;
replay_loop(R, [{op, Op} | Rest]) ->
    replay_after(apply_op(R, Op), Rest);
%% Time passed here. The recorded `Ms` is what the clock read *after* the advance,
%% and a strict replay holds the replay to it: landing somewhere else means the timer
%% wheel is not the one that was recorded, which is a divergence however faithfully
%% the steps line up. Lenient replay takes whatever advance is available, for the same
%% reason it skips an unrunnable step — a shrink candidate has had entries removed,
%% so its wheel is legitimately not the original's.
replay_loop(R = #r{lenient = Lenient, skipped = Skipped, trace = Trace}, [{clock, Ms} | Rest]) ->
    case dst_time:advance_to_next() of
        true ->
            Now = dst_time:now_ms(),
            case Lenient orelse Now =:= Ms of
                true -> replay_after(spend(R#r{trace = [{clock, Now} | Trace]}), Rest);
                false -> {{error, {clock_diverged, Ms, Now}}, R}
            end;
        false when Lenient ->
            replay_loop(R#r{skipped = Skipped + 1}, Rest);
        false ->
            {{error, {clock_diverged, Ms, no_timer}}, R}
    end.

replay_after(R = #r{check_every = Every, since_check = N}, Rest) when N < Every ->
    replay_loop(R, Rest);
replay_after(R = #r{mod = Mod, sut = Sut}, Rest) ->
    case safe_check(Mod, Sut) of
        ok -> replay_loop(R#r{since_check = 0}, Rest);
        {violation, _} = V -> {V, R};
        {error, _} = E -> {E, R}
    end.

%% ---------------------------------------------------------------------------
%% Invariants
%% ---------------------------------------------------------------------------

%% Bounded, and in a process of its own, so a `check/1` that calls into a
%% suspended process is reported rather than hanging the run. It cannot catch the
%% worse case — a client API that swallows its own timeout and answers plausibly,
%% making the invariant vacuous — which is why `dst_sut` says what it says.
safe_check(Mod, Sut) ->
    {Pid, MRef} = spawn_monitor(fun() -> exit({checked, catch Mod:check(Sut)}) end),
    receive
        {'DOWN', MRef, process, Pid, {checked, ok}} ->
            ok;
        {'DOWN', MRef, process, Pid, {checked, {violation, _} = V}} ->
            V;
        {'DOWN', MRef, process, Pid, {checked, Other}} ->
            {error, {check_returned, Other}};
        {'DOWN', MRef, process, Pid, Reason} ->
            {error, {check_crashed, Reason}}
    after ?CHECK_TIMEOUT ->
        erlang:demonitor(MRef, [flush]),
        exit(Pid, kill),
        {error, check_blocked}
    end.

%% ---------------------------------------------------------------------------
%% Teardown
%% ---------------------------------------------------------------------------

finish(Outcome, R) ->
    #r{mod = Mod, sut = Sut, sched = Sched, trace = Trace} = R,
    Stats = dst_sched:stats(Sched),
    ClockMs = dst_time:now_ms(),
    %% Release before terminate: the system has to be running again before it can
    %% be shut down, or terminate/1 waits on processes that cannot answer.
    ok = dst_sched:release(Sched),
    catch Mod:terminate(Sut),
    #{
        outcome => Outcome,
        seed => R#r.seed,
        trace => lists:reverse(Trace),
        steps => R#r.steps,
        ops => R#r.ops,
        clock_ms => ClockMs,
        skipped => R#r.skipped,
        sched => Stats
    }.
