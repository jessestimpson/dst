-module(eta_run).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
The run driver — Phase 3 of the DST framework
(design: `docs/design.md`).

Turns a seed into a run: start the clock, start the system, then alternate between
injecting client operations, stepping one process at a time, and letting virtual
time pass — checking the invariants throughout, and recording enough to replay the
whole thing exactly.

The system under test supplies six callbacks; see `eta_harness`.

```erlang
#{outcome := ok} = eta_run:run(my_sut, #{seed => 7, max_ops => 40}).
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

The clock starts before the system, always. A timer armed while `eta_time` is
inert goes to the real clock and stays there until it fires and re-arms, so
long-period timers — the interesting ones — would silently never be virtual. This
is why `init/2` is called by the driver rather than by the caller beforehand.
""".
-endif.

-export([run/2, replay/3, spawn_op/1]).

%% Reading a result. See `summary/1` and `audit/1`.
-export([summary/1, audit/1]).

%% Trace fixtures — a failure pinned to disk. See `save_fixture/4`.
-export([save_fixture/4, load_fixture/1, replay_fixture/1, check_fixture/1]).

-export_type([entry/0, outcome/0, result/0, fixture/0]).

-type entry() :: {op, eta_harness:op()} | {step, eta_sched:id()} | {clock, integer()}.
-type outcome() ::
    ok
    | {violation, eta_harness:violation()}
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
    %% Modules that were loaded *while the run was in progress*. Should be
    %% empty. See `run/2`.
    modules_loaded := [module()],
    %% Timers the clock refused to advance to because nothing schedulable owned
    %% them. Should be 0. See `eta_time:advance_to_next/1`.
    stray_timers := non_neg_integer(),
    sched := #{atom() => non_neg_integer()}
}.

-type fixture() :: #{
    version := pos_integer(),
    harness := module(),
    trace := [entry()],
    opts := map(),
    outcome := outcome(),
    saved := calendar:datetime()
}.

%% Bumped if the on-disk shape ever changes, so a stale fixture fails with
%% something a person can read rather than a badmatch.
-define(FIXTURE_VERSION, 1).

%% An invariant reads a frozen system, so it should return immediately. Anything
%% slower is a `check/1` that called into a suspended process — see `eta_harness`.
-define(CHECK_TIMEOUT, 5000).

%% Where `spawn_op/1` parks pids until the driver has registered them. The driver's
%% own process dictionary, which is safe because `execute/2` runs there and nowhere
%% else — and private, because nothing outside this module reads the key.
-define(PENDING_OPS, '$eta_run_pending_ops').

-record(r, {
    mod :: module(),
    sut :: eta_harness:state(),
    sched :: eta_sched:sched(),
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
    %% Snapshot of the code path taken once the scheduler owns the system, so
    %% that anything loaded afterwards can be reported. See `finish/2`.
    loaded = [] :: [module()],
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
- `log` (`true`) — collect a `eta_log` record of the run. `false` suppresses the
  events but **not** the sequence numbers `log/1` hands out, so a harness that
  stamps its operations from them keeps working. See `eta_log`.
- `preload` (`[]`) — applications whose modules to load before the run starts.
  **Name your own application here.** `kernel`, `stdlib` and `eta` are always
  included; anything you add is loaded on top. `false` disables it entirely.

  This is the fix for `modules_loaded`, and the only reliable one, because
  warming is per *code path* rather than per VM: running one seed does not warm
  the branch a later seed takes, so "run it twice" fixes one seed and not the
  next. Loading your application up front warms all of it.

## Fields that mean the run was not deterministic

`stray_timers` counts timers held by a process the scheduler does not own, or
that has died. A deadline like that cannot make anything runnable, so advancing
to it would move the clock on behalf of something that can never take a step. The
driver steps over them, and reports how many it found — a non-zero count means
your system holds a timer outside the schedule, usually from a process spawned in
`init/2` or one killed while blocked in a `receive ... after`.

`sched.adopted_late` counts processes that ran before the scheduler owned them,
and `sched.timeouts` counts steps that ended without the process reaching a
receive.

`modules_loaded` lists modules that were loaded **while the run was in
progress**, and it should be empty. Loading a module on demand is a synchronous
call into `code_server`, which the scheduler does not own, so a scheduled
process that reaches a module it has not touched yet blocks on something
outside the schedule. The scheduler reads that as the step ending, `code_server`
makes the process runnable again at a moment decided by wall clock, and every
choice after that point shifts.

The symptom is that the *first* run in a fresh VM produces a different trace
from every later run of the same seed. Nothing is logged, no scheduler warning
fires, and `adopted_late` stays 0 throughout, which is why this is reported
rather than left for you to deduce.

The fix is the `preload` option above. The names reported here tell you which
application to name in it, and the rule that usually identifies the culprit is
that **the module which bites is the one only reachable from a scheduled
process** — anything `init/2` itself touches has already been loaded by the time
it matters.

### The worse failure, and why `preload` is on by default

A process waiting on `code_server` is, to `eta_sched`, a process blocked in a
receive. It is not runnable. So if enough of the system reaches for cold code at
once, **nothing is runnable, no timer is pending, and the run ends at what looks
exactly like quiescence** — having done almost nothing, and reporting `ok`.

Measured on the 2PC harness with preloading disabled and a cold module every
client touches: 5 operations, **5 steps**, nothing exited, `outcome => ok`. A
healthy run of the same workload takes around a hundred steps.

`modules_loaded` does not catch that one, because the loads complete after the
run has finished. Nothing catches it. It is the reason `preload` defaults to
loading `kernel`, `stdlib` and `eta` rather than waiting to be asked, and the
reason to name your own application there even when runs look fine.

A run that ends far too early with `ok` and nothing exited is the symptom.

`audit/1` checks these for you, and keeps checking them as more are added:

```erlang
ok = eta_run:audit(eta_run:run(Mod, Opts)).
```
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
the schedule that actually ran, which is how `eta_shrink` recovers a trace that
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
traced, so a process it creates is not adopted by `eta_sched` until the driver
registers it — and in that window the new process runs on the real scheduler. One
op process racing alone is usually harmless; two of them, from operations injected
close together, race each other to deliver their first message, and the order they
arrive in is real-world timing rather than anything the seed controls. It shows up
as a seed producing two different traces, which is the framework failing at the one
thing it exists to do.

The process spawned here blocks before running `Fun`, so it cannot act before it is
owned. `eta_run` releases it once registration is done, and from that point the
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
%% Reading a result
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc """
Any trace-carrying result, with the trace replaced by its length. What you want
when printing.

Inspecting one whole is the first thing anybody does after a first successful
run, and it is useless: a 20-operation run produces a few hundred trace entries,
so a shell renders the schedule and elides the fields you were looking for
underneath it.

```erlang
#{outcome := ok, steps := 97, trace_length := 213, modules_loaded := []} =
    eta_run:summary(eta_run:run(my_harness, #{seed => 1})).
```

Takes a `t:eta_shrink:result/0` too, and there `trace_length` says something the
other fields do not: which trace you were handed. It matches `shrunk` when
`verified` is true and `original` when it is false, because an unverified shrink
gives the original back.

The trace itself is for later, and even then it is not what you read. `eta_log`
is.
""".
-endif.
-spec summary(map()) -> map().
summary(Result = #{trace := Trace}) ->
    (maps:remove(trace, Result))#{trace_length => length(Trace)}.

-if(?DOCATTRS).
-doc """
Whether this run's seed means anything.

```erlang
ok = eta_run:audit(eta_run:run(my_harness, Opts)).
```

Several fields in a result say "part of this interleaving was decided by wall
clock rather than by the schedule", and a run with any of them set is one whose
seed will not reproduce. They are all silent — nothing fails, nothing is logged
except the scheduler's own timeout warning, and the invariants stay green.

Rather than asking every caller to know which fields those are, and to keep
knowing as more are added, this checks them:

- `modules_loaded` — code loaded mid-run, so a scheduled process made a
  synchronous call into `code_server`. Fix with `preload`.
- `sched.adopted_late` — processes that ran before the scheduler owned them.
  Fix by spawning through `eta_run:spawn_op/1` and `eta_sched:spawn/1`.
- `sched.timeouts` — steps that ended without the process reaching a receive,
  so it was suspended wherever it happened to be.

`{suspect, Reasons}` rather than `{error, _}` deliberately: the run happened and
its violation, if any, is real. What you cannot do is trust the seed to give it
back.

Assert on this next to your invariants. It is the single most valuable line in a
simulation suite after the determinism gate itself.
""".
-endif.
-spec audit(result()) -> ok | {suspect, [{atom(), term()}]}.
audit(Result = #{modules_loaded := Modules, sched := Sched}) ->
    Checks = [
        {modules_loaded, Modules, []},
        {adopted_late, maps:get(adopted_late, Sched, 0), 0},
        {timeouts, maps:get(timeouts, Sched, 0), 0},
        {stray_timers, maps:get(stray_timers, Result, 0), 0}
    ],
    case [{What, Found} || {What, Found, Clean} <- Checks, Found =/= Clean] of
        [] -> ok;
        Suspect -> {suspect, Suspect}
    end.

%% ---------------------------------------------------------------------------
%% Fixtures
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc """
Writes a trace to disk as a self-contained reproduction, after checking that it
still reproduces.

## Why not just pin the seed

Because a seed names a schedule **only in the context of a particular
`generate/2`**. Change the operation mix and every seed you pinned quietly
starts testing something else. If the test asserts on a violation it fails and
you find out; if it asserts `ok` it goes vacuous without a word, which is the
same silent failure as a quiescence-gated invariant that never evaluates.

A trace has no such coupling. `replay/3` never calls `generate/2` — it walks the
entries it is given — so a saved trace survives a rewritten workload entirely.
It also survives a different seed, since nothing is drawn from one.

Pin a seed to test that seeds reproduce. Pin a trace to regress a bug.

## What is saved

The trace, the harness module, and the options. The options matter as much as
the trace: a reproduction that needs `config => #{mode => broken}` and is
replayed without it does not reproduce, and `eta_shrink` reports "nothing to
shrink" for a failure that just happened. Carrying them means a test cannot get
that wrong.

## What is checked

The trace is replayed **strictly** before anything is written, and the outcome
it produced is stored with it. So a fixture on disk is one that has been
demonstrated to reproduce, rather than one that was believed to.

Take the trace from a `eta_shrink:shrink/3` result whose `verified` is `true`,
which is `eta_shrink`'s own version of this check.

## What you cannot do with it

Write the mirror test. Replaying a *failing* trace against a *fixed*
implementation does not report `ok` — the fix changes which messages exist, so
recorded step ids stop being runnable and you get `{error, {diverged, _, _}}`.
"The fix works" is a claim about the whole system, and a seed sweep is the right
shape for it.
""".
-endif.
-spec save_fixture(file:name_all(), module(), [entry()], map()) ->
    {ok, outcome()} | {error, term()}.
save_fixture(Path, Mod, Trace, Opts) ->
    Result = replay(Mod, Trace, Opts#{lenient => false}),
    Outcome = maps:get(outcome, Result),
    case Outcome of
        {error, Reason} ->
            {error, {did_not_replay, Reason}};
        _ ->
            Fixture = #{
                version => ?FIXTURE_VERSION,
                harness => Mod,
                trace => Trace,
                opts => maps:remove(lenient, Opts),
                outcome => Outcome,
                saved => calendar:universal_time()
            },
            case file:write_file(Path, term_to_binary(Fixture)) of
                ok -> {ok, Outcome};
                {error, Reason} -> {error, Reason}
            end
    end.

-if(?DOCATTRS).
-doc "Reads a fixture without running it. Raises on an unreadable or stale file.".
-endif.
-spec load_fixture(file:name_all()) -> fixture().
load_fixture(Path) ->
    {ok, Bin} = file:read_file(Path),
    case binary_to_term(Bin) of
        Fixture = #{version := ?FIXTURE_VERSION} -> Fixture;
        #{version := V} -> error({eta_run, {unsupported_fixture_version, V, Path}});
        _ -> error({eta_run, {not_a_fixture, Path}})
    end.

-if(?DOCATTRS).
-doc """
Replays a fixture and returns the full result, using the harness and options it
was saved with.

The whole point is that a test names a file and nothing else:

```erlang
#{outcome := {violation, #{property := atomicity}}} =
    eta_run:replay_fixture("test/fixtures/atomicity.eta").
```
""".
-endif.
-spec replay_fixture(file:name_all()) -> result().
replay_fixture(Path) ->
    #{harness := Mod, trace := Trace, opts := Opts} = load_fixture(Path),
    replay(Mod, Trace, Opts#{lenient => false}).

-if(?DOCATTRS).
-doc """
Replays a fixture and compares the outcome with the one recorded when it was
saved.

`ok` means the reproduction still holds. `{changed, _}` means the system no
longer does what it did, which is what you want to see when a fix lands — though
note that a genuine fix usually shows up as a *divergence* rather than a clean
run, for the reason given in `save_fixture/4`.
""".
-endif.
-spec check_fixture(file:name_all()) ->
    ok | {changed, #{expected := outcome(), actual := outcome()}}.
check_fixture(Path) ->
    #{outcome := Expected} = load_fixture(Path),
    case maps:get(outcome, replay_fixture(Path)) of
        Expected -> ok;
        Actual -> {changed, #{expected => Expected, actual => Actual}}
    end.

%% ---------------------------------------------------------------------------
%% Driving
%% ---------------------------------------------------------------------------

drive(Mod, Opts, Source) ->
    Seed = maps:get(seed, Opts, 0),
    ok = preload(maps:get(preload, Opts, [])),
    %% Before `init/2`, so that anything the system logs while starting up is in
    %% the record too. The driver names itself once, here, and every entry it
    %% writes from now on is tagged as the framework's rather than the system's.
    ok = start_log(maps:get(log, Opts, true)),
    ok = eta_log:label('$eta'),
    %% Before the system, always — see the module doc.
    ok = eta_time:start(#{seed => Seed}),
    try
        {ok, Sut} = Mod:init(Seed, maps:get(config, Opts, #{})),
        Sched0 = eta_sched:new(#{seed => Seed}),
        Sched = eta_sched:register(Sched0, Mod:processes(Sut)),
        %% Snapshot here rather than at the top: whatever `init/2` loaded was
        %% loaded before a scheduler existed, which is exactly where a system is
        %% supposed to warm its code path. Only what loads from *this* point is
        %% a real-clock dependency, and hoisting the register call out of the
        %% record makes the ordering explicit rather than relying on the
        %% unspecified evaluation order of record fields.
        Loaded = erlang:loaded(),
        R = #r{
            mod = Mod,
            sut = Sut,
            sched = Sched,
            loaded = Loaded,
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
        eta_time:stop()
    end.

%% Load every module of the named applications, before there is a scheduler to
%% be outside of.
%%
%% Loading a module on demand is a synchronous call into `code_server`, which the
%% scheduler does not own, so a scheduled process reaching a cold module blocks
%% outside the schedule and is made runnable again by wall clock. `modules_loaded`
%% reports that after the fact; this is how you stop it happening.
%%
%% It is the *only* reliable fix, because warming is per code path rather than
%% per VM. Running one seed does not warm the branch a later seed takes — a
%% workload that first reaches a timeout handler loads code no earlier seed
%% touched — so "run it twice" fixes one seed and not the next.
%%
%% Deliberately not automatic beyond the defaults. `eta_run` is handed a module,
%% not an application, so it cannot infer what to load, and loading every module
%% of every loaded application would drag in Mix, ExUnit and the whole of OTP,
%% firing any `-on_load` functions they carry.
preload(false) ->
    ok;
preload(Apps) when is_list(Apps) ->
    %% `kernel` and `stdlib` because a system under test reaches them and nobody
    %% would think to name them; `eta` because a transformed module's rewritten
    %% calls land in `eta_time` from inside a scheduled process.
    lists:foreach(fun preload_app/1, [kernel, stdlib, eta | Apps]).

preload_app(App) ->
    case application:load(App) of
        ok -> ok;
        {error, {already_loaded, App}} -> ok;
        {error, Reason} -> error({eta_run, {preload_failed, App, Reason}})
    end,
    case application:get_key(App, modules) of
        {ok, Modules} ->
            _ = code:ensure_modules_loaded(Modules),
            ok;
        undefined ->
            %% Loaded but with no module list, which means the `.app` is not
            %% what it claims to be. Silently preloading nothing is exactly the
            %% failure this option exists to prevent.
            error({eta_run, {preload_failed, App, no_modules}})
    end.

%% `log => false` for a run that does not want the record. `stamps` rather than
%% `stop`, so `eta_log:log/1` keeps returning real sequence numbers: a harness is
%% told to stamp its operations from them, and stamps that all read 0 would make
%% any "A finished before B started" invariant vacuous without saying so.
%%
%% Either way the previous run's log is discarded, so `analyze/0` cannot quietly
%% show you an older story.
start_log(false) -> eta_log:trace(stamps);
start_log(true) -> eta_log:trace(start).

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
    case choose(R, eta_sched:runnable(Sched), OpsLeft > 0) of
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
%% sort. On a timer-driven system it made `eta_shrink` a no-op: the shrinker replays
%% original trace to classify it, got a clean run, and reported "nothing to shrink"
%% for a failure that had just happened.
idle(R = #r{ops_left = OpsLeft, trace = Trace}) ->
    case eta_time:advance_to_next(schedulable(R#r.sched)) of
        true ->
            Now = eta_time:now_ms(),
            _ = eta_log:log({clock, Now}),
            loop(spend(R#r{trace = [{clock, Now} | Trace]}));
        false when OpsLeft > 0 ->
            %% `quiet_p` chose to let time pass and there was no time to pass.
            after_action(do_op(R));
        false ->
            {ok, R}
    end.

%% The log entry goes in *before* the step runs, so that everything the chosen
%% process records lands underneath it. That ordering is what makes the log read
%% as "the scheduler picked r2, and here is what r2 then did".
do_step(R = #r{sched = Sched, trace = Trace, steps = Steps}, Id) ->
    _ = eta_log:log({step, Id}),
    {_Outcome, _} = eta_sched:step(Sched, Id),
    spend(R#r{trace = [{step, Id} | Trace], steps = Steps + 1}).

do_op(R) ->
    #r{mod = Mod, sut = Sut, rand = Rand} = R,
    {Op, Rand1} = Mod:generate(Sut, Rand),
    apply_op(R#r{rand = Rand1}, Op).

apply_op(R, Op) ->
    #r{mod = Mod, sut = Sut, sched = Sched, trace = Trace, ops = Ops} = R,
    _ = eta_log:log({op, Op}),
    Sut1 = Mod:execute(Op, Sut),
    %% An operation is issued by spawning; pick up whatever it created. Already
    %% known pids are ignored, so re-registering the whole set is correct.
    Sched1 = eta_sched:register(Sched, Mod:processes(Sut1)),
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
    case lists:member(Id, eta_sched:runnable(Sched)) of
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
            {{error, {diverged, Id, eta_sched:runnable(Sched)}}, R}
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
    %% The same schedulability rule `idle/1` used to record the entry. Replaying
    %% under a laxer one would advance to a deadline the generating run stepped
    %% over, so a trace from a system carrying a stray timer would not replay.
    case eta_time:advance_to_next(schedulable(R#r.sched)) of
        true ->
            Now = eta_time:now_ms(),
            case Lenient orelse Now =:= Ms of
                true ->
                    _ = eta_log:log({clock, Now}),
                    replay_after(spend(R#r{trace = [{clock, Now} | Trace]}), Rest);
                false ->
                    {{error, {clock_diverged, Ms, Now}}, R}
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

%% `erlang:loaded/0` rather than `code:all_loaded/0`: it is a BIF returning bare
%% module names, where the latter builds `{Module, Filename}` pairs and may go
%% through the code server — which is precisely the process this is here to
%% catch traffic to.
-spec loaded_since([module()]) -> [module()].
loaded_since(Before) ->
    Known = maps:from_keys(Before, []),
    lists:sort([M || M <- erlang:loaded(), not is_map_key(M, Known)]).

%% Two sources, joined against the scheduler's id-to-pid map here so that neither
%% one has to think in ids. A process that labelled itself wins: it is the source
%% closest to the truth, and letting the harness override it is how one process
%% ends up under 2 names in the same report.
%%
%% Both are optional. A run where nobody names anything gets `p0`, `p1` and so on.
collect_labels(Mod, Sut, Sched) ->
    ByPid = maps:merge(harness_labels(Mod, Sut), eta_log:self_labels()),
    maps:fold(
        fun(Id, Pid, Acc) ->
            case maps:find(Pid, ByPid) of
                {ok, Name} -> Acc#{Id => Name};
                error -> Acc
            end
        end,
        #{},
        eta_sched:procs(Sched)
    ).

%% Wrapped because this runs during teardown, after a run that may already have
%% failed: a harness whose labelling crashes should not turn a reported violation
%% into a teardown error.
harness_labels(Mod, Sut) ->
    case erlang:function_exported(Mod, labels, 1) of
        false ->
            #{};
        true ->
            case catch Mod:labels(Sut) of
                Map when is_map(Map) -> Map;
                _ -> #{}
            end
    end.

%% Bounded, and in a process of its own, so a `check/1` that calls into a
%% suspended process is reported rather than hanging the run. It cannot catch the
%% worse case — a client API that swallows its own timeout and answers plausibly,
%% making the invariant vacuous — which is why `eta_harness` says what it says.
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
    Stats = eta_sched:stats(Sched),
    ClockMs = eta_time:now_ms(),
    Strays = eta_time:strays(),
    %% Before `release/1` and `terminate/1`, so that teardown's own loading is
    %% not blamed on the run.
    Modules = loaded_since(R#r.loaded),
    %% Names, now and not before. Ids are handed out as processes register, so
    %% the mapping does not exist until the run is over, and only the harness can
    %% supply it — with its own state in hand. Doing it here keeps every callback
    %% off the hot path.
    ok = eta_log:register_labels(collect_labels(Mod, Sut, Sched)),
    %% Everything logged after this ran on the *real* scheduler, because
    %% `release/1` resumes every suspended process. It is real, it is
    %% nondeterministic, and it is not part of the run. `eta_log` hides it by
    %% default and can only do so because of this marker.
    _ = eta_log:log(released),
    %% Release before terminate: the system has to be running again before it can
    %% be shut down, or terminate/1 waits on processes that cannot answer.
    ok = eta_sched:release(Sched),
    catch Mod:terminate(Sut),
    #{
        outcome => Outcome,
        seed => R#r.seed,
        trace => lists:reverse(Trace),
        steps => R#r.steps,
        ops => R#r.ops,
        clock_ms => ClockMs,
        skipped => R#r.skipped,
        modules_loaded => Modules,
        stray_timers => Strays,
        sched => Stats
    }.

%% Whether a deadline addressed to `Dest` can produce a scheduling event.
%%
%% Two conditions, and both are load-bearing. The process has to be one the
%% scheduler owns, because a deadline for anything else moves the clock on behalf
%% of something that can never take a step. And it has to be **alive**, because
%% `eta_sched:procs/1` deliberately remembers processes that have exited so that a
%% trace can name one — a dead owner's timer is exactly the stray this guards
%% against, not a reason to advance.
schedulable(Sched) ->
    Owned = maps:from_keys(maps:values(eta_sched:procs(Sched)), []),
    fun(Dest) -> is_map_key(resolve_dest(Dest), Owned) andalso alive(Dest) end.

resolve_dest(Pid) when is_pid(Pid) -> Pid;
resolve_dest(Name) when is_atom(Name) -> whereis(Name).

alive(Pid) when is_pid(Pid) -> is_process_alive(Pid);
alive(Name) when is_atom(Name) -> whereis(Name) =/= undefined.
