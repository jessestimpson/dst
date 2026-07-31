# A reusable deterministic simulation testing framework for the BEAM

> **How to read this.** This is the design record of the work that produced this
> library, written inside the project that motivated it: `dgen`, a durable
> distributed `gen_server` with a replicated process registry. It is kept as-is
> because what each phase *taught* — including the parts of the plan that turned
> out to be wrong — is the valuable part, and rewriting it to be
> project-neutral would cost that.
>
> So: module and file paths such as `dgen_registry_member`, `dgen_mem` and
> `test/support/sim/` refer to that project, not to this one. For the manual, see
> the walkthrough, [`docs/01`](01-what-dst-is.md) through
> [`docs/05`](05-gotchas.md).

**Status:** Phases 0–4 are done, and Phase 5's central problem — OTP's own
process creation — turned out to have a cheaper answer than the plan assumed, so
that is done too. `dst_sched`, `dst_time`, `dst_transform`, `dst_after_transform`,
`dst_observe`, `dgen_mem`, `dst_harness`, `dst_run` and `dst_shrink`, with two systems
under test on the full driver: `dgen_registry` and two-phase commit. The suite runs
with no FoundationDB (`DGEN_BACKEND=dgen_mem mix test`).

Both systems now reproduce their own schedule exactly and replay a recorded trace
strictly; `dst_2pc` also shrinks. What remains is the acceptance criteria below and
the rest of Phase 5's "no real-time dependence anywhere" goal.

The framework has found one defect in `dgen_registry` that nobody planted, and two
in itself — a missing trace entry and a real-time dependence that had been recorded
as an unexplained seed. All three are written up under Phase 5.

This document starts life as a plan and becomes the design document for the
framework as the phases land. Each completed phase should leave behind not just a
tick but what was *learned* building it — the assumptions that turned out wrong are
the most valuable thing here, because they are what a reader would otherwise have
to rediscover.

## What this is for

`test/support/sim/` today runs an N-member `dgen_registry` cluster in one VM and
injects seeded network faults. It found three replication defects, so it earns its
keep. But it is **not** deterministic simulation testing: the fault *schedule* is
seeded, while process scheduling, timers, and commit timing are not controlled. A
failing seed usually reproduces. Usually is not a guarantee.

This plan closes that gap, and does it in a way that is not welded to
`dgen_registry` — the scheduler, the virtual clock, the run driver, and the shrinker
are all system-agnostic. `dgen_registry` becomes the first consumer and the proving
ground, not the product.

## What "true DST" buys that we do not already have

Worth being concrete, because the current harness is not bad and this is a
multi-month investment.

1. **Exact replay.** A seed reproduces a failure bit-for-bit, every time, including
   on another machine. Today a seed reproduces the fault schedule and hopes the
   scheduler cooperates.
2. **Speed — the big one.** With a virtual clock, waiting out a 1s timeout costs
   nothing; the run jumps to the next event. The current 5-seed soak takes 47.8s
   and is dominated by real waiting. The same wall clock should buy **thousands**
   of seeds instead of five. Bug-finding scales with interleavings visited, so this
   is the single largest expected return.
3. **Faults we cannot inject today.** Commit-level faults (spurious conflicts,
   fenced commits, a commit worker dying between commit and reply) and timer-level
   faults (a timeout firing in an unlucky window) become first-class and
   reproducible.
4. **Minimal repros.** A failure shrinks to the shortest schedule that still fails,
   which is the difference between "here is a 4000-event trace" and "here are the
   six steps that break it".

What it does *not* buy: it is not a proof. TLC still explores every interleaving of
an abstraction exhaustively; DST samples interleavings of the real code. They fail
in disjoint ways — see `formal/README.md`, where the two disagreeing about the
protocol is exactly what let the partial-batch bug survive.

## The central primitive, validated then built

Everything rests on being able to serialize arbitrary BEAM processes — run exactly
one at a time, to quiescence — so that the interleaving is fully determined by the
controller's choice sequence.

That works, on **uninstrumented** code, using only `erlang:suspend_process/1`,
`erlang:resume_process/1`, and process introspection. The original feasibility
probe, three chatty processes fanning messages at each other:

```
Same seed, 5 runs:
  steps=14 events=31 head=[a: 5, c: 4, b: 4, b: 3, c: 3, c: 2, b: 2, b: 1]   (×5, identical)
distinct outcomes across 5 runs of seed 42: 1

Different seeds:
  seed 1: steps=14  head=[a: 5, c: 4, b: 4, b: 3, c: 3, c: 2]
  seed 2: steps=12  head=[a: 5, b: 4, c: 4, c: 3, a: 3, a: 3]
  seed 3: steps=15  head=[a: 5, b: 4, a: 3, b: 2, a: 1, c: 4]
  seed 4: steps=17  head=[a: 5, c: 4, a: 3, c: 2, a: 1, b: 4]
distinct schedules across 4 seeds: 4
```

(`dst_sched` now does this properly — the probe polled status, which is both
wasteful and, as Phase 0 discovered, racy. The result stands.)

The key consequence, and the reason this approach is tractable: **you do not need to
intercept messages to control delivery order.** Erlang already guarantees FIFO
per ordered pair, so if only one process runs at a time, the global message order is
determined by the order in which senders were stepped. Controlling *who runs* is
sufficient. That removes an entire layer of instrumentation the design would
otherwise need.

## The four sources of nondeterminism

| Source | Remedy | Phase |
|---|---|---|
| Preemptive process scheduling | Serializing scheduler (validated above) | 0 |
| Timers and clock reads | Virtual clock + discrete-event loop | 1 |
| External I/O (FDB, a NIF) | Pluggable deterministic backend | 2 |
| Unseeded randomness (`rand`, `make_ref`, pids) | Seeded RNG; discipline about ordering by identity | 0 |

The fourth deserves a note. Refs and pids differ between runs and always will;
that is only a problem if they influence *control flow*. `dgen_registry` is clean
here — `elect_leader/5` orders by `member_id/0`, which is `{node(), Name}` and
stable — but a framework should ship a linter for the anti-pattern, because it is
easy to introduce and produces failures that look like scheduler bugs.

## Architecture

Six components. The first four are system-agnostic and are the reusable framework;
the last two are what a system under test provides.

```
dst_sched    serializing scheduler: owns the process set, picks who steps next
             from a seeded RNG, detects quiescence, emits the choice sequence
dst_time     virtual clock + timer wheel; when nothing is runnable, advance to
             the next timer deadline rather than waiting
dst_run      driver: seed -> setup -> generate -> step -> check -> teardown,
             recording everything needed for exact replay
dst_shrink   delta-debugging over the choice sequence and the op list
─────────────────────────────────────────────────────────────────────────────
<sut>_world  the system's external dependencies, made deterministic and
             fault-injectable (for dgen: a pure-Erlang dgen_backend)
<sut>_model  workload generator + invariants
```

### The SUT contract

```erlang
-callback init(Seed :: integer(), Config :: map()) -> {ok, Sut :: term()}.
-callback processes(Sut) -> [pid()].              %% what to schedule
-callback generate(Sut, Rand) -> {Op :: term(), Rand}.
-callback execute(Op, Sut) -> Sut.
-callback check(Sut) -> ok | {violation, term()}. %% run after every step
-callback terminate(Sut) -> ok.
```

Shipped, with two corrections the sketch above did not anticipate — see Phase 3.
`execute/2` cannot call the system synchronously and must spawn through
`dst_run:spawn_op/1`, and `check/1` cannot ask a scheduled process anything at all.

The original note here claimed `dgen_registry`'s existing `DGen.Sim.Invariants` and
workload generator would "drop into `check/1` and `generate/2` almost unchanged,
which is a useful sanity check on the contract's shape". That was wrong, and
usefully so: `check_always/1` reaches `dgen_registry:status/1`, a `gen_server:call`
that cannot be served while the member is suspended and which swallows its own
timeout, so the invariant would pass vacuously rather than fail. The contract's
shape was fine; the assumption that an invariant may *ask* rather than *read* was
not. `dst_2pc` was written against the contract instead, and the registry port now
carries the cost of rewriting those checks to read ETS.

### Interception strategy

Timers and clock reads need rewriting; a `parse_transform` enabled only under a
`dst` build profile is the right mechanism, rewriting within SUT modules:

| From | To | Where |
|---|---|---|
| `erlang:send_after/3`, `start_timer/3`, `cancel_timer/1` | `dst_time:*` | `dst_transform` |
| `erlang:monotonic_time/0,1`, `system_time/0,1` | `dst_time:*` | `dst_transform` |
| `receive ... after T -> B end` | a receive clause on a `dst_time` timer | `dst_after_transform` |

Opt-in at compile time, absent in production builds. No runtime cost, and nothing
to accidentally ship.

| `erlang:spawn*`, `proc_lib:spawn*` | `dst_sched:spawn*` (gated) | `dst_transform` |

That last row was in the original table, removed in a later revision on the grounds
that `dst_sched` adopts children through `set_on_spawn` tracing and so needed no
rewrite, then reinstated and built when porting `dgen_registry` measured what
adoption actually costs. `set_on_spawn` adopts a child *after the fact*; it does not
stop the child acting first. The original table was right.

The spawn row is also the **one place the "only qualified calls" rule does not
apply**, and the exception is principled rather than pragmatic. That rule exists
because `monotonic_time` and friends are not auto-imported, so a bare call must be
to something the module defines and rewriting it would break the module.
`spawn/1,3` and `spawn_link/1,3` *are* auto-imported, so a bare `spawn(Fun)` is
`erlang:spawn(Fun)` — and `dgen_registry_member` writes all five of its spawns
unqualified. The transform rewrites unqualified spawns, guarded on the module not
defining that name itself, which is the condition the original rule was protecting.

The third row is the one this document previously said was impossible — see
Phase 5.

## Phases

Each phase is independently useful; the plan can stop after any of them.

### Phase 0 — Harden the scheduler — **DONE**

Shipped as [`src/dst_sched.erl`](../../src/dst_sched.erl), with exit criteria in
`test/dst_sched_test.exs`. All four items landed: trace-driven quiescence, correct
selective-receive runnability, auto-registration of spawned processes, and a
recorded choice sequence replayable independently of the seed.

*Exit criteria, met:* 1000 runs of one seed produce a byte-identical event trace
*and* choice sequence (~23s); 12 seeds produce distinct interleavings over the same
event multiset; a recorded sequence replays exactly under a deliberately different
RNG seed.

The Concuerror spike was dropped: the framework is being built without new
dependencies. It remains listed under Alternatives as a thing to know about.

#### What was wrong in the plan

Three assumptions in this document's original Phase 0 turned out to be false. They
are recorded because each was discovered only by building it, and each would
silently produce a scheduler that *looks* correct:

1. **"Replace busy-wait quiescence detection with `erlang:trace/3` on
   `'receive'`."** The `'receive'` flag cannot serve as a progress signal at all.
   It fires when a receive *scans* a message, matching or not — a non-matching
   message produces a `'receive'` event and stays in the queue. Progress is instead
   computed from the queue-length delta, corrected for self-sends (nothing else
   runs, so those are the only arrivals). Quiescence uses the `running` flag's
   `in`/`out` events.

2. **Checking status after resuming is a race.** A process blocked in a receive
   reads `waiting` *before it has been scheduled in*, so the step concludes without
   the process ever running. The `in` event must be awaited first, as proof it got
   the CPU, before `waiting` means anything.

3. **The block must be recorded on every step, not just unproductive ones.** A
   `receive` blocks only after failing to match every queued message, so after
   *any* step the remaining mailbox is known not to match. Recording the block only
   when a step consumed nothing leaves a process that consumed one message and
   blocked on the rest looking runnable forever.

There was also a design flaw not anticipated here at all: the scheduler runs in the
driver's process, so a catch-all `receive` clause while awaiting trace events
**consumes and discards the system under test's messages to the driver**. It
presented as a `gen_server` reply vanishing while its caller exited `:normal`.
Every receive in the scheduler now matches trace-shaped messages only, and there is
a regression test for it. Phase 3 should reconsider whether the scheduler belongs
in its own process, which would remove the hazard structurally.

### Phase 1 — Virtual time — **DONE**

Shipped as [`src/dst_time.erl`](../../src/dst_time.erl) (clock + timer wheel) and
[`src/dst_transform.erl`](../../src/dst_transform.erl) (the parse transform), with
the discrete-event loop as `dst_sched:run/3`. Exit criteria in
`test/dst_time_test.exs`, driving a transformed system under test
(`test/support/dst_timer_sut.erl`).

Timer faults are in: `drop_p` (a timer that never fires but stays cancellable and
readable — a lost message, not an unset timer) and `skew_ms` (deadlines perturbed
at creation, never into the past).

*Measured:* 100 minutes of simulated time — 200 heartbeat ticks at 30s — in **8ms**
of real time. That is the phase's whole argument, and it is the multiplier that
makes thousands of seeds affordable where five were not.

`dgen_registry` is wired: `dgen_registry_member`, `_connector`, `_elector`,
`dgen_server` and `dgen_queue` carry an `-ifdef(DST)` guard, and `mix.exs` passes
`{d, 'DST'}` in the test environment. Integration coverage is in
`test/dgen_registry_virtual_time_test.exs`, including the replication heartbeat
driving a gapped follower's recovery — a path that costs
`?REPLICA_HEARTBEAT_INTERVAL` of wall clock to test otherwise.

#### Design notes worth keeping

**No process of its own.** `dst_time` is ETS plus module functions, deliberately.
A `gen_server` would make every `send_after/3` a blocking call, and a blocking call
*is* quiescence to `dst_sched` — so each timer set would end a step, consume a
scheduling choice, and pollute the recorded schedule with the framework's own
traffic. Safe because only one process under test runs at a time.

**Inert unless started.** Every `dst_time` function delegates to its `erlang`
counterpart when no clock is running, so a module built with the transform behaves
normally outside a simulation. This is what makes the transform safe to leave
enabled in a build that also runs ordinary tests, and it removes the need for a
separate artefact.

**Only qualified calls are rewritten.** `erlang:monotonic_time()` is rewritten, a
bare `monotonic_time()` is not — none of these are auto-imported, so an unqualified
call is a call to a function the module defines itself, and rewriting it would
break the module. Code that wants to be simulated must qualify its time calls,
which is the prevailing style and what `dgen_registry` already does.

**`timer:sleep/1` is deliberately not rewritten.** Code under simulation should not
sleep, and virtualising it silently would hide that rather than surface it.

*Later correction:* the reason originally given here — "a sleeping process is
neither runnable nor blocked on a receive" — is not accurate. `timer:sleep(N)` is
`receive after N -> ok end`, so the process *is* blocked in a receive; what defeats
`dst_sched` is narrower, that its runnability test is `queue_len > blocked_at` and a
sleeper's wakeup is a real-clock `after` with an empty mailbox. That distinction
matters, because it means the same mechanism `dst_after_transform` uses (Phase 5)
would virtualise a sleep too. Whether it *should* is a separate question, and the
answer differs on either side of the transform boundary: a sleep inside the SUT
wants a virtual timer, while a sleep in the driver — `dgen_registry:await_ready/2`
— wants to pump the scheduler instead, since the driver is the process that has to
advance the clock. Not built; recorded so the reasoning is not rediscovered.

#### Wiring a real system in: what it takes

**Not every module should be transformed.** The rule that emerged: transform the
modules whose *processes run inside* the simulation; leave the client-facing API the
driver calls on the real clock. `dgen_registry:await_ready/2` polls a deadline with
`timer:sleep/1`, so against a frozen virtual clock it would spin forever — it is
excluded, and that is not an oversight to fix later but the correct boundary. The
same reasoning excludes `dgen`'s client-side call path.

**Two sleep-based deadline loops exist in the code under test**, and they are the
real obstacle to full simulation rather than anything about timers:
`dgen_registry:await_ready_loop/2` (excluded, above) and
`dgen_registry_member:gather_caught_up/6`, which runs in an off-loop helper the
member spawns. A sleeping process is neither runnable nor blocked on a receive, so
`dst_sched` cannot step it at all. `gather_caught_up/6` exits immediately on its
common path, so it does not bite today; a full run under the scheduler will need it
converted to a timer. Recorded here rather than fixed, since it is Phase 2+ work.

**Enabling the transform for the whole test environment, not a separate profile,
is deliberate.** Because `dst_time` delegates to `erlang` unless a clock is running,
the ordinary suite runs unchanged — and in doing so continuously proves that
inertness across all 297 tests, which a dedicated profile would only prove for the
tests run under it. The property that makes the transform safe is the one most
worth having under constant test.

**The clock must be started before the system.** A timer armed while `dst_time` is
inert goes to the real clock and stays there until it fires and re-arms. Starting
the clock afterwards leaves long-period timers — precisely the ones a simulation
cares about — on real time. It presented as a run sitting at *one* fired timer
across four minutes of simulated time.

**Wiring the transform in means the system's own deadlines become the driver's.** A
direct registration waits for `replicate_timeout` before degrading open (§5.5); once
that is a virtual timer, a frozen clock means the registration simply does not
resolve. This is correct, and it is a real change in how a test must be written —
the write path can no longer be driven without also driving time.

#### What building the framework taught

The scheduler and the clock have to be used *together*; the clock alone reintroduces
the race it exists to remove. Driving `advance_to_next/0` in a bare loop against a
free-running system under test, the clock can reach the next deadline before the
process has handled the previous timer and armed its next one — so the loop sees
nothing pending and stops early. It cost a confusing test failure (1 tick instead of
200) before the cause was obvious. Under `dst_sched` the race cannot exist, because
the idle callback is consulted only once nothing is runnable. Worth stating plainly
in the framework's eventual usage docs: **`dst_time` is not a standalone facility.**

### Phase 2 — Deterministic backend — **DONE**

Shipped as [`src/dgen_mem.erl`](../../src/dgen_mem.erl): MVCC read versions,
read-conflict detection, versionstamps, watches, and seeded commit fault injection
(`conflict_p`, `commit_fail_p`). `dgen_backend` was already a behaviour with
`dgen_erlfdb` as its only implementation, so this needed no production change —
the seam was already there.

*Exit criterion, met:* `DGEN_BACKEND=dgen_mem mix test` runs the whole suite green
with no FoundationDB involved. Two tags are excluded there, both because they need
FDB by construction rather than by accident: `:cluster` starts real peer BEAM nodes
that open the backend themselves, which per-VM ETS cannot serve, and
`:differential` opens the FDB sandbox on purpose to compare the two backends
against each other.

**Key layout is reused, not reimplemented.** `erlfdb_subspace` and `erlfdb_tuple`
are pure Erlang — no NIF — so the packed keys are byte-identical to FoundationDB's
rather than merely similar. Reimplementing order-preserving tuple encoding would
have risked ordering bugs in `dgen_queue` that present as protocol bugs, which is a
poor trade for the small pleasure of being self-contained.

#### The MVCC window is a version distance, not a duration

Worth recording because the first instinct was to write it off. FoundationDB does
not measure its five-second MVCC window in seconds; it advances the read version at
a fixed rate (`VERSIONS_PER_SECOND` = 1e6) and states the window as
`MAX_READ_TRANSACTION_LIFE_VERSIONS` — five seconds' *worth of versions*. Modelling
it that way is a few lines, not a clock-dependent special case.

It also composes with Phase 1 to give something better than fidelity: under a
virtual clock the window is *controllable*, so `transaction_too_old` is reachable
instantly. The test that exercises it against FoundationDB needs a six-second sleep;
against `dgen_mem` it needs none.

#### Differential testing is what made it trustworthy

A reimplementation is worth exactly as much as the comparison behind it; testing it
against its own author's understanding proves nothing about fidelity. So the tests
run the same operations against both backends and assert identical results, and
every significant bug below was found that way rather than by reasoning:

| Bug | What FDB does | What `dgen_mem` did |
|---|---|---|
| Read of a key with a pending versionstamped write | raises `accessed_unreadable` (1036) | returned the stale committed value |
| `keys_in_range` at an existing start key | includes it | excluded it (`ets:next/2` is strictly-greater) |
| `on_error/2` on `transaction_too_old` (1007) | retryable | treated as fatal |
| `get_versionstamp/1` | future, resolves after commit | resolved eagerly to `undefined` |
| Concurrent commits | serialised | interleaved |
| A watch's baseline | the creating transaction's read | the moment it was registered |
| A commit's version vs. an outstanding read version | strictly greater | could be equal |

The first is the one worth dwelling on. Returning a stale value where the real
backend raises is the worst shape an infidelity can take: a loud error becomes a
silent wrong answer. It surfaced as `dgen_server`'s state cache serving stale
state, because the cached version matched a version key the transaction had already
written but could not yet read. Nothing in the suite pointed at the backend.

#### Commits must be serialised, and the lock must survive a kill

FoundationDB serialises commits. Without the same guarantee a second transaction
slips between a commit's conflict check and its writes, and three things break
quietly: a conflict is missed (so a transaction that should have been *fenced*
commits — §5.1's leader fence), a key's version list loses an entry to a lost
read-modify-write, and versions land out of order so a read returns an older value
than it should. This is what the intermittent failures were.

The fix is an ETS lock rather than a serialising process, for the same reason
`dst_time` has none: a blocking call into a process reads as quiescence to
`dst_sched`, so every commit would end a scheduling step.

That introduced a second bug immediately, and a more instructive one: `try ...
after` **does not run when a process is killed untrappably**, and killing a process
mid-flight is routine here — a simulated member crash is exactly that. One such
death wedged every later commit in the VM, which presented as the whole suite
hanging in an unrelated `on_exit`. The lock now reclaims from a holder that is no
longer alive. Any lock in code that also injects crashes needs this property, and
it is easy to leave out because the happy path is unaffected.

#### A watch is anchored to a transaction, not to a moment

The last two rows of that table are one bug wearing two faces, and it is the most
instructive thing Phase 2 produced — because the symptom pointed nowhere near the
cause and the obvious diagnosis was wrong.

The symptom: `call/4` in `dgen` occasionally exited `:timeout` under `dgen_mem`, at
roughly one run in five, always taking the full timeout rather than churning. The
first hypothesis was starvation — `dgen_mem` retries a conflict immediately where
FoundationDB backs off exponentially, so two contending consumers might livelock.
Plausible, and wrong. A livelock burns time; this was a *lost wakeup*, which is why
it cost exactly the timeout and not a variable amount.

FoundationDB defines a watch relative to the value the creating transaction would
read. `dgen_mem` registered it against the present instead, which loses any write
landing between the transaction's read and the watch's registration:

- `consume_queued/5` in `dgen_server` reads an empty queue, then watches the push key. A
  push committing in that window left the consumer asleep on a queue that already
  had work — and nothing in the consumer, the queue, or the caller was wrong.
- `push_call/7` in `dgen` writes the reply sentinel and watches it in one transaction.
  Registering against the present meant the transaction's *own* commit fired it, so
  every call did a wasted read-and-rewatch round trip — and that round trip is
  precisely the window the first case then lost a reply in.

Underneath sat a second defect that made the fix look broken: `do_commit/3` chose
`max(LastCommit + 1, Clock)` as its version, so with the clock ahead of the commit
counter — the usual case — a commit could land at exactly a read version already
handed out. Both `has_conflict/3` and the corrected `watch/3` ask whether a key
changed *after* a read version, so equality there is a conflict silently not
detected and a watch that never fires. Commit versions are now
`current_version(Db) + 1`: strictly ahead of the clock and of the last commit,
hence of every read version already issued.

Two things generalise from this. First, **"it times out" is a shape, not a
symptom** — a full-timeout stall and a churning one have different causes, and the
distinction was available before any code was read. Second, the differential test
that settled it took four lines against each backend and was decisive where a day
of reasoning about the retry path would not have been: FoundationDB fired, and
`dgen_mem` sat silent.

#### A test that was racy for a different reason than it looked

One failure looked like a latent test race — an async cast followed immediately by
a read — and the tempting fix was to make the assertion wait. That would have been
wrong: `DGen.Server.cast/2` and `call/2` **both ride the durable queue**, so FIFO
ordering guarantees the read observes the earlier cast. The failure was the
versionstamp-readability bug above, and weakening the test would have hidden it and
left a weaker test behind. Worth remembering when a new backend makes an old test
fail: the faster substrate exposes real bugs at least as often as it exposes racy
tests.

#### A test that asserted the schedule instead of the property

The counterexample to the section above, and a useful one, because the rule is not
"never weaken a test" — it is "assert the property, not the schedule that usually
produces it".

`DGen.RegistrySimTest`'s Guarantee 13 check flips 150 registrations at once and
samples a follower while the batch replicates, asserting it only ever sees 0 or 150
matches. It began failing intermittently once Phase 1 wired the transform into test
builds and shifted the timing: *"the query observed 2916 half-applied batch
state(s); counts were [1]"*. But a count of 1 was not a torn read. The test uses a
pacer op to take the leader's first commit slot so the flips ride a single batch,
and when the split landed 1-then-149 instead, both batches were applied perfectly
whole — the assertion was pinned to a batch boundary the test does not control.

It now derives the legal counts at runtime, by tracing the `{names_batch, …}` casts
the follower actually receives and taking their prefix sums. That is strictly
stronger than `0 or n`: it holds for any split, and it rejects a genuinely torn
read under every split rather than only under the lucky one. Three non-vacuity
guards keep it from degrading into a tautology — the sampler must straddle the
transition, no resync may have re-baselined the replica, and the largest batch must
carry at least half the ops, since 150 batches of one would satisfy the boundary
check while testing no atomicity whatsoever.

The general shape is worth naming, because a DST framework exists to make schedules
controllable and this is what the alternative costs: **a test that hardcodes an
uncontrolled schedule reports timing changes as correctness failures**, and each one
spends the credibility the suite needs for its real findings. The two options are
to derive the expectation from what happened, as here, or to drive the schedule so
it cannot vary — which is what `dst_sched` will make possible in Phase 3.

### Phase 3 — Extract the framework — **in progress**

- ~~The behaviour, `dst_run`, record/replay, failure reporting.~~ Shipped as
  [`src/dst_harness.erl`](../../src/dst_harness.erl) and
  [`src/dst_run.erl`](../../src/dst_run.erl).
- ~~**Port a second, unrelated SUT.**~~ Shipped as `test/support/dst_2pc*.erl`.
- ~~`dst_sched` into its own process~~ — done first, as a prerequisite.
- ~~Port `dgen_registry`'s simulation onto the behaviour.~~ Shipped as
  `test/support/sim/registry_sut.ex`, reusing `DGen.Sim.Invariants` unchanged.
- ~~Scheduler-owned spawn.~~ Built; late adoptions on the registry fell from
  1212/1233 to 1/431. The residual is OTP's own `proc_lib` spawning, which is a
  Phase 5 item — see below.

*Exit:* `dgen_registry`'s simulation is purely an implementation of the behaviour,
and a second system runs on the same framework unmodified.

#### The scheduler needed its own process after all

Phase 0 ran `dst_sched` in the calling process, arguing that a scheduler process
would itself need scheduling. That is false — nothing under test ever calls the
scheduler, so it is never part of the system being serialized — and the argument
cost a whole class of bug that only discipline prevented (§Phase 0's catch-all
receive, which ate the system under test's replies to the driver).

Phase 3 forced the issue rather than merely tidying it. `dst_run` interleaves
stepping with invariant checks and operation injection, so it needs a mailbox the
scheduler is not also reading. The refactor came first for that reason.

`sched()` is now a handle to a process. The API still threads it through, which
keeps call sites unchanged, and the returned value is the same handle rather than
updated state — removing a false affordance rather than adding one. The old record
was never usable as a value: the scheduler's real state is the suspend status of
live OS processes, so keeping an earlier `sched()` and stepping it again was never
going to replay anything.

#### Two-phase commit, because reusability has to be exercised

The second system under test is 2PC: a coordinator, three participants, an
atomicity invariant, and nothing whatsoever in common with the registry — no
leader, no replication, no durable backend. It carries a defect selectable by
config (`first_vote_wins`, deciding on the first vote to *arrive*), because a
framework that has only ever been pointed at correct systems has not been shown to
find anything.

Two measurements, and the difference between them is the point:

| | |
|---|---|
| 25-transaction runs finding the defect | **60/60 seeds** |
| **1**-transaction runs finding the defect | **52/200 seeds** |

The first number looks like the impressive one and is nearly worthless: a defect
with twenty-five chances to fire says nothing about whether the scheduler explores.
The second is the real result. With the workload *pinned* — same votes every run,
so `generate/2` draws nothing from the seed — the seed varies exactly one thing,
the order in which three votes reach the coordinator, and that alone decides
whether the run violates atomicity. Same input, different schedule, different
outcome, reproducibly. That is the claim of deterministic simulation testing stated
as a test, and nothing else in this suite states it.

Also measured, on the correct coordinator: 40 seeds give 40 distinct traces, and
every run advances the virtual clock to a full 30-second prepare timeout — 20
simulated minutes across the forty, in well under a second of real time.

#### The registry port, and the gap it found

`DGen.Sim.RegistrySut` runs a real three-member cluster under `dst_run`, reusing
`DGen.Sim.Invariants` **unchanged** — which is what this document originally
predicted, was wrong about for a while, and is now true, because the fix went into
the observation mechanism (`dst_observe`) rather than into the invariants.

Three of its four properties hold: runs complete cleanly, the invariants are
checked against a frozen system after every step, virtual time carries the waiting,
and different seeds produce different schedules. **A seed does not yet reproduce
its own schedule**, and the replay test is skipped rather than weakened.

The cause is measured. Of 1233 processes in one run, **1212 were adopted late** —
spawned by a system process and left running on the real scheduler until the
`set_on_spawn` trace event was handled. The registry spawns a short-lived helper
for almost every operation, so almost nothing it creates is under control from
birth, and the interleaving of those helpers is wall-clock rather than schedule.

The remedy was the reinstated table row, and it is built: `dst_sched:spawn/1` and
friends start the child **blocked** on a token, so there is no window at all. The
child is spawned by MFA rather than as a fun, which is what lets the scheduler tell
a gated child from one it must chase — the spawn trace event carries
`{dst_sched, gated, _}`. Inert with no scheduler running, like everything else here.

**Measured: 1212 late adoptions in 1233 processes became 1 in 431.**

The residual is not dgen's to fix, and identifying it is the useful part. The
remaining ungated children are `proc_lib:init_p` — processes OTP creates inside
`gen_server:start_link`, in modules no transform of *this* codebase touches, about
seven per run. That is precisely the OTP-coverage problem Phase 5 exists for, and it
is now a concrete instance rather than an estimate: **full replay on `dgen_registry`
is gated on Phase 5**, not on anything left in Phase 3. Pure-Erlang systems that
spawn through the transform — `dst_2pc` — are unaffected.

Two smaller things the port needed, both worth knowing before porting anything
else:

- **It only runs on `dgen_mem`.** FoundationDB transactions expire on the real
  clock, so a member suspended mid-transaction dies of `tooslow`. Phase 2 was the
  prerequisite for Phase 3 in a more literal sense than the phase ordering implied.
- **Operations must be expressed in terms stable across runs.** A cluster's member
  names embed a unique integer, so an operation carrying one makes the trace
  incomparable between runs and unreplayable against a fresh cluster. The failure
  looked exactly like a scheduling divergence — the traces differed at index 0 —
  and was nothing of the sort. Operations name members by index now. This is the
  same discipline the "four sources of nondeterminism" table asks for around refs
  and pids, applied to the workload rather than to the code under test.

- **A system with periodic timers never quiesces**, so "nothing runnable and no
  timers pending" is not a termination condition for it. `dgen_registry` heartbeats
  forever, and the first port ended every run on the step budget. `dst_run` grew a
  `settle_steps` phase: once the operations are exhausted, run a bounded while
  longer and finish `ok`. That phase is not dead time — it is the only place the
  invariants are checked with no client traffic, which is where the quiet-period
  defects live.

#### What building the driver taught

**A plain `spawn` in `execute/2` is a determinism hole.** Operations cannot be
executed synchronously — every process the scheduler owns is suspended, so a call
into one from the driver is never answered — so an operation spawns a process. But
the driver is not traced, so `set_on_spawn` does not adopt it, and the new process
runs on the *real* scheduler until the driver registers it. One racing process is
usually harmless; two, from operations injected close together, race each other to
deliver their first message. It presented exactly as it should have: one seed
producing two different traces. `dst_run:spawn_op/1` parks the process on a
handshake until registration is done, which closes it.

**Invariants must read state out of band, and the failure mode is silence.**
`check/1` runs against a frozen system, so anything that sends a message and waits
cannot be served. `dst_run` bounds the call and reports `check_blocked`, and there
is a system under test in the suite whose whole purpose is to make that diagnosis
fire. But the likelier mistake is worse and nothing can catch it:
`dgen_registry:status/1` catches its own call timeout and returns `undefined`, so
an invariant built on it computes over "no member believes it leads" and passes,
having checked nothing. **This is what blocks porting the registry** —
`DGen.Sim.Invariants.check_always/1` reaches `status/1` today, so it would go
quietly vacuous under the scheduler. The leader checks have to be rewritten to read
ETS, the way `Cluster.replica/1` already does.

**Leaving the system alone has to be a deliberate choice.** The obvious driver
injects an operation whenever nothing is runnable. That is wrong, and it is the
kind of wrong that removes a whole class of defect from reach: `dgen_registry`'s
traffic-triggered resync gap only manifests when nothing else is happening, so a
client that always has something to say papers over it before it can be observed.
`dst_run`'s `quiet_p` is the probability of letting time pass instead, and it
exists for that reason rather than as a tuning knob. Acceptance criterion 4 is
unreachable without it.

### Phase 4 — Shrinking — **DONE**

Shipped as [`src/dst_shrink.erl`](../../src/dst_shrink.erl), with exit criteria in
`test/dst_shrink_test.exs`. Delta debugging over the recorded trace, shrinking the
operation list and the choice sequence together — they are entries in one list, so
one pass does both.

*Measured* against `dst_2pc`'s planted defect: **26 entries to 8**, in 142 candidate
replays and 24ms, verified to replay strictly. What comes out is readable, which is
the whole point — the transaction whose plan carries the dissenting vote, then the
six scheduling decisions that let a `yes` reach the coordinator first.

#### The oracle has three answers, and conflating two of them is the bug

A candidate is tested by replaying it, and the result is the same violation, a
clean run, or a **divergence** — the candidate is not a valid schedule and is
evidence of nothing. Treating divergence as "did not fail" is the classic way to
build a shrinker that appears to work and quietly reverts safe removals.

Divergence is avoided here rather than classified. `dst_run:replay/3` grew a
`lenient` mode that skips a step naming a process that is not currently runnable,
because entries are not independent: removing an operation necessarily strands the
steps belonging to the process it created, so under strict replay nearly every
candidate diverges and the search learns nothing.

That makes the surviving candidate a recipe rather than an artefact — a lenient
replay silently dropped entries, so the candidate is not something to hand anyone.
What the run *executed* is, and `dst_run` records exactly that, so a shrink ends by
taking the executed trace and verifying it **strictly**. That verification is not a
formality; it is the only thing between this and a shrinker that reports a
beautifully small trace nobody can reproduce.

#### Positional ids bound how much can be removed

`dst_sched` assigns ids in registration order, so deleting an operation renumbers
every process created after it and the surviving `{step, Id}` entries stop naming
what they named. Measured on the shrunk 2PC trace: removing the leading operation —
entirely irrelevant to the violation — left the run clean with six steps skipped,
because those six no longer referred to the client that mattered.

So an irrelevant leading operation can survive shrinking, and did. The result is
still a genuine repro; it is the *minimality* that is approximate. Closing it means
remapping ids as entries are removed, which is a real extension rather than a
tuning change.

#### What a multi-invariant system must supply

The default "same failure" test is the violation's `property` when the system
reports one, and *any* violation otherwise. The fallback is honest rather than
good: with more than one invariant it will happily shrink one bug into a different
one, which is worse than no shrink because it looks like progress. `shrink/3` takes
a `match` predicate for exactly that, and `dst_2pc`'s test suite exercises the case
where nothing matches — the original trace comes back with `verified => false`
rather than a smaller trace implying success.

*Exit:* see Acceptance below.

### Phase 5 — OTP coverage and hardening (~3–4 weeks, highest risk)

The expensive tail, and the reason "true DST" is a months-long effort rather than a
weeks-long one.

A `parse_transform` over SUT modules does not touch `gen_server`, `supervisor`, or
`proc_lib`, so **OTP's own timers and receives stay on the real clock**. Three
options, none free:

1. Compile sim-profile copies of the OTP modules with the transform applied.
   Thorough, and a maintenance burden that tracks OTP releases.
2. Configure timeouts to `infinity` in simulation and let the framework's timers be
   the only ones. Cheap, but changes the behaviour under test — timeouts are exactly
   what several `dgen_registry` paths hinge on (§3's register blocking, §5.5's
   replicate-before-ack).
3. Sim-aware wrappers the SUT calls instead of `gen_server:call` etc. Invasive to
   the SUT, contrary to the "runs the real code" goal.

Recommendation: (1), scoped to the handful of modules that actually matter, with (2)
as the fallback where (1) proves impractical.

**That recommendation was wrong, and both of the reasons it was wrong are worth
keeping.** Option (1) is not merely expensive, it is close to unusable; and the
problem it was meant to solve has a fourth answer the list did not contain.

*Exit:* no real-time dependence anywhere in a run.

#### Shadowing an OTP module: what actually happens

Before building anything, the obvious first step is to check whether option (1) is
even permitted. It is not, without deliberate effort:

- **stdlib is sticky.** `code:load_binary/3` on `proc_lib` fails with
  `{error, sticky_directory}`. A shadow requires `code:unstick_dir/1` on stdlib's
  ebin first — a thing no test suite should be doing lightly.
- **The blast radius is the whole VM.** With a deliberately broken `proc_lib`
  loaded, `Agent.start_link/1` fails immediately with `undef`. Every OTP process
  start goes through it, ExUnit's and Logger's included, for the entire run.
- **It is ~1600 lines of copied OTP** pinned to one release, where a mismatch on a
  future OTP is a subtle breakage rather than a loud one.

None of that is fatal, but all of it is permanent, and the whole cost buys one
thing: control of where OTP spawns.

#### Building the child instead of intercepting it

The fourth option, and the one that shipped: **do not intercept OTP's spawn — make
an equivalent process yourself.**

`gen_server:enter_loop/3` lets a process that `gen_server` did not start become a
`gen_server`. So `dst_sched:start_monitor/3` spawns a *gated* process, sets up the
two process-dictionary entries `init_p/3` in `proc_lib` would have set, runs `init/1`,
acknowledges, and enters the loop. `dst_transform` rewrites `gen_server:start_*` in
transformed modules, so `dgen_transaction` gained three inert lines and no
production code names `dst_sched`.

Verified to produce a genuine OTP process — `sys:get_state/1`, `sys:get_status/1`
and `sys:suspend/1` all work on it, and `$initial_call` reads correctly. Continue
chaining is handled explicitly, because `enter_loop/3` takes a state rather than a
continue and `dgen_transaction:init/1` returns `{ok, St, {continue, do_begin}}`.
Unsupported init shapes raise rather than guess.

No unsticking, no copied OTP, blast radius of one call site. The lesson generalises
past this framework: when interception is blocked, construction may not be.

#### Replay took three causes, not one

Each looked like the whole problem when it was the visible one, and each was found
by measuring rather than reasoning:

| | Cause | Remedy | Effect |
|---|---|---|---|
| 1 | Spawned children adopted after they had started running | gated `dst_sched:spawn/1` | `adopted_late` 1212/1233 → 1/431 |
| 2 | OTP spawning inside `gen_server:start_monitor` | `dst_sched:start_monitor/3` | `adopted_late` → 0 |
| 3 | Handover that was not quiescent | `init/2` waits for it | distinct traces 8 → 5 |

The third is the one to remember. `dst_harness` requires `init/2` to return with the
system quiescent, and `dgen_registry`'s own SUT violated that: `Cluster.start/3`
waits for *readiness*, which is not the same thing, and about one start in five
left a member's elector still running an unprocessed call. Everything else about
the handover was identical — leader, epoch, applied version, process count, and the
whole timer wheel — so it looked like anything but a startup problem.

Two hypotheses that fit the evidence were tested and rejected on the way, both
plausible enough to have been believed: state carried between runs in the shared
`dgen_mem` database (a fresh database per run gave the same pattern), and timer
insertion order at startup, since `dst_time` breaks deadline ties by sequence (the
wheel was identical across six runs).

One seed still does not replay, and both of those explanations are now dead.

#### `receive ... after` is rewritable after all

This document assumed, in several places, that a transform could reach a module's
timer *calls* but not its receive timeouts: `after` is a language construct, so
there is nothing to redirect. The assumption drove Phase 5's shape — option (1)
looked expensive partly because a transformed OTP module would still have had
real-time receives in it.

It is wrong. `receive Cs after T -> B end` is the five-element abstract form
`{'receive', Anno, Cs, T, B}` — fully visible to a transform, and rewritable into a
receive with no real-time dependence:

```erlang
begin
    Ref  = make_ref(),
    TRef = dst_time:arm_after(T, Ref),
    receive
        Pat1 -> dst_time:disarm_after(TRef, Ref), Body1;
        ...
        {'$dst_after', G} when G =:= Ref -> B
    end
end
```

Shipped as a pass inside [`src/dst_transform.erl`](../../src/dst_transform.erl), with
`dst_time:arm_after/2` and `disarm_after/2` behind it and the properties pinned in
`test/dst_after_test.exs`. Measured: a 60-second timeout resolves in single-digit
milliseconds, and the same receive compiled without the transform ignores the
virtual clock entirely.

Not yet applied to any `dgen` module. Adoption is still a Phase 5 decision — the
point of building it now was to make that decision against measurements instead of
an assumption.

**Three things the obvious rewrite gets wrong**, none of which were visible by
reading it:

1. **A zero timeout must not go through the timer wheel.** `after 0` is a mailbox
   poll, not a wait. Routed through a timer it becomes a wait on something nothing
   may advance — 500ms and a leaked timer, where the original cost nothing.
   `arm_after/2` delivers a zero straight to the caller's own mailbox, which also
   preserves ordering: a selective receive scans in arrival order, so an
   already-queued message still wins.
2. **The disarm must go at the head of each clause, not after the receive.** The
   natural form — save the result, disarm, return it — takes the receive out of
   tail position, turning `loop() -> receive ... -> loop() end` from constant stack
   into unbounded growth. Measured at 200,005 words over 50,000 iterations against
   5 for the untransformed original. A block is transparent to tail position, so
   moving the disarm into the clause heads restores it exactly; it also makes the
   disarm exception-safe for free.
3. **The timeout clause must match a fresh variable and compare in a guard.**
   Re-matching the bound ref is correct but warns on every rewritten receive, which
   breaks `--warnings-as-errors`.

The cost it does carry: a timer is created and cancelled even when a matching
message was already waiting, which the native `after` does not pay. Two ETS
operations per receive — irrelevant for an occasional timeout, worth weighing
before putting it on a hot receive loop.

#### Mix does not recompile a module when its parse transform changes

Found while checking that the tests above actually fail when the transform breaks.
They did not. Reintroducing the tail-position bug left the suite green, because
`mix compile` rebuilt only `dst_after_transform` and left every module built with
it stale; the failure appeared only after touching the SUT source by hand.

This is a general hazard for transform-based testing, and it applies equally to
`dst_transform` and `dst_timer_sut`. `test/dst_after_test.exs` closes it by
recompiling its SUT from source in `setup_all`, so the test can never exercise
yesterday's transform. Worth doing anywhere else a transform is the thing under
test — a green suite that cannot go red is the most expensive kind of test there
is.

### The first defect the framework found on its own

Not a planted one, and not one anybody was looking for. Reaching acceptance
criterion 1 needed an invariant strong enough to see a same-version replica
divergence; the first 200-seed sweep with that invariant in place reported nine
failing seeds on an **unfaulted** three-member cluster, and they were all one real
bug in `dgen_registry`.

**What it was.** A group commit may bind and clear one name in the same batch — a
`register` and an `unregister` of it arriving together. That is an ordinary
serialisation: the register is answered `yes`, the unregister then removes what it
bound, and every replica agrees, because the broadcast carries both ops in order.
What did not agree was the *forwarding* follower. Its `{register_reply, …}` arrives
behind the broadcast (FIFO), and `handle_register_reply/4` re-inserted the row on
`yes` — version-guarded, so the insert fired precisely when the batch that removed
the name had already been applied. The follower ended up holding a binding no other
member had, **at the same applied_version as all of them**: permanent, because gap
detection compares versions, and adoptable, because the freshest-wins gather (§5.7)
may pick that replica and fan it out. The same shape as the partial-batch
divergence, from a different cause.

The fix is a deletion — the reply handler answers and writes nothing, which is what
its two siblings (`unregister_reply`, `set_meta_reply`) already did, and what
`flush_deferred/1` already did for the deferred half of the same decision. The
correct behaviour was written down three times in the same module; this path just
did something else.

**What is worth keeping from how it was found.**

*Neither existing method could have found it.* The formal model has no unregister
action at all: a single name only ever changes by being *bound*
(`rep' = [rep EXCEPT ![f][m.name] = m.pid]`), and everything else replaces a whole
replica. A batch that binds and clears one name is outside its abstraction, so TLC's
`PrefixConsistency` proof holds over a protocol that cannot express the bug.
The pre-DST sim harness could not check `same_version_same_replica` at all except
after `Cluster.converge/2`, and converging *heals it*: the heal delivers
`{nodeup, _}`, every member re-joins, and a re-join makes the leader re-snapshot it.
The bug was reachable only in the window between "the system has stopped moving" and
"anything has been repaired", and nothing before this could name that window.

*The invariant had to be gated on quiescence, and getting the gate right took a
false positive.* Members write their replicas optimistically — `route_unregister/3`
deletes the row on both the calling member and the leader before anything commits —
so replica agreement is simply false in mid-operation. The first gate was "nothing
runnable", which the scheduler can answer directly (`dst_sched:current/0` exists for
that). It fired on a seed where two members had speculatively deleted a name the
third still held, and the run reported it as a divergence: the leader's commit was
parked on its transaction worker, so nothing was runnable *in the middle of an
operation*. The window a speculative write lives in is bounded by its operation, not
by the scheduler going quiet, so the gate is both — nothing runnable **and** no
client waiting. The general rule: **a property that only holds at rest needs the
system's own definition of rest, and "no process wants to run" is not it.**

*A fault model that cannot be healed has to be narrowed.* Loss on a request channel
(`unregister_req`, `resync_req`) is recovered in production by the `{nodeup, _}` a
reconnect delivers, so dropping those without delivering the node event injects a
fault real distribution cannot produce — messages vanishing while both ends believe
the link is up. The harness already pays that debt at heal time, but a run asserting
a property *during* the fault has no heal to pay it at, and delivering the node
event instead triggers the rejoin-and-resnapshot that erases the evidence. So the
DST port drops only the leader's replication broadcasts, whose recovery is
self-contained: the follower sees a version discontinuity and asks for a resync,
with no node event involved. As it turned out the drops were not needed for this
defect at all — a perfect network produced *more* failing seeds than a lossy one
(9 of 200 against 4), because loss mostly re-baselines the replica before the
divergence can be observed.

*The network had to become a scheduled process.* `DGen.Sim.Net` sits on every
inter-member message. Left unscheduled it delivers them from a process running free
on the real scheduler, so whether a message is in a member's mailbox when the driver
next asks what is runnable is wall-clock timing — and it is also where the fault
decisions are made, which must be a function of the seed. It is registered with
`dst_sched` now, and its decision log is readable from ETS rather than by calling it,
for the same reason the invariants read ETS: at the one moment the log is most
wanted, the process holding it is suspended.

### The unexplained seed was never a seed

`dgen_registry` reproduced its own schedule for three of four measured seeds. Seed 3
did not, two hypotheses had been tested and rejected, and it stood as the framework's
one open determinism gap. It is closed, and the answer was in neither hypothesis
because the question was wrong.

**It is always the first run in a fresh VM**, whichever seed that happens to be.
Reversing the order of the measured seeds moves the failure with it: run it as
`[13, 11, 7, 3]` and seed 13 produces two schedules while seed 3 produces one. Five
runs of a seed group as `[[1], [2,3,4,5]]` — run 1 alone, the rest identical.

**The cause is on-demand code loading.** A module that has not been used yet is
loaded on first call, and that load is a *synchronous call to `code_server`* — a
process `dst_sched` does not own. So a scheduled process reaches a module it has not
touched, blocks on a process outside the schedule, and the scheduler reads that as
the step ending. The code server, running free, makes it runnable again at a moment
wall-clock timing decides, and every choice after that point shifts.

It is invisible to everything that was tried, which is why it survived three
investigations: no process is adopted late, no scheduler timeout fires, no warning is
logged, and the process really did block — the scheduler's accounting is correct
throughout. What finally showed it was logging **trace events against step
boundaries**: the step before the divergence had the member scheduled in and out of
`code_server:call/1`, three times, and nothing else in the run mentioned the code
server at all.

The fix is to load everything before the scheduler owns anything —
`RegistrySut.preload/0`, one call to `code:ensure_modules_loaded/1` per application.
Every seed measured now reproduces exactly (9 seeds × 3 runs, and 4 × 5 in both
orders).

Two things worth carrying to other systems under test:

- **`init/2`'s contract is broader than "quiesce the system".** It is "hand over a
  system that will not do anything the schedule did not ask for", and a first-touch
  code load is exactly that, as much as an in-flight `gen_server` call is. Any SUT
  wants the same preload.
- **A determinism bug that only shows on the first run of a VM is invisible to the
  natural experiment.** Measuring a seed by running it five times in one VM puts four
  of the five runs in the warm state, so the outlier reads as flakiness in the *seed*.
  Measuring identically-shaped runs and looking at *which* of them differs — rather
  than how many — was the step that turned this from a mystery into a one-line fix.

### The trace was missing an action

Found while asking why `dst_shrink` did nothing on the defect above: it reported
"nothing to shrink" for a failure that had just happened, because `classify/3`
replays the trace to confirm it and the replay came back clean.

`dst_run` records `{step, Id}` and `{op, Op}`. Its third action — **advancing the
virtual clock** — was not recorded at all, on the reasoning that the clock is a
function of the timers and the timers are a function of the schedule. But
`replay_loop/2` walks only the entries it is given, so a replay of a system with
timers ran against a clock that never moved: nothing became runnable, every recorded
step was refused, and a strict replay failed at *entry 0*. Which reads as a scheduling
divergence at the very first choice, and is nothing of the sort.

`{clock, Ms}` is an entry type now, holding what the clock read after the advance;
strict replay holds the replay to it and lenient takes whatever advance is available,
the same split `{step, Id}` already had. Registry traces replay strictly with nothing
skipped.

The reason this survived is worth naming: **`replay/3` had only ever been exercised
against a system with no timers.** `dst_2pc` has none, so its traces were complete by
accident, and the registry's replay was measured by *re-running the seed* — which
exercises `run/2`, not `replay/3`. Two things that both look like "replay works" and
only one of them was tested.

## Effort

| Phase | Estimate | Cumulative |
|---|---|---|
| 0 — scheduler | **done** | — |
| 1 — virtual time | **done** | — |
| 2 — deterministic backend | **done** | — |
| 3 — framework extraction | **mostly done** | — |
| 4 — shrinking | **done** | — |
| 5 — OTP coverage | **mostly done** | — |

Engineer-weeks, and estimates rather than commitments — Phase 0's
selective-receive work and Phase 5 both have real discovery risk.

**A useful stopping point exists at ~8 weeks** (phases 0–2 plus a thin driver):
deterministic scheduling, virtual time, no FDB dependency, and thousands of seeds
per run. The reusable framework and shrinking are what phases 3–4 add, and they are
what make it worth doing as a framework rather than as more `test/support/sim/`.

## Acceptance criteria

The framework should be judged on whether it can find a bug we already understand,
from a cold start.

1. **Rediscover the partial-batch divergence.** Revert the `names_batch` commit and
   require the framework to find a same-version replica divergence within a bounded
   seed budget, with no test written specifically for it.
2. **Shrink it** to a schedule a human can read — on the order of ten steps, not
   thousands.
3. **Replay it** bit-for-bit, on a different machine, from the seed alone.
4. **Rediscover the traffic-triggered resync gap** likewise, which needs virtual
   time to be reachable at all (it only manifests when nothing else is happening).

Criterion 1 is the real test. A DST framework that cannot re-find a bug we know is
there, in code we know contains it, is not yet working.

### Results — 1, 2 and 3 met; 4 outstanding

The mutation is `-DMUTATION_PARTIAL_BATCH` in `dgen_registry_member`, off unless
`DGEN_MUTATION=partial_batch` asks for it and impossible in a release build (see
`erlc_options/1` in mix.exs). It restores both halves of the pre-fix protocol: one
broadcast message per op sharing a version, and `apply_bcast/6`'s "another message
of the batch we are already applying" clause. The criteria run as
`test/dgen_registry_mutation_test.exs`, in about 13 seconds.

**1 — Rediscovered.** Under a three-member cluster dropping one replication
broadcast in five: **11 of 50 seeds** violate `same_version_same_replica`, the first
at **seed 2**, about 0.3 seconds of searching. No test was written for the defect;
the property is the spec's corollary, evaluated at quiescence.

Worth being precise about what "no test written for it" bought and what it cost. The
framework did not find this from nothing: it needed an invariant that can observe a
same-version divergence and a fault that can produce a partial batch. Both are
general — the invariant is `PrefixConsistency`'s stated corollary, and the fault is
"a follower briefly loses the leader's stream" — but neither existed before the
criterion was attempted, and building the invariant is what found the *unplanted*
defect above. The honest summary is that the framework re-finds the bug in seconds
once it can see the property, and seeing the property was the work.

**2 — Shrunk, with a caveat that is the shrinker's known limit rather than a
surprise.** A twelve-operation trace is 71–85 entries and ddmin removes **0–3** of
them; it never removes an *operation*. That is exactly the positional-id bound
recorded in Phase 4: deleting an operation renumbers every process created after it,
so the surviving `{step, Id}` entries stop naming what they named and the candidate
stops failing. The 2PC example understated the cost, because 2PC has few processes.

What produces something readable is a smaller *workload*: the defect still reproduces
with **three** registrations, and that trace is **38 entries — 3 operations, 34
scheduling decisions, one clock advance** — verified to replay strictly. So the
target ("on the order of ten steps") is met in spirit at the scale of the workload,
and the lever that reaches it is reducing the operation count, not ddmin. Making
ddmin do that itself means remapping ids as entries are removed, still a real
extension.

**3 — Replays from the seed alone.** A failing seed reproduces its violation and its
exact schedule across runs (measured 8/8 identical, and asserted 3/3 in the test).
"On a different machine" is untested; what is demonstrated is bit-for-bit
reproducibility in a fresh VM, which is what the two determinism fixes above bought.

**4 — Not attempted.** The traffic-triggered resync gap needs the replication
heartbeat reverted as a second mutation. The machinery is now in place for it:
`quiet_p` makes the quiet period reachable, and `check_quiescent/1` is checked
precisely when nothing is happening, which is where that defect lives.

#### A fault schedule has to start where the run starts

The last thing between criterion 1 and criteria 2–3, and a general trap for any
seeded fault injector.

`DGen.Sim.Net` draws from its seeded RNG once per message the policy is in scope
for. The policy is set *after* the cluster is up — deliberately, so no member is
partitioned away before it has ever synced — and until then `drop_tags` is unset, so
every message of the cluster's own startup traffic drew from that RNG. Startup runs
on the real scheduler and varies in both order and message count, so the fault
schedule began at a **nondeterministic offset**. The symptom is unusually cruel: a
seed reproduces exactly right up to the first drop and never after, so the run looks
deterministic until precisely the moment it matters.

`set_policy/2` takes a `seed` now and restarts the schedule there. The general
statement: **a seeded fault schedule is only reproducible from the point its seed is
applied**, and anything that consumed the generator before that — setup, warm-up, a
previous phase of the same run — silently offsets it.

#### The framework found a code-server round trip in production code

`emit_telemetry/3` called `code:ensure_loaded(telemetry)` on every event.
`code:ensure_loaded/1` short-circuits on an already-loaded module but not on a
missing one, and `telemetry` is an optional dependency most deployments do not have
— so the common configuration paid a synchronous `code_server` call per event,
forever. Cached in `persistent_term` now.

It was found because `code_server` is a process the simulation does not control, so a
member emitting an event blocked outside the schedule. Two of the three determinism
bugs in this document turned out to be code-server round trips, which is worth
generalising: **under a user-level scheduler, any synchronous call to an OTP
service process is a real-time dependence**, whether it is `code_server`, `logger`'s
handlers, or `global`. Grepping for those is a cheaper first move than the
instrumentation it took to find these.

## Alternatives considered

**Concuerror** — a maintained systematic concurrency tester for Erlang, using DPOR
to explore interleavings exhaustively. It builds the scheduler we would otherwise
write, and it requires exactly what phases 0–2 produce anyway: no NIFs (Phase 2's
backend) and no distribution (already achieved — the `keyspace` option means
`dgen_registry` runs multi-member in one VM). Different tool for a different job:
DPOR exhausts *small* executions, DST samples *long* ones. Best outcome is both,
and spiking it in Phase 0 could materially cut the cost of everything after.

**PULSE** — the historical answer to this exact problem (a user-level scheduler with
randomized-but-replayable schedules). Effectively unmaintained; worth reading for
design, not for adoption.

**More property-based testing** — cheaper and complementary, but it samples *inputs*
and does not control *interleavings*, which is where the bugs found so far have
lived.

**Do nothing.** Defensible. The current harness found three real defects at a
fraction of this cost. The case for proceeding is the speed multiplier (item 2 at
the top) and exact replay — not a claim that the current one is exhausted.

## Open questions

- Does Concuerror's DPOR subsume enough of phases 0 and 4 to change the plan?
- How much of OTP genuinely needs the transform? Possibly just `gen_server` and
  `gen_statem`; worth measuring before committing to Phase 5's estimate.
- Is `erlang:suspend_process/1` safe enough at scale? It is documented as intended
  for debuggers, and misuse can deadlock. The probe is clean at three processes;
  behaviour at hundreds, with supervisors and monitors, is unverified.
- Should the virtual clock also drive `dgen_transaction`'s commit worker, or should
  the sim backend simply complete commits synchronously under the scheduler?
- `dgen_mem` has no retry backoff where FoundationDB backs off exponentially with
  jitter. A real `timer:sleep/1` is the wrong answer — a sleeping process is
  neither runnable nor blocked on a receive, so `dst_sched` cannot step it — so a
  backoff worth having is one the scheduler drives. Left out until a workload is
  observed to need it; the timeouts that prompted the question turned out to be the
  watch-anchoring bug instead.
