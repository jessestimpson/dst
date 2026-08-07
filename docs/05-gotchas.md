# Gotchas and footguns

Grouped by where they bite. If you're here because a seed stopped reproducing,
start with the method rather than the list.

## AI disclosure

This document is pending a human rewrite. We still expect the content to be mostly
correct.

## How to debug a determinism failure

Reading the code and thinking hard has a poor record against "the same seed
produced 2 different runs". 3 techniques work better.

**Look at which runs differ, not how many.** Run a seed 5 times and group the
identical traces. A grouping of `[[1], [2,3,4,5]]` says run 1 is special, which
points at warm-up or first-touch state. `[[1,3], [2], [4,5]]` says the
divergence is a coin flip on every run. The 2 have completely different causes,
and the distinction costs nothing to measure.

**Vary the position rather than the seed.** If seed 3 is the one that fails, run
your seeds in reverse order. If the failure follows whichever seed is now first,
the seed was never the variable and you're looking at the first-run problem
below.

**Snapshot everything at the point of divergence and diff it.** State, mailbox
contents, process status, the timer wheel. Twice here the difference has been
invisible in state and obvious in mailboxes. When even mailboxes matched, what
worked was logging scheduler events against step boundaries: which process was
scheduled in and out, and what it was executing at the time.

Bisecting what you compare helps too. Removing the client workload entirely and
finding the system still nondeterministic, or suddenly not, narrows the search
for very little effort.

**Put the evidence in the failure message rather than deriving it.** The most
recent determinism bug found in this library survived 3 rounds of theorising.
What worked was adding `inspect(eta_run:summary(Result))` to the assertion
message and looping until it failed. The answer arrived in one shot and was none
of the 3 theories.

That generalizes past determinism bugs. A test whose failure message carries the
run's summary costs one line and saves the round trip where you guess, add a
print, and try to make it fail again.

## The code server

**`modules_loaded` in a run result is what tells you.** It lists modules loaded
while the scheduler was in charge, and it should be empty. `eta_run:audit/1`
checks it for you.

Before that field existed the symptom was all you had: the first run in a fresh
VM produces a different trace from every subsequent run of the same seed, with
nothing adopted late, no scheduler timeout, no warning logged, and the
scheduler's accounting correct throughout.

Loading a module on demand is a synchronous call to `code_server`, which the
scheduler doesn't own. A scheduled process that reaches a module it hasn't
touched yet blocks on something outside the schedule. The scheduler reads that
as the step ending, the code server runs freely and makes the process runnable
again at a moment decided by wall clock, and every choice after that point
shifts. On the second run everything is loaded and it never happens again.

The fix is the `preload` option. Name your own application; `kernel`, `stdlib`
and `eta` are always included.

```erlang
eta_run:run(my_harness, #{seed => 1, preload => [my_app]}).
```

If you drive `eta_sched` yourself instead of going through `eta_run`, nothing
preloads for you and there's no `modules_loaded` afterwards to tell you it
happened. Call it directly, once, before the first scheduler exists:

```erlang
ok = eta_run:preload([my_app]).
```

Running a seed twice is not a substitute, and this is worth being precise about:
**warming is per code path, not per VM.** A seed that first reaches a timeout
handler runs a branch no earlier seed touched and loads code no earlier seed
needed. Warming up fixes the seed you warmed and not the next one.

### The worse failure, and the field that catches it

A process waiting on `code_server` is, to `eta_sched`, a process blocked in a
receive. It isn't runnable. So if enough of the system reaches for cold code at
once, nothing is runnable, no timer is pending, and **the run ends at what looks
exactly like quiescence.**

Measured on this project's own two-phase commit example, with preloading off and
a cold module every client touches:

```
ops: 5, steps: 5, exited: 0, processes: 9, outcome: ok
```

5 steps for 5 operations, against a workload that normally takes about a
hundred. Reported as success. `modules_loaded` doesn't catch this one, because
the loads complete after the run has finished — there's nothing left to report
by the time anybody looks.

`sched.cold_code` catches it from the other side. The scheduler looks once, at
the moment a run finds nothing runnable and is about to return, for a process
sitting in `code_server:call/1` — which is where a process waiting on the code
server always is, because that function is a send followed by a one-clause
receive. Each entry names the process and the frame that reached the cold
module, and a warning is logged:

```erlang
#{sched := #{cold_code := [#{id := 3, pid := _, at := {my_client, commit, 2}}]}}
```

`at` is `undefined` when there's no frame to name, which happens when the call
into the cold module was a **tail call** — nothing of the caller is left on the
stack. The pid is still the answer then.

`audit/1` checks the field, so you get this for free if you're already asserting
on it.

None of that makes `preload` optional. Detection is after the fact: by the time
`cold_code` has something in it the run is already spoiled, and all you've
gained is knowing rather than guessing.

There's a second form that isn't a warm-up effect. `code:ensure_loaded/1`
returns immediately for a module that's already loaded, but for a *missing*
module it falls through to the code server every time. A common idiom for
optional dependencies looks like this:

```erlang
emit_telemetry(Event, Measurements, Metadata) ->
    case code:ensure_loaded(telemetry) of
        {module, telemetry} -> apply(telemetry, execute, [Event, Measurements, Metadata]);
        _ -> ok
    end.
```

For anyone without the optional dependency that's a synchronous code-server call
on every event, forever. Cache the answer in `persistent_term`.

Here's the general rule to carry to your own project. Under a user-level
scheduler, **any synchronous call into an OTP service process is a real-time
dependence**: `code_server`, `logger`'s handlers, `global`, `net_kernel`,
`application_controller`. Grepping for `code:ensure_loaded`, `code:is_loaded`
and `global:` along the paths your system exercises is much cheaper than the
instrumentation it takes to find these afterwards.

`logger` is the one of those the transform handles for you — see below.

### Logging is one of them, and it is handled

A `logger` call doesn't just format a string. It hands the event to a handler,
and the default handler `logger_std_h` is a process the scheduler doesn't own.
Under load it switches from asynchronous to synchronous, at which point a log
call is a synchronous call into an OTP service process. The handler then runs on
the real scheduler and makes your process runnable again at a moment nothing
chose, so a chatty system can have its schedule decided by how busy its log
handler happens to be. The symptom is a seed that mostly reproduces.

Every `logger` level function and `logger:log/2,3,4` is rewritten to
`eta_logger`, which records the event into `eta_log` while a run is collecting
and delegates to `logger` when one isn't. So your system's own logging becomes
part of the narrative `eta_log:analyze/0` prints, at a sequence number
comparable with everything else, instead of scrolling past in a separate stream.

Nothing is filtered by level and no formatter runs. The run's log is a record of
what happened rather than an operator's console, and deciding at record time what
a reader will want is how you lose the line that mattered.

## Build-time hazards

### Never give a module 2 parse transforms

Mix doesn't reliably order a module that names more than one. With 2 transform
attributes on a single module, an incremental build in which both the transform
and the module had changed failed with `undefined parse transform` about 2 times
in 3. It's nondeterministic, so it reads as flakiness rather than as a build
problem.

Delegating from one transform to another doesn't help, because the compiler
loads a transform module from the code path when it runs, so the ordering hazard
just moves down a level. If a module needs a second pass, add it to the
transform it already names and gate it behind an attribute. That's why
`eta_transform` carries both the state-publishing pass and the receive-timeout
rewrite as *passes*, and why modules opt into the first with `-eta_observe(...)`.
Those 2 used to be separate transforms and couldn't be used together.

It's also why you include `eta.hrl` rather than naming the transform: the header
carries the one attribute, so there's nothing to get wrong.

### Mix doesn't recompile a module when its transform changes

Edit a parse transform, run the tests, and the tests exercise yesterday's
transform. Everything passes. `mix compile` rebuilds the transform module and
leaves every module built with it stale.

The same applies to compiler options. Toggling a `-D` define doesn't reliably
rebuild the Erlang modules that depend on it, which matters if you use a define
to plant a defect deliberately. Run `mix compile --force` after changing either.
Where a test's whole purpose is to exercise a transform, recompile its subject
from source in `setup_all` so it can't exercise the stale one.

## Scheduler hazards

### A plain spawn is a determinism hole

A process created by a plain `spawn` isn't adopted until the scheduler notices
it, and in that window it runs on the real scheduler. One racing process is
usually harmless. 2, created by operations injected close together, race each
other to deliver their first message.

A gated spawn closes the window: the child starts blocked on a token only the
scheduler can send, so there is no interval in which it is alive and unowned.

**Inside a transformed module you get this for free.** Every local spawn form is
rewritten: `spawn`, `spawn_link`, `spawn_monitor` and `spawn_opt`, on both
`erlang` and `proc_lib`, qualified or bare. `start`, `start_link` and
`start_monitor` on `gen_server` and `gen_statem` are rewritten too, at both
arities, because those spawn inside OTP where no transform of your code
reaches.

**Inside `execute/2` you don't**, because your harness isn't transformed. Use
`eta_run:spawn_op/1` there. It is the same mechanism under a different name:

```erlang
spawn_op(Fun) ->
    eta_sched:spawn(Fun).
```

There used to be 2 gating protocols with 2 different tokens, and the harness had
to know which one it was in. There is 1 now, and the thing that releases a gated
child is `eta_sched:register/2` — which the driver already calls with whatever
`processes/1` returns after every operation. So the order operations become
runnable in is the order `processes/1` hands them over, and that is the only
ordering rule; it used to be that plus an undocumented one hidden in a
process-dictionary stack.

Watch `sched.adopted_late` in the run result. It should be at or very near zero.

### The distributed spawn forms raise

`spawn(Node, Fun)` and its relatives ask for a process on another node, and `eta`
does not simulate distribution. There is no honest rewrite: running the child
locally puts it on the wrong node, and letting it through puts it outside the
schedule entirely.

So while a run is active they raise:

```erlang
{eta_sched, {no_distribution, {erlang, spawn, 2},
             <<"eta does not simulate distribution; run the nodes as processes in one VM">>}}
```

With no run in progress they delegate, so a module built with the transform still
works normally outside a simulation. A multi-node system under `eta` runs its
nodes as processes in one VM.

### suspend_process/1 is documented for debuggers

That's what the scheduler is built on. It's clean at 3 processes and has been
fine at a few hundred, but behaviour at 1000s, with supervisors and monitors in
play, is unverified. Satisfy yourself about it before building on top for a very
large system.

### One clock, one log and one scheduler per VM

All 3 keep their state in named ETS tables, so exactly one of each exists per
node and runs have to be serial. Keep the tests `async: false`.

Getting it wrong is now loud rather than silent. Starting a second clock, log or
scheduler while a live process holds the first is refused, and the error names
the owner:

```erlang
{eta_time, {clock_in_use, <0.312.0>,
            <<"one per VM; keep runs serial (`async: false`) ...">>}}
```

It used to succeed. `eta_time:start/1` opened with an unconditional `stop()`, so
a second run deleted the first's tables and the first died on its next clock read
with an ETS `badarg` in a stack that named nothing to do with the cause.

If you want simulations to run in parallel, the unit of isolation is the **VM**,
not the test process. Run seeds in separate OS processes; that isolates crashes
and memory too. See [design.md](design.md) for why per-run state inside one VM is
a bigger change than it looks.

## Time hazards

### Start the clock before the system

`eta_run` does this for you, which is why `init/2` is called by the driver
rather than by you beforehand. A timer armed while `eta_time` is inert goes onto
the real clock and stays there until it fires and re-arms, so long-period
timers, the interesting ones, would silently never become virtual.

### receive ... after is handled

`eta_transform` rewrites calls, and `after` is a language construct rather than
a call, so this looks as though it can't work. It can: `receive Cs after T -> B
end` is an abstract form like any other, and the transform rewrites it into a
receive whose timeout is an ordinary message clause on the virtual clock.

**It's on by default.** `after` is the one real-time dependence a system can
hold without naming a function, so leaving it to an attribute would mean a
module that quietly wakes on the wall clock with nothing reporting it.

`-eta_after(false).` opts a module out, for a hot receive loop where the cost of
arming and cancelling a timer per receive is measurable. `after 0` is never
rewritten, since it's a mailbox poll rather than a wait.

### timer:sleep/1 is not rewritten, deliberately

A sleep is `receive after T -> ok end` inside OTP's `timer` module, which no
transform of your code reaches. The sleeping process is blocked on a timeout
rather than on a message, and `eta_sched` decides what's runnable by looking at
mailboxes, so it sees a process with nothing waiting for it and never steps it.
Only the real clock can end that wait, and the driver doesn't control the real
clock. Code under simulation shouldn't sleep at all, and quietly virtualizing
the call would hide that rather than surface it.

### A system with periodic timers never becomes quiet

Heartbeats mean "nothing runnable and nothing pending" is a state you never
reach. Without a bounded settle phase the only way such a run can end is the
step budget, which is reported as an error, so every run looks like a failure.
That's what `settle_steps` is for, and the phase isn't dead time. It's the only
part of a run where the invariants are checked with no client traffic at all.

### A timer outlives the scheduler's ownership of its process

A timer belongs to a process, and the scheduler is not obliged to own that
process. When it doesn't, the timer is still in the wheel, and a driver that
advances to the earliest deadline moves the clock on behalf of something that can
never take a step. 2 runs of one seed then differ by an inserted `{clock, Ms}`
with no step behind it, hundreds of entries in.

2 ordinary ways to get one. A process killed while blocked in a rewritten
`receive ... after` never reaches the clause head that disarms it, so its
deadline outlives it. And a process spawned before the scheduler existed, which
means anything your system starts during `init/2`, is outside the schedule by
construction.

`eta` steps over such deadlines rather than advancing to them, so this no longer
changes a schedule, and reports the count as `stray_timers` in the run result.
`audit/1` checks it. A non-zero count still means something worth fixing: a
process holding a timer that nothing will ever schedule. Either hand it to
`processes/1` so it becomes part of the run, or don't start it in `init/2`.

### Timeouts outside the transform boundary are on the real clock

If you deliberately leave a client-facing module untransformed, its
`gen_server:call` timeouts are real. Under the scheduler a client is suspended
between steps, so when its call times out is decided by wall clock. The usual
workaround is to configure the timeout high enough that it can't fire during a
run, which removes the behaviour it guards from the set of things you're
testing.

A **transformed** module's calls are virtual whenever a clock is running,
whether or not you asked for a network. That used to depend on the network, so
the ordinary configuration — scheduler and clock up, `net => false` — waited out
a real 5 second `after` inside OTP on every call. If you have a run from before
that was mysteriously sensitive to machine load, this was why.

### A call from the driver still can't have a virtual timeout

The exception to the above, and it follows from how the clock moves. `eta_run`
advances only to deadlines belonging to a process it can step, and steps over
everything else as a stray. A virtual deadline armed by the driver is therefore
one nothing ever reaches: the driver is blocked waiting for a timeout that only
the driver can deliver.

So a call made from `execute/2` or `check/1` keeps its real timeout, which is
the same rule as before by a different route — those callbacks must not call
into a scheduled process, and `eta_harness` says why. What changed is only that
getting it wrong still ends in a 5 second exit rather than in a run with no way
to finish.

### What a virtual call timeout means is not what a real one means

Worth internalizing before reading a counterexample. The clock only moves when
nothing is runnable, so a virtual call timeout fires exactly when the system
reached a state where nothing could make progress and this was the earliest
deadline. That's a much stronger statement than production's, where a timeout
mostly means somebody was slow — and it's a good lost-message and deadlock
detector for it.

The flip side is that **"the callee was slow" doesn't exist under a virtual
clock**, because computation costs no virtual time. The race where a caller
gives up and the answer arrives just afterwards is reachable only through a
timer or a network delay, never through the callee taking too long. If that race
is what you're hunting, you need `skew_ms` or a lossy network policy; a timeout
on its own won't find it.

For the same reason, the *numbers* matter more than they do in production.
Deadlines are what the clock jumps between, so a timeout value orders events
against every heartbeat, lease and retry in the system. Every default is 5000,
so they land on one deadline and fire in creation order, the same order every
run. `skew_ms` is what breaks the tie, and it reaches receive and call deadlines
as well as the timers your system sets itself.

`drop_p` deliberately does *not* reach them. A timer your system sets to fire at
another process can go missing with that process; a deadline a process arms for
itself cannot, and dropping one produces a hang wearing the shape of a finding.

## Observability hazards

### An invariant that asks is an invariant that checks nothing

Covered on the [previous page](04-writing-a-system-under-test.md), and repeated
here because it's the failure that doesn't announce itself. A client API that
catches its own call timeout returns a plausible answer for a suspended process,
and the invariant computes over that answer and passes. `eta_run` bounds
`check/1` and reports `check_blocked` when it hangs. It can't detect the case
where the API answers.

### "It times out" is a shape, not a symptom

When something hangs, measure how long it hangs for. A stall costing *exactly*
the timeout is a lost wakeup: a message that never arrived, a watch that never
fired. A variable one is contention. Some call timeouts against the in-memory
backend here were diagnosed as a retry livelock and were nothing of the sort.
They were a watch anchored to the wrong version, which looks nothing like
contention once somebody has looked at the distribution.

### If you build a deterministic substitute, compare it differentially

An in-memory replacement for a database is a large piece of code that has to
behave exactly like the thing it replaces, and a substitute that's subtly wrong
is worse than none, because every finding it produces is suspect. Every bug we
found in this project's in-memory backend came from running the same operations
against both implementations and diffing the results, not from reading the code.

Version equality is where those bugs cluster. "Did this key change after my read
version?" and "should this watch fire?" are both questions about a boundary, and
a commit landing exactly on the boundary is the case that gets written wrong: a
conflict that isn't detected, a watch that never fires.

## Workload and trace hazards

### Operations must be stable across runs

An operation carrying a generated name, a pid or a ref makes the trace
incomparable between runs and unreplayable against a fresh system. The symptom
is a trace difference at a very early index, which reads exactly like a
scheduling divergence.

### Every action the driver takes has to be in the trace

`eta_run` records `{step, Id}` and `{op, Op}`. For a while it didn't record its
third action, advancing the clock, on the reasoning that the clock is a function
of the timers and the timers are a function of the schedule, so a replay would
advance in the same places by itself.

It doesn't, because `replay/3` walks only the entries it's given. A trace from a
system with timers replayed against a clock that never moved. Nothing became
runnable, every recorded step was refused, and a strict replay failed at entry 0.
`eta_shrink` reported "nothing to shrink" for a failure that had just happened,
because it replays the trace to classify it and got a clean run back.

`{clock, Ms}` is an entry type now. What's transferable is why it survived so
long. `replay/3` had only ever been exercised against a system with no timers,
while the timer-driven system's "replay works" measurement re-ran the *seed*,
which exercises `run/2`. 2 different claims that both sound like "replay works",
and only one of them was tested.

### A seed-pinned regression test is coupled to your generator

A seed names a schedule **only in the context of a particular `generate/2`.**
Change the operation mix and every seed you pinned quietly starts testing
something else.

If the test asserts on a violation you find out, because it fails. If it asserts
`ok` it goes vacuous without a word, which is the same silent failure as an
invariant that never gets evaluated.

Pin the trace instead. `replay/3` never calls `generate/2`, it walks the entries
it's given, so a saved trace survives a rewritten workload entirely.

```erlang
%% once, from a shrink whose `verified` was true
{ok, _Outcome} = eta_run:save_fixture("test/fixtures/atomicity.eta",
                                      my_harness, Trace, Opts),

%% in the test, forever after
#{outcome := {violation, #{property := atomicity}}} =
    eta_run:replay_fixture("test/fixtures/atomicity.eta").
```

`save_fixture/4` replays the trace strictly before writing anything and refuses
on a divergence, so a fixture on disk is one that has been demonstrated to
reproduce. It stores the harness and the options alongside the trace, which
matters as much as the trace does: a reproduction that needs
`config => #{mode => broken}` and gets replayed without it does not reproduce.

**Pin a seed to test that seeds reproduce. Pin a trace to regress a bug.**

You can't write the mirror test. Replaying a failing trace against a fixed
implementation does not report `ok`, because the fix changes which messages
exist, so recorded step ids stop being runnable and you get
`{error, {diverged, _, _}}`. "The fix works" is a claim about the whole system,
and a seed sweep is the right shape for it.

## Fault injection hazards

### A seeded fault schedule starts where its seed is applied

The symptom is specific enough to recognize. A seed reproduces exactly right up
to the first injected fault, and never after.

A simulated network draws from its seeded generator once per message the policy
applies to. If the policy is installed after the cluster is up, which it usually
has to be so that no member is partitioned away before it has ever synced, then
the cluster's own startup traffic has already consumed draws. Startup runs on
the real scheduler and varies in both order and message count, so the fault
schedule begins at an offset that differs on every run.

Anything that consumes the generator before the run begins silently offsets the
schedule. Reseed when the run starts, and treat "the fault schedule is a
function of the seed" as something to assert rather than assume.

### Only inject faults the system can actually see

Erlang guarantees that messages between an ordered pair of processes arrive in
send order, and doesn't guarantee delivery at all. Dropping messages is
therefore a faithful model of a failing link, and reordering them within a pair
isn't. Inject reordering and every bug you find is a false positive.

The subtler version is that dropping a message models a link failure, and a real
link failure is never *just* lost messages. It comes with `nodedown` and `nodeup`
on both sides, and systems hang their recovery off those signals. Drop messages
without delivering the node event and you've injected something no real network
produces: messages vanishing while both ends still believe the link is up. The
unrecovered state you then report isn't a defect.

There's a trap on the other side of that. If you do deliver the node event,
recovery may repair the very divergence you were trying to observe. In the
registry, a rejoin makes the leader send that member a fresh snapshot. A run
that asserts a property *during* a fault has to restrict the loss to a channel
whose recovery is self-contained.

### Faults are not free, and the currency is the other faults

Adding a second fault costs you search efficiency on the first. Measured on a
quorum register: a workload that reproduced its write-back bug in **21 of 200
seeds** dropped to **12 of 200** once a replica-pause fault was added, with the
first failing seed moving from 6 to 53. Operations that time out never complete,
and only completed operations can violate a safety property.

The sharper version of the same problem, seen before retuning that pause rate:
the crashed-writer fault that made the bug reachable **disappeared from the event
log entirely.** Not one completed. The new fault had crowded out the old one.

The rule above is about injecting faults the system could never see. This is the
opposite failure and it's just as quiet: inject too many and the suite stays
green because nothing interesting ever completes.

### A workload happens at virtual time zero

Worth knowing before you inject anything with a *duration*, because the natural
mental model is wrong.

The virtual clock only advances when nothing is runnable, and the driver is busy
injecting and stepping throughout, so **an entire `max_ops` workload is
typically injected while the clock still reads 0.** A fault lasting about as
long as a run is therefore either active for all of it or none of it.

The consequence is that such a fault tunes bimodally rather than smoothly. 10
seeds of a 3-replica register with a 10-second pause gave give-up counts of
`0, 0, 0, 0, 0, 0, 9, 9, 11, 12`. Lowering the rate makes jams rarer. It can't
make them milder.

## Shrinking hazards

### Step ids are positional, which bounds what can be removed

The scheduler assigns ids in registration order, so deleting an operation
renumbers every process created after it and the surviving `{step, Id}` entries
stop naming what they named.

How much that costs depends on how many processes your system creates. Against
two-phase commit, delta debugging goes from 26 entries to 8. Against the
registry, it removes between 0 and 3 entries out of 70 to 85, and never removes
an operation at all.

So on a process-heavy system, what makes a reproduction readable is a smaller
*workload* rather than a better shrinker. Search over `max_ops` as well as over
seeds. A registry defect that produces an 80-entry trace under 12 operations
still reproduces under 3, and that trace is 38 entries.

### Give your violations a property key

`eta_shrink`'s default test for "the same failure" is the violation's `property`
key when there is one, and any violation at all when there isn't. With more than
one invariant, the fallback will cheerfully shrink one bug into a different one,
which is worse than no shrink because it looks like progress.

## Teardown hazards

ETS tables are owned by the process that created them, so an `on_exit` cleanup
races the VM's own, and a cluster linked to the test process is already dead by
the time it runs. Kill client processes before deleting tables they write to. By
teardown the scheduler has been released, so anything still in flight is running
again, and a client reaching its `ets:insert` after the table has gone crashes
with a `badarg` that has nothing to do with the run.

And don't edit source while background test runs are in flight.

## One closing rule

2 clean runs isn't evidence for an intermittent fix. What is evidence is a test
that fails deterministically before the fix and passes after it. If you can't
produce one, the diagnosis isn't finished.

## Next

[A Journey Through DST](06-a-journey-through-dst.md) is the whole thing end to
end: an empty directory, a quorum register, a planted bug, and a minimized
replayable failure that explains itself.
