# Gotchas and footguns

Grouped by where they bite. If you're reading this because a seed stopped
reproducing, start with the method rather than the list.

## How to debug a determinism failure

Reading the code and thinking hard has a poor record against "the same seed
produced 2 different runs". 3 techniques work better.

**Look at which runs differ, not how many.** Run a seed 5 times and group the
identical traces. A grouping of `[[1], [2,3,4,5]]` says run 1 is special, which
points at warm-up or first-touch state. `[[1,3], [2], [4,5]]` says the
divergence is a coin flip on every run. The 2 have completely different causes,
and the distinction costs nothing to measure.

**Vary the position rather than the seed.** If seed 3 is the one that fails,
run your seeds in reverse order. If the failure follows whichever seed is now
first, the seed was never the variable and you're looking at the first-run
problem below.

**Snapshot everything at the point of divergence and diff it.** State, mailbox
contents, process status, the timer wheel. Twice here the difference has been
invisible in state and obvious in mailboxes. When even mailboxes matched, what
worked was logging scheduler events against step boundaries: which process was
scheduled in and out, and what it was executing at the time.

Bisecting what you compare helps too. Removing the client workload entirely and
finding the system still nondeterministic, or suddenly not, narrows the search
for very little effort.

## The code server

The symptom is that the first run in a fresh VM produces a different trace from
every subsequent run of the same seed. Nothing is adopted late, no scheduler
timeout fires, no warning is logged, and the scheduler's accounting is correct
throughout.

Loading a module on demand is a synchronous call to `code_server`, which the
scheduler doesn't own. A scheduled process that reaches a module it hasn't
touched yet blocks on something outside the schedule. The scheduler reads that
as the step ending, the code server runs freely and makes the process runnable
again at a moment decided by wall clock, and every choice after that point
shifts. On the second run everything is loaded and it never happens again.

Load everything before the scheduler owns anything, at the top of `init/2`:

```elixir
defp preload do
  for app <- [:my_app, :stdlib, :kernel] do
    _ = Application.load(app)
    :code.ensure_modules_loaded(Application.spec(app, :modules) || [])
  end
end
```

The obligation belongs to `init/2` for the same reason quiescence does. Its
contract isn't "start the system". It's "hand over a system that won't do
anything the schedule didn't ask for".

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

For anyone without the optional dependency that's a synchronous code-server
call on every event, forever. Cache the answer in `persistent_term`.

Here's the general rule to carry to your own project. Under a user-level
scheduler, any synchronous call into an OTP service process is a real-time
dependence. `code_server`, `logger`'s handlers, `global`, `net_kernel`,
`application_controller`. Grepping for `code:ensure_loaded`, `code:is_loaded`,
`logger:` and `global:` along the paths your system exercises is much cheaper
than the instrumentation it takes to find these afterwards.

## Build-time hazards

### Never give a module 2 parse transforms

Mix doesn't reliably order a module that names more than one. With 2 transform
attributes on a single module, an incremental build in which both the transform
and the module had changed failed with `undefined parse transform` about 2
times in 3. It's nondeterministic, so it reads as flakiness rather than as a
build problem.

Delegating from one transform to another doesn't help, because the compiler
loads a transform module from the code path when it runs, so the ordering
hazard just moves down a level. If a module needs a second pass, add it to the
transform it already names and gate it behind an attribute. That's why
`dst_transform` carries the state-publishing pass, and why modules opt into it
with `-dst_observe(...)`.

It's also why `dst_after_transform`, which puts `receive ... after` timeouts on
the virtual clock, isn't currently applied to anything. The modules that would
want it already carry `dst_transform`, and the 2 can't coexist. Merging the
passes is the way out, and we haven't needed it yet.

### Mix doesn't recompile a module when its transform changes

Edit a parse transform, run the tests, and the tests exercise yesterday's
transform. Everything passes. `mix compile` rebuilds the transform module and
leaves every module built with it stale.

The same applies to compiler options. Toggling a `-D` define doesn't reliably
rebuild the Erlang modules that depend on it, which matters if you use a define
to plant a defect deliberately. Run `mix compile --force` after changing
either. Where a test's whole purpose is to exercise a transform, recompile its
subject from source in `setup_all` so it can't exercise the stale one.

## Scheduler hazards

### A plain spawn is a determinism hole

The driver isn't traced, so a process it creates isn't adopted until the driver
registers it, and in that window the process runs on the real scheduler. One
racing process is usually harmless. 2, created by operations injected close
together, race each other to deliver their first message.

Inside a transformed module `spawn` is rewritten to `dst_sched:spawn/1`
automatically. Inside `execute/2` it isn't, because your harness isn't
transformed, so use `dst_run:spawn_op/1` there. Watch `stats.adopted_late` in
the run result. It should be at or very near zero.

### suspend_process/1 is documented for debuggers

That's what the scheduler is built on. It's clean at 3 processes and has been
fine at a few hundred, but behavior at 1000s, with supervisors and monitors in
play, is unverified. Satisfy yourself about it before building on top for a
very large system.

### One clock per VM

`dst_time` keeps its state in named ETS tables, so exactly one virtual clock
exists per node and 2 simulations running at once will corrupt each other. Keep
the tests `async: false`.

## Time hazards

### Start the clock before the system

`dst_run` does this for you, which is why `init/2` is called by the driver
rather than by you beforehand. A timer armed while `dst_time` is inert goes
onto the real clock and stays there until it fires and re-arms, so long-period
timers, the interesting ones, would silently never become virtual.

### receive ... after is not a timer call

`dst_transform` rewrites calls, and `after` is a language construct rather than
a call, so a module compiled with it still has real-clock receive timeouts.
Without `dst_after_transform`, a system under simulation has to avoid
real-time-dependent receives. Use `infinity` where you can.

### timer:sleep/1 is not rewritten, deliberately

A sleep is `receive after T -> ok end` inside OTP's `timer` module, which no
transform of your code reaches. The sleeping process is blocked on a timeout
rather than on a message, and `dst_sched` decides what's runnable by looking at
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

### Timeouts outside the transform boundary are on the real clock

If you deliberately leave a client-facing module untransformed, its
`gen_server:call` timeouts are real. Under the scheduler a client is suspended
between steps, so when its call times out is decided by wall clock. The usual
workaround is to configure the timeout high enough that it can't fire during a
run, which removes the behavior it guards from the set of things you're
testing.

## Observability hazards

### An invariant that asks is an invariant that checks nothing

Covered on the [previous page](04-writing-a-system-under-test.md), and repeated
here because it's the failure that doesn't announce itself. A client API that
catches its own call timeout returns a plausible answer for a suspended
process, and the invariant computes over that answer and passes. `dst_run`
bounds `check/1` and reports `check_blocked` when it hangs. It can't detect the
case where the API answers.

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
against both implementations and diffing the results, not from reading the
code.

Version equality is where those bugs cluster. "Did this key change after my
read version?" and "should this watch fire?" are both questions about a
boundary, and a commit landing exactly on the boundary is the case that gets
written wrong: a conflict that isn't detected, a watch that never fires.

## Workload and trace hazards

### Operations must be stable across runs

An operation carrying a generated name, a pid or a ref makes the trace
incomparable between runs and unreplayable against a fresh system. The symptom
is a trace difference at a very early index, which reads exactly like a
scheduling divergence.

### Every action the driver takes has to be in the trace

`dst_run` records `{step, Id}` and `{op, Op}`. For a while it didn't record its
third action, advancing the clock, on the reasoning that the clock is a
function of the timers and the timers are a function of the schedule, so a
replay would advance in the same places by itself.

It doesn't, because `replay/3` walks only the entries it's given. A trace from
a system with timers replayed against a clock that never moved. Nothing became
runnable, every recorded step was refused, and a strict replay failed at entry
0. `dst_shrink` reported "nothing to shrink" for a failure that had just
happened, because it replays the trace to classify it and got a clean run back.

`{clock, Ms}` is an entry type now. What's transferable is why it survived so
long. `replay/3` had only ever been exercised against a system with no timers,
while the timer-driven system's "replay works" measurement re-ran the *seed*,
which exercises `run/2`. 2 different claims that both sound like "replay
works", and only one of them was tested.

## Fault injection hazards

### A seeded fault schedule starts where its seed is applied

The symptom is specific enough to recognize. A seed reproduces exactly right up
to the first injected fault, and never after.

A simulated network draws from its seeded generator once per message the policy
applies to. If the policy is installed after the cluster is up, which it
usually has to be so that no member is partitioned away before it has ever
synced, then the cluster's own startup traffic has already consumed draws.
Startup runs on the real scheduler and varies in both order and message count,
so the fault schedule begins at an offset that differs on every run.

Anything that consumes the generator before the run begins silently offsets the
schedule. Reseed when the run starts, and treat "the fault schedule is a
function of the seed" as something to assert rather than assume.

### Only inject faults the system can actually see

Erlang guarantees that messages between an ordered pair of processes arrive in
send order, and doesn't guarantee delivery at all. Dropping messages is
therefore a faithful model of a failing link, and reordering them within a pair
isn't. Inject reordering and every bug you find is a false positive.

The subtler version is that dropping a message models a link failure, and a
real link failure is never *just* lost messages. It comes with `nodedown` and
`nodeup` on both sides, and systems hang their recovery off those signals. Drop
messages without delivering the node event and you've injected something no
real network produces: messages vanishing while both ends still believe the
link is up. The unrecovered state you then report isn't a defect.

There's a trap on the other side of that. If you do deliver the node event,
recovery may repair the very divergence you were trying to observe. In the
registry, a rejoin makes the leader send that member a fresh snapshot. A run
that asserts a property *during* a fault has to restrict the loss to a channel
whose recovery is self-contained.

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

`dst_shrink`'s default test for "the same failure" is the violation's
`property` key when there is one, and any violation at all when there isn't.
With more than one invariant, the fallback will cheerfully shrink one bug into
a different one, which is worse than no shrink because it looks like progress.

## Teardown hazards

ETS tables are owned by the process that created them, so an `on_exit` cleanup
races the VM's own, and a cluster linked to the test process is already dead by
the time it runs. Kill client processes before deleting tables they write to.
By teardown the scheduler has been released, so anything still in flight is
running again, and a client reaching its `ets:insert` after the table has gone
crashes with a `badarg` that has nothing to do with the run.

And don't edit source while background test runs are in flight.

## One closing rule

2 clean runs isn't evidence for an intermittent fix. What is evidence is a test
that fails deterministically before the fix and passes after it. If you can't
produce one, the diagnosis isn't finished.
