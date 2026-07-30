# Writing a system under test

Two-phase commit is small enough that every callback is obvious. A real system
isn't, and most of the work of adopting DST isn't writing the harness. It's
making the system observable and getting the invariants right.

The examples below come from `dgen_registry`: several nodes each holding a
replica of a `name -> pid` map, with one elected leader through which all
changes pass. Where a detail of that system matters we explain it where it
appears.

## The contract

```erlang
-callback init(Seed :: integer(), Config :: map()) -> {ok, sut()}.
-callback processes(sut()) -> [pid()].
-callback generate(sut(), rand:state()) -> {op(), rand:state()}.
-callback execute(op(), sut()) -> sut().
-callback check(sut()) -> ok | {violation, violation()}.
-callback terminate(sut()) -> ok.
```

`sut()` is whatever you want it to be. A map is conventional.

## init/2

The system has to be handed over idle. Whatever is still in flight when the
driver takes control ran on the real scheduler, and its effects land at a point
in the schedule that nothing chose.

That's a stronger condition than the one most systems already offer. A
cluster's startup helper typically waits until every node reports itself ready,
meaning a leader elected and an initial sync completed. That's the right
definition for a client and the wrong one here. Ready means the system can
accept work. Idle means it isn't currently doing any. A member can be ready
with an unanswered call still sitting in its mailbox, and about 1 start in 5 of
the registry was.

What that costs you is a run whose first few steps are decided by wall clock,
and it's unusually hard to diagnose from state alone. Leader, epoch, applied
version and even the whole timer wheel can be identical between 2 runs that
then diverge. The difference shows up in mailboxes and process status, which is
what the check has to look at:

```elixir
defp await_quiescent(cluster, attempts) do
  busy =
    for pid <- processes_to_check(cluster),
        {:status, st} = Process.info(pid, :status),
        {:message_queue_len, n} = Process.info(pid, :message_queue_len),
        st != :waiting or n > 0,
        do: pid

  if busy == [] do
    :ok
  else
    Process.sleep(1)
    await_quiescent(cluster, attempts - 1)
  end
end
```

Polling on the real clock is correct here and only here, because this runs
before the scheduler exists. It's the one place in a run where wall clock is
allowed to matter.

Include everything that holds messages on the system's behalf, not just its
supervision tree. A simulated network process sitting on an undelivered startup
cast isn't quiescence either. Which processes have something waiting for them
when the scheduler takes over *is* the initial state of the run.

## processes/1

The scheduler assigns ids in registration order and the trace records ids, so
the order this callback returns is part of the contract. 2 runs that register
processes in different orders aren't comparable, and the resulting early trace
difference reads as a scheduling divergence.

```elixir
def processes(%{cluster: cluster, clients: clients}) do
  tree =
    for m <- Enum.sort_by(Map.values(cluster.members), & &1.index),
        {_id, pid, _type, _mods} <- Supervisor.which_children(m.sup),
        is_pid(pid),
        do: pid

  [cluster.net | tree] ++ Enum.filter(clients, &Process.alive?/1)
end
```

The `Enum.sort_by(..., & &1.index)` is load-bearing. That map is keyed by
member names which embed a unique integer, so iterating it directly orders by
atom, and the atoms differ between runs.

Leave out processes that can never do anything. This workload registers names
against dummy processes that block forever, and scheduling those would only add
choices that can't matter.

## generate/2

An operation is recorded in the trace and may be replayed later against a
freshly started system, so it has to mean the same thing in both. Anything
generated at runtime fails that test:

```elixir
# Wrong: member names embed a unique integer, so on the next run this names a
# different process, or none at all.
{:register, :sim_2210_m3, :worker_1}

# Right: name the member by index and resolve it when the operation executes.
{:register, 3, :worker_1}
```

The same applies to pids, refs, generated atoms, and anything ordered by a map
keyed on one of those. If an operation has to name a process, name its position
and resolve it in `execute/2`.

## execute/2

Covered on the [previous page](03-two-phase-commit.md). The short version:
every process the scheduler owns is suspended, so a synchronous call into one
from the driver is never answered. Operations are issued by spawning, and the
spawn goes through `dst_run:spawn_op/1` so the new process can't act before the
scheduler owns it.

## check/1

An invariant runs against a frozen system, so anything that sends a message and
waits can't be served.

The loud version of getting this wrong is a hang, which `dst_run` bounds and
reports as `check_blocked`. The quiet version is worse and nothing can catch
it. Consider a status API written the way most of them are:

```erlang
status(Name) ->
    try gen_server:call(Name, status, 500) of
        S -> S
    catch
        exit:_ -> undefined
    end.
```

Under the scheduler every call to that returns `undefined`, because the process
it asks is suspended and always will be. An invariant built on it, say "no 2
members believe they are the leader at the same epoch", computes over an empty
set and passes. The suite stays green and the invariant has been switched off
without anybody touching it.

So invariants read state out of band. There are 2 ways to arrange that, and the
choice comes up on every system.

If the state already lives somewhere readable, read it there. The registry's
members keep their replicas in ETS because the replication protocol needs them
there anyway, so the invariant that compares replicas is an `ets:tab2list` per
member.

If the state exists only inside a process, publish it.
`-dst_observe([leader, epoch])` makes the transform republish those fields into
the process dictionary on every callback return, and `dst_observe:read/1` reads
them from outside. That works on a suspended process, in a couple of
microseconds, whatever the mailbox depth. Publishing on every return is what
makes staleness impossible, since there's no assignment site anybody can
forget. Name the fields you need rather than using `-dst_observe(all)`, because
`read/1` copies what was published and gets called after every step.

## Deciding when a property holds

Getting this wrong produces either false alarms or a suite that passes without
checking anything. Properties fall into 3 tiers, and a real system has some in
each.

**Always true.** Assert after every step. In the registry, 2 members may both
believe they lead only under different epochs. That's a safety property of the
fencing token, and there's no instant at which it may be false.

**True at rest.** These need a definition of "rest". The registry's example is
that 2 members reporting the same version must hold the same map. That isn't
true continuously, and not because of a bug. Members write optimistically,
ahead of the replicated stream, so a member legitimately holds different
content from its peers at the same version while an operation is in flight.

The scheduler can answer whether the system is at rest, since it's the thing
that knows:

```elixir
def quiescent?(%{clients: clients}) do
  case :dst_sched.current() do
    :undefined -> false
    sched -> :dst_sched.runnable(sched) == [] and not Enum.any?(clients, &Process.alive?/1)
  end
end
```

Both halves of that condition are necessary. "Nothing runnable" alone isn't
quiescence, because a system can have nothing runnable in the middle of an
operation. A commit parked on a worker leaves every other process idle while
the write it will confirm is still speculative. The window a speculative write
lives in is bounded by its operation, not by the scheduler going quiet. Stated
generally: a property that only holds at rest needs the system's own definition
of rest, and "no process wants to run" isn't it.

**True after convergence.** Heal every injected fault, drain the network, wait
for agreement, then assert. Some properties genuinely need this, but healing
can also destroy what you were trying to observe. In the registry, converging
delivers a `nodeup` to every member, each re-announces itself, and a
re-announcement makes the leader send that member a fresh snapshot, repairing
exactly the inconsistency under investigation. A permanent, undetectable
divergence is only visible in the window between the system going quiet and
anything being repaired, which is the middle tier.

## Proving the check ran

An invariant gated on quiescence that never reaches a quiescent state passes by
never being evaluated, and the run reports `ok` either way. Count the
evaluations and assert the count in a test:

```elixir
defp check_quiescent(cluster) do
  bump(:quiescent_checks)
  Invariants.check_quiescent(cluster)
end
```

`check/1` runs in a fresh process per check, so the counter can't live in the
system-under-test state. Use a public ETS table or a counter ref.

The same applies to workloads. A test asserting that a batch is applied
atomically should also assert that a batch of more than one operation actually
occurred, or a change in batching behavior will turn it into a tautology that
keeps passing.

## Assert the property, not the schedule

2 rules that look contradictory and aren't.

The first is not to weaken a failing test without proving the race is real. A
test in this project failed intermittently and the tempting fix was to make the
assertion wait for the value it wanted. That would have been wrong. Both
operations rode the same durable queue, and FIFO ordering guarantees the read
observes the earlier write. The failure was a real bug in the storage layer,
and weakening the test would have hidden it and left a weaker test behind.

The second is to fix a test that asserts the schedule instead of the property.
Another test flipped 150 registrations at once and asserted that a sampler only
ever observed 0 or 150 of them, which pinned it to a batch boundary the test
doesn't control. When the leader split the work 1-then-149 instead, both
batches were applied perfectly whole and the test reported 1000s of torn reads.
It now derives the legal counts at runtime from the batches the follower
actually received, which holds under any split and rejects a genuinely torn
read under all of them.

The distinction is whether the thing being asserted is guaranteed by the system
or merely produced by it most of the time.

## Check that your tests can fail

2 clean runs isn't evidence that an intermittent bug is fixed. What is evidence
is a test that fails deterministically before the fix and passes after it.

The stronger form is to break something on purpose and confirm the test
notices. `dst`'s own acceptance criteria are that idea at a larger scale: plant
a known defect behind a compile flag and require the framework to find it again
from a cold start.

## Next

[Gotchas and footguns](05-gotchas.md).
