# What deterministic simulation testing is

Deterministic simulation testing is a way of finding the bugs you would otherwise
only understand in hindsight: the ones that need a particular interleaving of
concurrent events, that surface once a month in production with no reproduction, and
that you can only write a regression test for after you already know what they are.

The approach is to take control of everything in a running system that is not
determined by its own logic — which process runs next, when a timer fires, what the
clock reads — and drive all of it from a single seeded random number generator. Do
that thoroughly enough and a seed stops being a detail of the test setup and becomes
the name of an entire execution. The same seed produces the same interleaving on
every run, so a failure you saw once you can see again, on demand, with a debugger
attached.

This page covers what the technique gives you and what it costs, so that you can
decide whether it is worth doing. The rest of the walkthrough covers how.

## A bug that needed hindsight

This framework grew up alongside a distributed process registry, and one particular
defect in that registry is a good illustration of the kind of thing being hunted.
You do not need to know the registry to follow it. Here is enough of the design.

Several nodes each run a member process holding a local copy of a `name → pid` map.
One member is the leader, and every change goes through it. The leader applies
changes in batches, stamps each batch with a version number, and broadcasts the batch
to the other members, who apply it and record the version they have reached. A member
that misses a broadcast finds out on the next one, because a batch carries the version
of its predecessor and that will not match what the member holds; it stops applying
and asks the leader for a fresh copy of the whole map. So a member's version number
is a promise about its contents. Two members reporting the same version hold the same
map, and the reconstruction that runs when leadership moves relies on exactly that
when it picks the most up-to-date replica it can find.

Registrations that arrive at a follower rather than the leader are forwarded, and the
follower learns the verdict from a reply the leader sends after the broadcast.

Now the bug. Suppose one client registers `worker_3` while another unregisters it,
and the two operations land in the same batch. That is a legitimate ordering, and
every member handles it correctly: the broadcast carries both operations in order, so
each member applies the registration and then the removal and ends with the name
unbound. The leader agrees.

Then the follower that forwarded the registration receives the reply, which says
`yes`, and its handler writes the binding into the local map. In isolation that looks
entirely reasonable, since the reply means the registration committed and the row
ought to be there. But the batch has already gone past and taken the row out again,
so the member is left holding a name no other member holds, at exactly the same
version number as everyone else.

Nothing in the protocol will notice, because the mechanism that catches a member
falling behind compares versions and the versions agree. The divergence is permanent,
and if leadership moves the new leader may pick that replica as the freshest and copy
it to everyone.

To write a test for that by hand you would need two client operations on the same
name, arriving close enough together to share a batch, ordered registration first,
with the registration forwarded from a follower rather than issued at the leader, and
the reply processed after the broadcast rather than before. You can write that test,
but only once you already know the answer, which is not much help.

Under simulation it took a sweep of 200 seeds and about forty seconds to surface.
Nine of the two hundred failed, and no faults were injected at all: no dropped
messages, no crashes, nothing but ordinary scheduling.

## How the technique works

None of this is new. FoundationDB built its storage engine this way and described the
approach publicly; Antithesis and TigerBeetle have since made it much more widely
known. The mechanism is easier to state than to build:

> Take control of every source of nondeterminism in the system, drive all of them
> from one seeded random number generator, and a seed becomes a complete, replayable
> execution.

On the BEAM there are three things to bring under control.

**Process scheduling.** The VM's own scheduler decides which of your runnable
processes gets the CPU, and that decision is not reproducible. `dst_sched` takes it
over: it suspends every process it owns, then resumes exactly one at a time and lets
it run until it blocks. Which process runs next comes from the seeded generator.

**Time.** Timers fire on the wall clock, so a run that depends on a five-second
timeout is neither reproducible nor quick. `dst_time` provides a virtual clock and
timer wheel, and a parse transform points a module's `erlang:send_after/3` and
friends at it. The clock only moves when the driver moves it, which it does when
nothing is runnable, jumping straight to the next deadline.

**Anything outside the VM.** Sockets, ports, NIFs, a database. These are your
problem, and usually the largest single cost of adopting the technique. The registry
above needed an in-memory implementation of its storage backend before any of the
rest of this was possible.

Message ordering is conspicuously missing from that list, and it is worth saying why,
because the obvious design for a deterministic scheduler intercepts every message and
controls delivery order. That turns out to be unnecessary on the BEAM. Erlang already
guarantees that messages between an ordered pair of processes arrive in send order,
so once only one process runs at a time, the global order in which messages are
delivered is fixed by the order in which senders were stepped. Controlling who runs
is enough to determinise the whole system, and that is what makes the approach
practical without rewriting the code under test.

## What it buys you

The obvious benefit is reproduction. When a run fails, the seed reproduces it — not
usually, not most of the time, but exactly, because the same process makes the same
choice at the same point on every run. You can add a print statement, attach a
debugger, or go home and come back tomorrow, and the failure will still be there.

Less obvious, and about equally valuable, is that failures can be made small. A run
that fails after two thousand scheduling decisions is not something anybody can read.
Because the framework records the sequence of decisions it made, it can replay
altered versions of that sequence and check whether the failure survives, which turns
minimisation into a search. `dst_shrink` does that with delta debugging. Against the
two-phase commit example on [page 3](03-two-phase-commit.md) it reduces a
twenty-six-entry trace to eight in about twenty-four milliseconds, and what comes out
is close to a proof sketch of the defect: the transaction carrying the dissenting
vote, followed by the six scheduling decisions that let a `yes` reach the coordinator
first.

Virtual time changes the economics more than it might appear. A thirty-second
timeout costs nothing, because when no process is runnable the driver simply moves
the clock to the next deadline. A registry run in this project simulates roughly 280
seconds of cluster life in about 200 milliseconds of wall clock, and that ratio is
the difference between a two-hundred-seed sweep being a coffee break and being an
overnight job.

Finally, a scheduler that decides when *not* to give the system work makes a whole
class of defect reachable. Some bugs only appear when nothing is happening — a
replica that lost the last message before traffic stopped, with nothing left arriving
to reveal the gap. Ordinary tests never sit still long enough to see that. Here,
letting time pass instead of injecting the next operation is a deliberate choice the
driver makes with a tunable probability.

## What it does not do

It is not exhaustive. A model checker such as TLC explores every interleaving of an
abstraction of your protocol; simulation testing samples interleavings of the real
code. The two answer different questions. TLC can tell you a protocol is correct and
cannot see your implementation of it; simulation can find bugs in the implementation
and can never prove there are none left. The registry discussed above is covered by
both, and they have caught disjoint problems: the formal model has no unregister
operation at all, which is precisely why it could not have found the bug described
earlier.

It is not cheap. Getting to the first bug here meant a scheduler, a virtual clock, a
parse transform, an in-memory replacement for the database, and a harness per system
under test. There is a useful stopping point well short of that — a scheduler,
virtual time and a thin driver gets you most of the value. Rigorously solving all
nondeterminism buys you perfect reproducibilty, but the price may be high.

It does not replace property-based testing. Property-based testing samples inputs;
simulation testing controls interleavings. They compose well, and they find different
things.

And it does not tolerate a single leak. One `receive ... after` on the real clock,
one synchronous call into an OTP service process, one plain `spawn`, and
reproducibility stops being true. It stops silently, too: nothing fails, and the
seeds simply stop meaning anything. Most of [page 5](05-gotchas.md) is about
finding those leaks.

## Next

[Setting up a project](02-setting-up.md) covers the build configuration, the work of
removing nondeterminism you cannot schedule, and how to tell whether any of it is
working.
