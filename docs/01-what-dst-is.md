# What is deterministic simulation testing?

Deterministic simulation testing is a way of finding the bugs you would otherwise
only understand in hindsight. Distributed systems, especially those with high
concurrency, like many Erlang and Elixir libraries, can exhibit unique behaviors
that depend on things typically outside of a developer's control, such as the
precise moment a timer fires or the scheduling on runnable processes on the
BEAM schedulers. Some bugs may be the result of a specific interleaving of
those concurrent events. DST aims to reproduce a specific problematic
event interleaving with zero uncertainty.

The approach involves defining a single seed for a random number generator
and deriving all system concurrency from that single seed. When a particular
seed shows some system pathology, the seed itself becomes the "name" of the bug.
Any person on any machine can reproduce the bug with certainty and troubleshoot
its details.

DST is an investment in time and complexity. This page explains the costs and
potential rewards so that you can make an informed decision for your project.
The rest of the walkthrough covers how to use the `dst` library itself.

## An example bug

`dst` was built out of necessity. We were working on an Erlang process registry
(`dgen_registry`) that had its own bespoke replication strategy, and things
were getting complicated. This particular registry had a simple leader election
process, and a goal to store `name -> pid` mapping in memory. Simple enough
conceptually, but when the leader leaves the cluster, a follower must take
over, and the project needs to provide certain guarantees about the consistency
of the mapping during such an event, and the whole system needs to be as fast
and responsive as possible.

Given these requirements, when a follower receives a request for a pid
registration, it forwards the request to the leader, and upon receiving
a response, records the mapping locally. In addition, the leader replicates
new mappings to all followers. This means a particular follower has 2
potential write paths to its local mapping: from a request that it forwards,
and from a request that it was not otherwise involved in.

When `dst` was being used for the first time, a particular simulation caused
a register and an unregister of a particular name to land in the same batch on
the leader, but had origins from 2 different followers. When these writes
were interleaved just right, one of the followers ended up with a stale
mapping that never gets resolved.

Now, one could argue that this flaw should have been identified in the design
stage, but even if it was, writing a test to prevent its regression would require
an extreme amount of control over the event interleavings. DST provides exactly
this control, and at the same time gives the promise of finding more.

## How it works on the BEAM

In combination with formal methods and traditional testing techniques, DST has
been a critical part in the development of some of the most successful
distributed systems projects: FoundationDB and TigerBeetle. The method is pretty
easy to state concisely:

> Take control of every source of nondeterminism in the system, drive all of them
> from one seeded random number generator, and a seed becomes a complete, replayable
> execution.

On the BEAM, that means the following.

**Process scheduling.** The VM's scheduler tracks runnable Erlang processes and
decides when to execute them. A single scheduler is single threaded, but the
order of executions is not in your control. `dst_sched` acts as our process
scheduler. It suspends every process it owns, and resumes exactly one at a time,
allowing it to run until it yields. The next runnable process to be scheduled
is decided from the seeded RNG.

**Time.** Timers are driven by a system wall clock. Say you have a `gen_server`
that takes some action every 1000 msec. How many messages can it process in that
1 sec of wall clock time? It can vary; it's unpredictable; it's not reproducible.
`dst_time` provides a virtual clock. The clock moves deterministically, detached
from the wall clock, meaning we can control executions, but also we can effectively
speed time up dramattically, covering a lot of virtual time in a short amount of
real time.

**Anything outside the VM.** Network, file I/O, NIFs, a database, etc. `dst` cannot
directly help here. It's up to you to root these out and replace them with
deterministic stand-ins.

## What it buys you

Right off the bat, you get perfect bug reproduction. When CI finds a failure,
it records the seed and the pathology can be replayed by anyone on the team.
When actual determinism is achieved, the reproduction is perfect.

With perfect replayability, you can debug system state more precisely, and
find the logic error more quickly.

But perfect replayability also buys you something less obvious: minimization
of the trace (a.k.a. shrinking). Once a trace and behavior is identified,
a search can be conducted to find the minimal set of steps necessary to
match the behavior. This can reduce a trace of 1000s of events to a dozen,
again speeding up the resolution. `dst_shrink` is provided for this purpose.
Even still, the trace is merely a set of steps for the scheduler to take,
and doesn't tell a story about your system. `dst_log` is provided to help
you define the narrative of your system.

As mentioned above, virtual time can cover lots of ground that real-time
simulation testing could not. A run of `dgen_registry` can simulate ~280
seconds worth of testing in ~200 msec of real time.

Finally, reliable and reproducible testing methods are critical if you
choose to enlist an AI agent in developing distributed systems logic.
Some things can cannot be left to probability, such as your bank account's
balance after concurrent transactions. A DST framework can be one pillar
in a formal methods approach to AI-assisted development.

## Drawbacks of DST

It's not exhaustive. There is no guarantee that every possible event
interleaving will be exercised. Instead, formal modeling with something
like TLA+ and checking with TLC **can** make such a guarantee.

It's not cheap. As we alluded above, you are responsible for rooting out
all sources of nondeterminsm. `dst` will help you along the way, but there
is real significant work in managing out the nondeterminism. Your
Erlang modules will need to be written in a certain way for `dst` to
engage its tooling. For example, the `dst_transform` parse transform
is a critical part of `dst`'s tooling, and it means your code must
adhere to its requirements.

It's not a fuzzer or property-based testing. You may choose to employ
a fuzzer to generate more DST workloads, but the inputs are your
responsibility. `dst` will help with the execution.

## Next

[Setting up a project](02-setting-up.md) covers the buidl config, some
tips on removing nondeterminism sources, and how to conclude whether or
not it's working.
