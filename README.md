# dst

Deterministic simulation testing for the BEAM.

`dst` takes control of the parts of a running Erlang system that aren't decided
by its own logic: which process runs next, when a timer fires, what the clock
reads. All of it is driven from a single seeded RNG.

## What it does

- **Serializes scheduling.** `dst_sched` suspends every process it owns and
  resumes exactly one at a time, letting it run until it blocks. The next
  process to run is decided by the seed.
- **Virtualizes time.** `dst_time` is a clock that only moves when the driver
  moves it, which it does when nothing is runnable, jumping straight to the
  next deadline. A 30 second timeout costs nothing.
- **Records and replays.** `dst_run` writes down every decision it made, and
  `replay/3` follows that trace back exactly.
- **Shrinks failures.** `dst_shrink` delta-debugs a failing trace down to the
  decisions that matter, then verifies the result still reproduces.
- **Explains failures.** A trace says which process ran, not what it did.
  `dst_log` records both on one timeline, with your processes named, so a
  counterexample reads as a story instead of a column of integers.
- **Reads frozen state.** `dst_observe` gets a suspended process's state out
  without asking it anything, which is the only way an invariant can inspect a
  system the scheduler has stopped.

## Getting Started

A system under test provides 6 callbacks (see `dst_harness`), and a run is one
call:

```erlang
#{outcome := Outcome, trace := Trace} =
    dst_run:run(my_sut, #{seed => 7, max_ops => 25, max_steps => 20000}).
```

When a run fails, shrink it:

```erlang
#{trace := Minimal, verified := true} =
    dst_shrink:shrink(my_sut, Trace, #{seed => 7, max_ops => 25}).
```

The library ships with a two-phase commit implementation as an example,
and includes an optional bug that will break an invariant for demonstration
purposes.

## Documentation

The walkthrough is the manual, in 5 parts:

1. [What DST is](docs/01-what-dst-is.md). Discusses the kinds of bugs we're
   looking for, how the technique works, and how your project implementation
   needs to change to use it.
2. [Setting up a project](docs/02-setting-up.md). Includes build configuration,
   some first steps to remove nondeterminism, and getting the system running
   for the first time.
3. [A worked example](docs/03-two-phase-commit.md). A simple two-phase commit
   implementation, with an included bug for demonstration.
4. [Writing a system under test](docs/04-writing-a-system-under-test.md). The
   behaviour callbacks and the practices around them.
5. [Gotchas and footguns](docs/05-gotchas.md). Nondeterminism can leak in from
   many sources. This page details some common pain points.

[docs/design.md](docs/design.md) is still a work in progress. Eventually, it will
be the internal specification for the `dst` project itself.

## Installation

```elixir
def deps do
  [{:dst, "~> 0.1", runtime: false}]
end
```

`runtime: false` rather than `only: :test`. Your modules include
`dst/include/dst.hrl`, and `-include_lib` is resolved before any `-ifdef` inside
the header, so it has to be findable in every environment. `runtime: false`
keeps `dst` out of your release all the same: it isn't added to your
application's `applications` list, so nothing here ships or runs in production.

## Your system under test has to be Erlang

`dst_transform` is an Erlang parse transform, so it never reaches an Elixir
module, and `?DST_LOG` and `?DST_ROLE` are Erlang macros. A system written in
Elixir gets none of it: its timers stay on the real clock, its spawns aren't
gated by the scheduler, and it can't record into `dst_log`.

**Your harness is the exception**, and a real one. It isn't transformed — it
runs in the driver process, outside the schedule — so it can be written in
Elixir and call `dst_log` directly. Your tests are `.exs` already.

So an Elixir project can use `dst` today with the simulated part written in
Erlang. Making Elixir a first-class system-under-test language is on the list
below.

## Status

Early. The API is stable enough to build on and has driven 2 systems, but it has
one consumer beyond its own test suite, so expect rough edges outside the paths
that consumer exercises.

### Known gaps, roughly by size

- **No network simulation.** `dst` controls scheduling and time; it does not
  control message delivery. A client that sends to 3 peers does so inside one
  scheduler step, so a partially-delivered broadcast isn't a state the scheduler
  can produce, and you have to model that fault in your workload instead.
  Partial delivery, per-link reordering and partitions are all out of reach.
  This is the largest gap.
- **Elixir systems can't be transformed.** See above.
- **One simulation per VM.** `dst_time` and `dst_log` keep state in named ETS
  tables, so runs must be serial. Keep the tests `async: false`.
- **No distribution.** A multi-node system has to run its nodes as processes in
  one VM.
- **`erlang:suspend_process/1` at scale is unverified.** It's documented for
  debuggers. Clean at a few hundred processes; behaviour at thousands, with
  supervisors and monitors in play, hasn't been measured.
- **Shrinking is bounded by positional step ids.** Deleting an operation
  renumbers every process created after it, so operations that contributed
  nothing often survive a shrink. See [docs/05](docs/05-gotchas.md).
