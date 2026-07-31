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
  [{:dst, "~> 0.1", only: :test}]
end
```

`only: :test` is the usual choice. The parse transform goes on your modules
behind an `-ifdef(DST)` guard so it can't reach a release build, and nothing
here is needed at runtime in production.

## Status

Early.
