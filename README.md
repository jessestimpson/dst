<img src="assets/eta.svg" alt="" width="112" align="right">

# eta

Deterministic simulation testing for the BEAM.

`eta` takes control of the parts of a running Erlang system that aren't decided
by its own logic: which process runs next, when a timer fires, what the clock
reads. All of it is driven from a single seeded RNG.

Pronounced like the Greek letter η, **AY-tuh**, not usually spelled out as E-T-A.

The name is short for **étalon**, the reference artifact a measurement is read
against. Here that artifact is the seed: it names an execution, and every rerun
is compared to it. `eta` is also what the library computes on every idle step,
since the discrete-event loop does nothing but advance to the next deadline.

## What it does

- **Serializes scheduling.** `eta_sched` suspends every process it owns and
  resumes exactly one at a time, letting it run until it blocks. The next
  process to run is decided by the seed.
- **Virtualizes time.** `eta_time` is a clock that only moves when the driver
  moves it, which it does when nothing is runnable, jumping straight to the
  next deadline. A 30 second timeout costs nothing.
- **Records and replays.** `eta_run` writes down every decision it made, and
  `replay/3` follows that trace back exactly.
- **Shrinks failures.** `eta_shrink` delta-debugs a failing trace down to the
  decisions that matter, then verifies the result still reproduces.
- **Explains failures.** A trace says which process ran, not what it did.
  `eta_log` records both on one timeline, with your processes named, so a
  counterexample reads as a story instead of a column of integers.
- **Reads frozen state.** `eta_observe` gets a suspended process's state out
  without asking it anything, which is the only way an invariant can inspect a
  system the scheduler has stopped.
- **Injects network faults.** `eta_net` can be configured to automatically inject
  faults in message delivery on simulated Erlang nodes.

## Getting Started

A system under test provides 6 callbacks (see `eta_harness`), and a run is one
call:

```erlang
#{outcome := Outcome, trace := Trace} =
    eta_run:run(my_harness, #{seed => 7, max_ops => 25, max_steps => 20000}).
```

When a run fails, shrink it:

```erlang
#{trace := Minimal, verified := true} =
    eta_shrink:shrink(my_harness, Trace, #{seed => 7, max_ops => 25}).
```

The library ships with a [two-phase commit implementation as an example](test/support/ets_2pc.erl),
and includes an optional bug that will break an invariant for demonstration
purposes. We also provide a [walkthrough that implements an ABD Qurom Register](docs/06-a-journey-through-dst.md)
and demonstrates the bug-finding method.

## Documentation

The walkthrough is the manual, in 6 parts:

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
6. [A journey through DST](docs/06-a-journey-through-dst.md). The whole thing
   end to end, in 15 steps: an empty directory becomes a quorum register with a
   planted bug, and the bug becomes a minimized, replayable failure that
   explains itself.

[docs/design.md](docs/design.md) is still a work in progress. Eventually, it will
be the internal specification for the `eta` project itself.

## Installation

```elixir
def deps do
  [{:eta, "~> 0.1", runtime: false}]
end
```

`runtime: false` rather than `only: :test`. Your modules include
`eta/include/eta.hrl`, and `-include_lib` is resolved before any `-ifdef` inside
the header, so it has to be findable in every environment. `runtime: false`
keeps `eta` out of your release all the same: it isn't added to your
application's `applications` list, so nothing here ships or runs in production.

## Your system under test has to be Erlang

`eta_transform` is an Erlang parse transform, so it never reaches an Elixir
module, and `?ETA_LOG` and `?ETA_LABEL` are Erlang macros. A system written in
Elixir gets none of it: its timers stay on the real clock, its spawns aren't
gated by the scheduler, and it can't record into `eta_log`.

**Your harness is the exception**, and a real one. It isn't transformed — it
runs in the driver process, outside the schedule — so it can be written in
Elixir and call `eta_log` directly. Your tests are `.exs` already.

So an Elixir project can use `eta` today with the simulated part written in
Erlang. Making Elixir a first-class system-under-test language is on the list
below.

## Status

Early. The API is stable enough to build on and has driven 2 systems, but it has
one consumer beyond its own test suite, so expect rough edges outside the paths
that consumer exercises.

### Known gaps, roughly by size

- **Elixir systems can't be transformed.** See above.
- **One simulation per VM.** `eta_time` and `eta_log` keep state in named ETS
  tables, so runs must be serial. Keep the tests `async: false`.
- **No distribution.** A multi-node system has to run its nodes as processes in
  one VM.
- **`erlang:suspend_process/1` at scale is unverified.** It's documented for
  debuggers. Clean at a few hundred processes; behaviour at thousands, with
  supervisors and monitors in play, hasn't been measured.
- **Shrinking is bounded by positional step ids.** Deleting an operation
  renumbers every process created after it, so operations that contributed
  nothing often survive a shrink. See [docs/05](docs/05-gotchas.md).

---

The eta in the logo is outlined from [STIX](https://www.stixfonts.org) General
Italic, used under the SIL Open Font License.
