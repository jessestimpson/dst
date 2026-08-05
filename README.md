<img src="assets/eta.svg" alt="" width="112" align="right">

# eta

Deterministic simulation testing for the BEAM.

Pronounced like the Greek letter η, **AY-tuh**, not usually spelled out as E-T-A.

When your project's tests run, `eta` transforms your code via an Erlang
`parse_transform` so that it can be driven by a special deterministic scheduler
and a virtual clock. You define a meaningful workload to exercise your system,
and `eta` will help you inject faults to reach interesting behavior. When
pathology is found, `eta` gives you a perfectly reproducible trace to debug.
This all happens at test time; the code that gets deployed to prod is untouched.

## What it does

- **Serializes scheduling.** `eta_sched` suspends every process it owns and
  resumes exactly one at a time, letting it run until it blocks. The next
  process to run is decided by the seed.
- **Virtualizes time.** `eta_time` is a clock that only moves when the driver
  moves it, which it does when nothing is runnable, jumping straight to the
  next deadline. A 30 second timeout costs nothing.
- **Records and replays.** `eta_run` records every decision it made, and
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
  faults in message delivery on simulated Erlang nodes, and to deliver the
  events a real link or node failure produces: `nodedown`/`nodeup` signals,
  `noconnection` DOWNs for every monitor held across the break, and node death as
  a single operation.

## Getting Started

A system under test provides 6 callbacks (see `eta_harness`), and a run is one
call:

```erlang
#{outcome := Outcome, trace := Trace} =
    eta_run:run(my_harness, #{seed => 7, max_ops => 25, max_steps => 20000, preload => [my_app]}).
```

When a run fails, shrink it:

```erlang
#{trace := Minimal, verified := true} =
    eta_shrink:shrink(my_harness, Trace, #{seed => 7, max_ops => 25, preload => [my_app]}).
```

The library ships with a [two-phase commit implementation as an example](test/support/ets_2pc.erl),
and includes an optional bug that will break an invariant for demonstration
purposes. We also provide a [walkthrough that implements an ABD Qurom Register](docs/06-a-journey-through-dst.md)
and demonstrates the creation of a project from scratch.

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
   end to end: an empty directory becomes a quorum register with a
   planted bug, and the bug becomes a minimized, replayable failure that
   explains itself.

[docs/design.md](docs/design.md) is still a work in progress. Eventually, it will
be the internal specification for the `eta` project itself.

## Installation

```elixir
def deps do
  [{:eta, "~> 0.1", only: :test}]
end
```

## Status

Early and under development.

### Known gaps, roughly by size

- **Limited Elixir support.** The Erlang `parse_transform` is the mechanism
  for keeping your code deployable to prod without disruptive changes. Elixir
  support may come later.
- **One simulation per VM.** `eta_time` and `eta_log` keep state in named ETS
  tables, so runs must be serial. Keep the tests `async: false`. In fact, we
  suggest creating a `mix dst` alias that keeps the rest of your tests
  separate from the simulation, although this isn't required.
- **Shrinking is bounded by positional step ids.** Deleting an operation
  renumbers every process created after it, so operations that contributed
  nothing often survive a shrink. This means that the shrunk replay can
  still be pretty large.

## AI full disclosure

 - This software is developed with strong assistance from LLMs and with humans 
   leading the ideas, testing, and debugging. We say this openly because it shaped
   how the project was built. If you are not happy with AI-developed code, this
   software is not for you. This disclosure was adopted from [antirez/ds4](https://github.com/antirez/ds4).
 - We strive to write and edit the documentation for human consumption. LLM-speak
   will eventually be rooted out in favor of imperfect human writing. Documentation
   generated wholly by LLMs must be annotated as such.

## Etymology

The name is short for **étalon**, the reference artifact a measurement is read
against. Here that artifact is the seed: it names an execution, and every rerun
is compared to it. `eta` is also what the library computes on every idle step,
since the discrete-event loop does nothing but advance to the next deadline.

---

The eta in the logo is outlined from [STIX](https://www.stixfonts.org) General
Italic, used under the SIL Open Font License.
