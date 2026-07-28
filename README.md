# dst

Deterministic simulation testing for the BEAM.

`dst` takes control of everything in a running Erlang system that is not decided by
its own logic — which process runs next, when a timer fires, what the clock reads —
and drives all of it from one seeded random number generator. A seed then names a
whole execution rather than a corner of one: the same seed produces the same
interleaving every time, so a concurrency bug you saw once you can see again, on
demand, with a debugger attached.

It is built for the bugs you would otherwise only understand in hindsight. The ones
that need a particular interleaving, surface once a month in production with no
reproduction, and that you can only write a regression test for after you already
know what they are.

## What it does

- **Serializes scheduling.** `dst_sched` suspends every process it owns and resumes
  exactly one at a time, running it until it blocks. Which one comes from the seed.
- **Virtualises time.** `dst_time` is a clock that only moves when the driver moves
  it, which it does when nothing is runnable, jumping straight to the next deadline.
  A thirty-second timeout costs nothing.
- **Records and replays.** `dst_run` writes down every decision it made, and
  `replay/3` follows that trace back exactly.
- **Shrinks failures.** `dst_shrink` delta-debugs a failing trace down to the
  decisions that matter, and verifies the result still reproduces.
- **Reads frozen state.** `dst_observe` gets a suspended process's state out without
  asking it anything, which is the only way an invariant can inspect a system the
  scheduler has stopped.

Message ordering needs no special handling. Erlang already guarantees that messages
between an ordered pair of processes arrive in send order, so once one process runs
at a time, delivery order follows from the order in which senders were stepped.

## A taste

A system under test provides six callbacks (see `dst_sut`), and a run is one call:

```erlang
#{outcome := Outcome, trace := Trace} =
    dst_run:run(my_sut, #{seed => 7, max_ops => 25, max_steps => 20000}).
```

When a run fails, shrink it:

```erlang
#{trace := Minimal, verified := true} =
    dst_shrink:shrink(my_sut, Trace, #{seed => 7, max_ops => 25}).
```

The library ships with a two-phase commit implementation as a worked example,
including a bug that only an unlucky interleaving reveals. It is walked through end
to end in the documentation.

## Documentation

The [walkthrough](docs/overview.md) is the manual, in five parts:

1. [What DST is](docs/01-what-dst-is.md) — the class of bug, how the technique
   works, and what it costs.
2. [Setting up a project](docs/02-setting-up.md) — build configuration, the
   nondeterminism you have to remove first, and how to tell it is working.
3. [A worked example](docs/03-two-phase-commit.md) — two-phase commit, start to
   finish.
4. [Writing a system under test](docs/04-writing-a-system-under-test.md) — the six
   callbacks and the practices around them.
5. [Gotchas and footguns](docs/05-gotchas.md) — read this one.

[docs/design.md](docs/design.md) is the record of how the library was built, kept for
what each phase taught rather than as a manual.

## Installation

```elixir
def deps do
  [{:dst, "~> 0.1", only: :test}]
end
```

`only: :test` is the usual choice. The parse transform goes on your modules under an
`-ifdef(DST)` guard so it cannot reach a release build, and nothing here is needed at
runtime in production.

## Status

Early. The API is stable enough to build on and has driven two systems, but it has
one consumer beyond its own test suite, so expect rough edges outside the paths that
consumer exercises. Known gaps are listed at the end of
[docs/design.md](docs/design.md); the two most likely to affect you are that
`receive ... after` needs a second parse transform which cannot currently coexist
with the first, and that a VM can host only one simulation at a time.
