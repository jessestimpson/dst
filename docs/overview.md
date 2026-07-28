# Deterministic simulation testing on the BEAM

A walkthrough of the simulation testing framework used in this project: what it does,
how to point it at a system of your own, and the things that will cost you an
afternoon if nobody warns you first.

It assumes you write Erlang or Elixir for a living. It does not assume you have built
a consensus protocol, read a TLA+ specification, or spent much time thinking about
linearizability; where distributed-systems vocabulary is unavoidable it is defined
where it appears.

## The pages

1. **[What deterministic simulation testing is](01-what-dst-is.md)** — the class of
   bug it targets, how the technique works, and what it costs.
2. **[Setting up a project](02-setting-up.md)** — the moving parts, the build
   configuration, and how to tell whether any of it is working.
3. **[A worked example: two-phase commit](03-two-phase-commit.md)** — a complete
   system under test, a planted bug, and the search that finds it.
4. **[Writing a system under test](04-writing-a-system-under-test.md)** — the six
   callbacks, and the practices that make a real system testable this way.
5. **[Gotchas and footguns](05-gotchas.md)** — everything that has gone wrong here,
   listed by the symptom you will actually see.

If you are only going to read one of them, read the last. Most of the cost of
adopting this is not writing the harness; it is the week spent not knowing why a seed
produced two different runs.

## The modules

| Module | Responsibility |
|---|---|
| `dst_sched` | Runs one process at a time. The scheduler. |
| `dst_time` | Virtual clock and timer wheel. |
| `dst_transform` | Parse transform: points timer, clock and spawn calls at the above. |
| `dst_observe` | Reads a suspended process's state without asking it. |
| `dst_sut` | The six callbacks a system under test provides. |
| `dst_run` | The driver: seed in, trace out. |
| `dst_shrink` | Reduces a failing trace to something readable. |

There is also `dst_after_transform`, which puts `receive ... after` timeouts on the
virtual clock. It is written and tested but not yet applied to anything, for a reason
covered on [page 5](05-gotchas.md).

The design document, [`docs/design.md`](design.md), is the historical record: what
each phase of the work taught, including the parts of the plan that turned out to be
wrong. These pages are the manual.
