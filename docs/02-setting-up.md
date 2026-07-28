# Setting up a project

## The parts, and how they fit together

There are four pieces of machinery, and once configured you rarely think about three
of them.

```
  dst_run          the driver: turns a seed into a run
    |
    +-- dst_sched  runs one process at a time, choosing from the seeded RNG
    +-- dst_time   virtual clock; timers fire when the driver says so
    +-- your SUT   six callbacks (dst_sut)
```

The fourth is `dst_transform`, a parse transform you apply to the modules that make
up the system under test. It rewrites their timer, clock and spawn calls to point at
`dst_time` and `dst_sched` instead of at `erlang`, and it changes nothing else about
them.

`dst_run` owns the seed. Every choice comes from a single `rand` state seeded from
it: which process to step, whether to inject a client operation or let the system
make progress, and whatever your workload generator draws. At each iteration the
driver does exactly one of three things — step a runnable process until it blocks,
inject the next generated operation, or advance the clock to the next timer deadline
— and then checks your invariants against the frozen system.

## Build configuration

The transform is opt-in per module and guarded so it cannot reach production:

```erlang
-ifdef(DST).
-compile({parse_transform, dst_transform}).
-endif.
```

with the simulation build defining `DST`. In Mix that is:

```elixir
defp erlc_options(:test), do: [:debug_info, {:d, :DST}]
defp erlc_options(_), do: [:debug_info]
```

and in rebar3, an equivalent profile carrying `{erl_opts, [{d, 'DST'}]}`.

Two decisions here are worth copying. The first is to enable the transform for the
whole test environment rather than for a dedicated simulation profile. `dst_time`
falls back to the real `erlang` functions whenever no virtual clock is running, so a
transformed module behaves exactly as it did before outside a simulation. Compiling
the ordinary test suite with the transform on means every test you already have is
continuously demonstrating that inertness, whereas a separate profile only
demonstrates it for the tests you remember to run under it.

The second is to guard per module rather than per application. You will not want the
transform everywhere, for reasons covered under the transform boundary below.

## Step one: find the nondeterminism you cannot schedule

Before any of the framework matters, take an inventory of everything your system
touches that the scheduler does not own. Ports, NIFs, sockets, timers inside
dependencies, and anything that talks to a service over a network.

For our process registry, that was the database. Its transactions expire on the
*real* clock, so a process suspended mid-transaction dies with `tooslow` — the
scheduler freezes the process and the database gives up on it. Our only choice was to
implement a substitute that could adhere to our determinism requirements: a
pure-Erlang, in-memory implementation of the same backend behaviour.

Choosing DST for your project necessarily means that you must find and root out all
sources of nondeterminism, which can be an expensive and time-intensive endeavor.
Make sure the investment will pay off before you get started.

## Step two: the transform boundary

Not every module should be transformed, and where you draw the line is a design
decision with consequences you will live with.

Transform the modules that *are* the system: the ones whose timers, spawns and clock
reads belong under the schedule's control. For the registry that is six modules — the
member, elector and connector processes, plus the generic server, queue and
transaction machinery underneath them.

Leave client-facing modules alone if they hold polling loops. The registry's public
API module is a collection of functions that clients call, and one of them waits for
the local node to become ready by polling with `timer:sleep/1`. Under a frozen clock
that loop never terminates, so the module stays outside the boundary deliberately.

That costs something, and it is better stated than discovered. A client's
`gen_server:call` timeout is on the real clock, and under the scheduler a client is
suspended between steps, so when its call times out is decided by wall clock rather
than by the schedule. This is a genuine determinism leak in the write path. The
workaround here is to configure the timeout high enough that it cannot fire during a
run, which in turn means the behaviour it guards — a registration blocking while no
leader is reachable — is not exercised at all. It is an acknowledged gap rather than
a solved problem, and it is the kind of thing to write down somewhere before somebody
"fixes" the boundary by transforming one more module and spends a day on the
resulting hang.

## Step three: qualify your time calls

The transform rewrites qualified calls, and only qualified calls:

| From | To |
|---|---|
| `erlang:send_after/3,4` | `dst_time:send_after/3,4` |
| `erlang:start_timer/3,4` | `dst_time:start_timer/3,4` |
| `erlang:cancel_timer/1,2` | `dst_time:cancel_timer/1,2` |
| `erlang:read_timer/1` | `dst_time:read_timer/1` |
| `erlang:monotonic_time/0,1` | `dst_time:monotonic_time/0,1` |
| `erlang:system_time/0,1` | `dst_time:system_time/0,1` |
| `erlang:timestamp/0` | `dst_time:timestamp/0` |
| `os:system_time/0,1`, `os:timestamp/0` | `dst_time:*` |
| `erlang:spawn/1,3`, `spawn_link`, `proc_lib` equivalents | `dst_sched:*` |
| `gen_server:start/3`, `start_link/3`, `start_monitor/3` | `dst_sched:*` |

A bare `monotonic_time()` is left alone, and has to be: none of these functions are
auto-imported, so an unqualified call is a call to something your own module defines,
and rewriting it would break the module. Code that wants to be simulated therefore
has to qualify its time calls, which is the prevailing style anyway.

`timer:sleep/1` is deliberately not rewritten either, and the reason is worth
understanding because the failure it produces is confusing. A sleep is
`receive after T -> ok end` inside OTP's `timer` module, which no transform of *your*
code reaches. The sleeping process is blocked on a timeout rather than on a message,
and `dst_sched` decides what is runnable by looking at mailboxes, so it sees a
process with nothing waiting for it and never steps it. Only the real clock can end
that wait, and the driver does not control the real clock. Code under simulation
should not sleep at all, and quietly virtualising `timer:sleep/1` would hide that
rather than surface it.

The spawn rewrites matter more than they look. A child spawned with a plain
`erlang:spawn` is adopted by the scheduler through `set_on_spawn` tracing, which
reports the spawn only after the child already exists — so it runs briefly, on the
real scheduler, before anything suspends it. `dst_sched:spawn/1` starts the child
blocked on a token instead, so there is no window at all.

## Step four: make your state readable

Invariants run against a system in which every process is suspended, so an invariant
cannot call into one. [Writing a system under test](04-writing-a-system-under-test.md)
makes the full argument; the setup step is a second attribute on the modules whose
state the invariants need:

```erlang
-ifdef(DST).
-compile({parse_transform, dst_transform}).
-endif.
-dst_observe([leader, epoch, applied_version]).
```

The transform then republishes those fields into the process dictionary on every
`gen_server` callback return, where `erlang:process_info/2` can read them from
outside — including while the process is suspended, in a couple of microseconds,
regardless of how deep its mailbox is. Reading is:

```erlang
#{leader := L, epoch := E} = dst_observe:read(my_server).
```

Publishing on every callback return is what makes staleness impossible: there is no
assignment site anybody can forget to update. Naming the fields you need is usually
better than `-dst_observe(all)`, since
`read/1` copies whatever was published and gets called after every step of a
simulation; publishing a state that contains an inverted index or a queue is not
something you want to do thousands of times.

The attribute is an ordinary module attribute that the compiler ignores, so it is
inert in a build without the transform.

## Step five: a first run

With a system under test written — that is the [next page](03-two-phase-commit.md) —
a run is one call:

```erlang
#{outcome := Outcome, trace := Trace, steps := Steps} =
    dst_run:run(my_sut, #{seed => 7, max_ops => 25, max_steps => 20000}).
```

The options you are likely to touch:

| Option | Default | Meaning |
|---|---|---|
| `seed` | 0 | Fixes the whole run |
| `config` | `#{}` | Passed through to your `init/2` |
| `max_ops` | 50 | Client operations to inject before letting the system settle |
| `max_steps` | 10000 | Safety bound on steps and clock advances together |
| `settle_steps` | 2000 | How much longer to run once the operations are done |
| `op_p` | 0.3 | Chance of injecting rather than stepping, when both are possible |
| `quiet_p` | 0.3 | Chance of letting time pass rather than injecting, when nothing is runnable |
| `check_every` | 1 | Run the invariants every N actions |

Two of those deserve additional explanation.

`settle_steps` exists because a system with periodic timers never becomes quiet. A
heartbeat means "nothing runnable and nothing pending" is a state you never reach, so
without a settle phase the only way such a run can end is by exhausting the step
budget, which is reported as an error — every run looks like a failure. Running on
for a bounded while after the last operation, and then finishing normally, makes
quiescence-by-timer a success. The phase is not dead time, either: it is the only
part of a run where the invariants are checked with no client traffic at all, which
is where quiet-period defects live.

`quiet_p` is the probability of letting the clock advance rather than injecting work
when nothing is runnable. The obvious driver always injects at that point, and doing
so removes a whole class of defect from reach, because the system is never left alone
with only its own timers for company. It is less a tuning knob than the thing that
makes those bugs reachable at all.

## Checking that any of this works

Three measurements, in order of how much they tell you.

The first is the scheduler's own accounting:

```erlang
#{sched := #{adopted_late := Late}} = dst_run:run(my_sut, Opts).
```

`adopted_late` counts processes that ran before the scheduler owned them. A run with
a high count is a run whose interleaving is partly decided by wall clock, and its
seed will not reproduce.

The second, and the single most valuable assertion in the whole setup, is that a seed
reproduces itself:

```erlang
Traces = [maps:get(trace, dst_run:run(my_sut, Opts#{seed => 3})) || _ <- lists:seq(1, 5)],
1 = length(lists:usort(Traces)).
```

Run it early, run it on several seeds, and run it again whenever you touch anything.
It is what catches the determinism leaks you have not thought of yet. When it fails,
pay attention to *which* of the runs differ rather than just how many distinct traces
came out; grouping five runs as `[[1], [2,3,4,5]]` means something very specific and
very fixable, as [page 5](05-gotchas.md) explains.

The third is that a recorded trace replays:

```erlang
Original = dst_run:run(my_sut, Opts),
Replayed = dst_run:replay(my_sut, maps:get(trace, Original), Opts),
0 = maps:get(skipped, Replayed).
```

This is a different claim from the second one — it exercises `replay/3` rather than
`run/2` — and conflating the two let a real bug live in this framework for months.
Test both.

## Next

[A worked example](03-two-phase-commit.md) is a complete system under test, small
enough to read in one sitting, with a bug planted in it.
