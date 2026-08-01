# Setting up a project

## The parts

Once these are configured you rarely think about most of them.

```
  dst_run          the driver: turns a seed into a run
    |
    +-- dst_sched  runs one process at a time, choosing from the seeded RNG
    +-- dst_time   virtual clock; timers fire when the driver says so
    +-- dst_log    what your system did, on the same timeline
    +-- your harness   six callbacks (dst_harness)
```

The other piece is `dst_transform`, a parse transform applied to the modules
that make up the system under test. It rewrites their timer, clock and spawn
calls to point at `dst_time` and `dst_sched` instead of `erlang`, and puts
`receive ... after` on the virtual clock. It changes nothing else about them.

You don't name it directly. A module opts in by including one header, covered
next.

`dst_run` owns the seed. Every choice comes from a single `rand` state seeded
from it: which process to step, whether to inject a client operation or let the
system make progress, and whatever your workload generator draws. At each
iteration the driver does exactly one of 3 things. It steps a runnable process
until it blocks, injects the next generated operation, or advances the clock to
the next timer deadline. Then it checks your invariants against the frozen
system.

## Build configuration

Every module that takes part in a simulation includes one header:

```erlang
-include_lib("dst/include/dst.hrl").
```

That's the whole opt-in. Under a simulation build it brings the parse transform
and the `?DST_LOG` and `?DST_LABEL` macros; under a release build it brings
nothing at all, so a module carrying it has no relationship to this library and
can ship unchanged.

What decides which is the `DST` define. In Mix:

```elixir
defp erlc_options(:test), do: [:debug_info, {:d, :DST}]
defp erlc_options(_), do: [:debug_info]
```

and in rebar3, an equivalent profile carrying `{erl_opts, [{d, 'DST'}]}`.

**That define is a contract, not a convention.** Forget it and the header
silently contributes nothing: no transform, so your timers stay on the real
clock, and no logging. The library made this mistake in its own build and its
example module quietly went untransformed for a while, which is a good argument
for putting the define in before anything else.

We'd suggest copying 2 decisions here.

The first is to enable the transform for the whole test environment rather than
a dedicated simulation profile. `dst_time` falls back to the real `erlang`
functions whenever no virtual clock is running, so a transformed module behaves
exactly as it did before outside a simulation. If you compile your ordinary
test suite with the transform on, every test you already have is continuously
demonstrating that inertness. A separate profile only demonstrates it for the
tests you remember to run under it.

The second is to guard per module rather than per application. You won't want
the transform everywhere, for reasons covered under the transform boundary
below.

## Step one: find the nondeterminism you can't schedule

Before any of the framework matters, take an inventory of everything your
system touches that the scheduler doesn't own. Ports, NIFs, sockets, timers
inside dependencies, and anything that talks to a service over a network.

For `dgen_registry`, that was the database. Its transactions expire on the
*real* clock, so a process suspended mid-transaction dies with `tooslow`. The
scheduler freezes the process and the database gives up on it. Our only choice
was to build a substitute that could meet the determinism requirements: a
pure-Erlang, in-memory implementation of the same backend behavior.

Choosing DST means you have to find and root out every source of
nondeterminism, and that can be expensive and time-intensive. Make sure the
investment will pay off before you get started.

## Step two: the transform boundary

Not every module should be transformed, and where you draw the line is a design
decision you'll live with.

Transform the modules that *are* the system, the ones whose timers, spawns and
clock reads belong under the schedule's control. For the registry that's 6
modules: the member, elector and connector processes, plus the generic server,
queue and transaction machinery underneath them.

Leave client-facing modules alone if they hold polling loops. The registry's
public API module is a collection of functions clients call, and one of them
waits for the local node to become ready by polling with `timer:sleep/1`. Under
a frozen clock that loop never terminates, so the module stays outside the
boundary deliberately.

That costs you something. A client's `gen_server:call` timeout is on the real
clock, and under the scheduler a client is suspended between steps, so when its
call times out is decided by wall clock rather than by the schedule. It's a
genuine determinism leak in the write path. Our workaround is to configure the
timeout high enough that it can't fire during a run, which in turn means the
behavior it guards, a registration blocking while no leader is reachable, isn't
exercised at all. It's an acknowledged gap rather than a solved problem. Write
it down somewhere before somebody "fixes" the boundary by transforming one more
module and spends a day on the resulting hang.

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

A bare `monotonic_time()` is left alone, and has to be. None of these functions
are auto-imported, so an unqualified call is a call to something your own
module defines, and rewriting it would break the module. Code that wants to be
simulated has to qualify its time calls, which is the prevailing style anyway.

`timer:sleep/1` isn't rewritten either, and the failure it produces is
confusing enough to explain. A sleep is `receive after T -> ok end` inside
OTP's `timer` module, which no transform of *your* code reaches. The sleeping
process is blocked on a timeout rather than on a message, and `dst_sched`
decides what is runnable by looking at mailboxes. So it sees a process with
nothing waiting for it and never steps it. Only the real clock can end that
wait, and the driver doesn't control the real clock. Code under simulation
shouldn't sleep at all, and quietly virtualizing `timer:sleep/1` would hide
that rather than surface it.

The spawn rewrites matter more than they look. A child spawned with a plain
`erlang:spawn` is adopted by the scheduler through `set_on_spawn` tracing,
which reports the spawn only after the child already exists. It runs briefly,
on the real scheduler, before anything suspends it. `dst_sched:spawn/1` starts
the child blocked on a token instead, so there's no window at all.

## Step four: make your state readable

Invariants run against a system in which every process is suspended, so an
invariant can't call into one.
[Writing a system under test](04-writing-a-system-under-test.md) makes the full
argument. The setup step is a second attribute on the modules whose state the
invariants need:

```erlang
-include_lib("dst/include/dst.hrl").
-dst_observe([leader, epoch, applied_version]).
```

The transform republishes those fields into the process dictionary on every
`gen_server` callback return, where `erlang:process_info/2` can read them from
outside. That includes while the process is suspended, in a couple of
microseconds, regardless of how deep its mailbox is. Reading is:

```erlang
#{leader := L, epoch := E} = dst_observe:read(my_server).
```

Publishing on every callback return is what makes staleness impossible. There's
no assignment site anybody can forget to update. Naming the fields you need is
usually better than `-dst_observe(all)`, since `read/1` copies whatever was
published and gets called after every step of a simulation. Publishing a state
that contains an inverted index or a queue isn't something you want to do 1000s
of times.

The attribute is an ordinary module attribute the compiler ignores, so it's
inert in a build without the transform.

## Step five: a first run

With a system under test written, which is the [next page](03-two-phase-commit.md),
a run is one call:

```erlang
#{outcome := Outcome, trace := Trace, steps := Steps} =
    dst_run:run(my_sut, #{seed => 7, max_ops => 25, max_steps => 20000}).
```

The options you're likely to touch:

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
| `preload` | `[]` | Applications to load before the run starts. **Name yours.** |
| `log` | `true` | Collect a `dst_log` record of the run |

3 of those deserve more explanation.

`preload` is the one to set on your first run. Loading a module on demand is a
synchronous call into `code_server`, which the scheduler doesn't own, so a
scheduled process that reaches cold code blocks on something outside the
schedule. `kernel`, `stdlib` and `dst` are always loaded; name your own
application on top. The [gotchas page](05-gotchas.md) has the failure it
prevents, and it is worse than it sounds.

`settle_steps` exists because a system with periodic timers never becomes
quiet. A heartbeat means "nothing runnable and nothing pending" is a state you
never reach, so without a settle phase the only way such a run can end is by
exhausting the step budget, which is reported as an error. Every run looks like
a failure. Running on for a bounded while after the last operation, then
finishing normally, makes quiescence-by-timer a success. The phase isn't dead
time either. It's the only part of a run where the invariants are checked with
no client traffic at all, which is where quiet-period defects live.

`quiet_p` is the probability of letting the clock advance rather than injecting
work when nothing is runnable. The obvious driver always injects at that point,
and doing so puts a whole class of defect out of reach, because the system is
never left alone with only its own timers for company. It's less a tuning knob
than the thing that makes those bugs reachable at all.

## Checking that any of this works

3 measurements, in order of how much they tell you.

The first is the run's own accounting:

```erlang
ok = dst_run:audit(dst_run:run(my_harness, Opts)).
```

Several fields in a result mean "part of this interleaving was decided by wall
clock rather than by the schedule", and a run with any of them set is one whose
seed won't reproduce. `audit/1` checks them, and keeps checking them as more get
added:

- `modules_loaded` — code loaded mid-run, so a scheduled process called into
  `code_server`. Fix with `preload`.
- `sched.adopted_late` — processes that ran before the scheduler owned them.
  Fix by spawning through `dst_run:spawn_op/1`.
- `sched.timeouts` — steps that ended without the process reaching a receive,
  so it was suspended wherever it happened to be.

It answers `{suspect, Reasons}` rather than an error, because the run happened
and its violation, if any, is real. What you can't do is trust the seed to give
it back. Assert on it next to your invariants.

The second is the single most valuable assertion in the whole setup: that a
seed reproduces itself.

```erlang
Traces = [maps:get(trace, dst_run:run(my_sut, Opts#{seed => 3})) || _ <- lists:seq(1, 5)],
1 = length(lists:usort(Traces)).
```

Run it early, run it on several seeds, and run it again whenever you touch
anything. It's what catches the determinism leaks you haven't thought of yet.
When it fails, pay attention to *which* of the runs differ rather than just how
many distinct traces came out. Grouping 5 runs as `[[1], [2,3,4,5]]` means
something very specific and very fixable, as [page 5](05-gotchas.md) explains.

The third is that a recorded trace replays:

```erlang
Original = dst_run:run(my_sut, Opts),
Replayed = dst_run:replay(my_sut, maps:get(trace, Original), Opts),
0 = maps:get(skipped, Replayed).
```

That's a different claim from the second one. It exercises `replay/3` rather
than `run/2`, and conflating the two let a real bug live in this framework for
months. Test both.

## Reading a result

Don't print one whole. A 20-operation run produces a few hundred trace entries,
so a shell renders the schedule and elides the fields you wanted underneath it.

```erlang
#{outcome := ok, steps := 97, trace_length := 213} =
    dst_run:summary(dst_run:run(my_harness, Opts)).
```

The trace is for later, once you have a failure worth shrinking, and even then
it isn't what you read. `{step, 8}` records which process the scheduler chose
and says nothing about what that process did. `dst_log` records what your
*system* did on the same timeline, with your processes named, and that is the
thing to read. [Page 3](03-two-phase-commit.md) shows one.

## Next

[A worked example](03-two-phase-commit.md) is a complete system under test,
small enough to read in one sitting, with a bug planted in it.
