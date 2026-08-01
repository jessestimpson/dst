-module(eta_harness).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
The contract between your system and `eta_run` — Phase 3 of the DST framework
(design: `docs/design.md`).

A harness is not the system under test. Your `gen_server`s and your protocol
code are that. A harness is the adapter that **starts** the system, **declares**
which processes to schedule, **drives** it with a workload, and **judges** it
against invariants. `eta_run` supplies the seed, owns the scheduler and the
clock, decides when to inject work and when to let the system make progress, and
records enough to replay the run exactly.

Six callbacks are required and one is optional.

## Two rules that are not obvious, and both bite silently

**`execute/2` must not block.** Every process the scheduler owns is *suspended*
between steps, so a synchronous call into one from the driver cannot be served
and will sit there until something times out. An operation is issued by spawning
a process to perform it, and `eta_run` picks that process up through
`processes/1` and schedules it like any other. This is not a limitation of the
framework so much as what a driver is: the client is part of the system being
interleaved.

**`check/1` must not call into a scheduled process either — and getting this
wrong does not fail loudly.** `eta_run` bounds the call and reports
`check_blocked` if it hangs, but the more likely outcome is worse. A client API
that catches its own timeout — a `status/1` that returns `undefined` on
`exit:_` — turns a suspended member into a plausible-looking answer, and an
invariant computed over "no members believe they lead" passes while checking
nothing at all.

Read state out of band: ETS, `persistent_term`, a table the system keeps anyway.
Reading a registry's names table directly, the way its own lookups do, is the
shape to copy; anything that sends a message and waits is not.

## The callbacks

`init(Seed, Config)` — start the system and return with it **quiescent**.
Anything still running when `eta_run` takes over ran on the real scheduler,
outside the schedule, and that is exactly the nondeterminism the framework
exists to remove.

`processes(State)` — every pid to schedule. Re-consulted after each operation,
so processes an operation creates are picked up; already-known pids are ignored,
so returning the full set every time is correct and expected. **The order is
part of the contract**, because `eta_sched` assigns ids in registration order and
the trace records ids.

`generate(State, Rand)` — the next operation, from the supplied `rand` state.
Return the advanced state; drawing entropy from anywhere else breaks replay.

`execute(Op, State)` — issue the operation. See the first rule above.

`check(State)` — the invariants, run against a frozen system. See the second
rule.

`terminate(State)` — tear down. Called however the run ends, including on
violation.

`labels(State)` — **optional, and the fallback rather than the main road.** A
`#{pid() => Name}` map, used when `eta_log:analyze/0` renders a run. Without a
name a step reads `p7`; with one, `participant-2`.

Prefer `?ETA_LABEL(Name)` in the process itself. A process that labels itself is
named in one place, so its step lines and its own events cannot disagree, and a
self-reported name wins here. What this callback is for is the processes that
*cannot* name themselves: something from a library you do not own, or a module
you deliberately kept `eta.hrl` out of. `eta_2pc`'s clients are the example —
they are anonymous funs handed to `eta_run:spawn_op/1`, so the harness is the
only thing that knows what each one is.

Called **once**, after the run, which is why it takes the whole state rather
than one pid at a time: a harness already holds its processes in lists and maps,
so building the mapping forwards is a comprehension, while answering "what is
this pid called" one at a time means writing reverse lookups you otherwise would
not need.

Name whatever you care about and leave the rest out; anything absent falls back
to its position. Names are ordinary terms and `eta_log` renders them: an atom
becomes itself, `{Kind, N}` becomes `kind-n`, anything else falls back to `~p`.
""".
-endif.

-export_type([state/0, op/0, violation/0]).

-type state() :: term().
-type op() :: term().
-type violation() :: term().

-callback init(Seed :: integer(), Config :: map()) -> {ok, state()}.
-callback processes(state()) -> [pid()].
-callback generate(state(), rand:state()) -> {op(), rand:state()}.
-callback execute(op(), state()) -> state().
-callback check(state()) -> ok | {violation, violation()}.
-callback terminate(state()) -> ok.
-callback labels(state()) -> #{pid() => term()}.

-optional_callbacks([labels/1]).
