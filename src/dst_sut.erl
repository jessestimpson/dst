-module(dst_sut).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
What a system under test provides to `dst_run` — Phase 3 of the DST framework
(design: `docs/design.md`).

Six callbacks. `dst_run` supplies the seed, owns the scheduler and the clock,
decides when to inject work and when to let the system make progress, and records
enough to replay the run exactly.

## Two rules that are not obvious, and both bite silently

**`execute/2` must not block.** Every process the scheduler owns is *suspended*
between steps, so a synchronous call into one from the driver cannot be served and
will sit there until something times out. An operation is issued by spawning a
process to perform it, and `dst_run` picks that process up through `processes/1`
and schedules it like any other. This is not a limitation of the framework so much
as what a driver is: the client is part of the system being interleaved.

**`check/1` must not call into a scheduled process either — and getting this wrong
does not fail loudly.** `dst_run` bounds the call and reports `check_blocked` if it
hangs, but the more likely outcome is worse. A client API that catches its own
timeout — a `status/1` that returns `undefined` on `exit:_` — turns a suspended
member into a plausible-looking answer, and an invariant computed over
"no members believe they lead" passes while checking nothing at all.

Read state out of band: ETS, `persistent_term`, a table the system keeps anyway.
Reading a registry's names table directly, the way its own lookups do, is the shape
to copy; anything that sends a message and waits is not.

## The callbacks

`init(Seed, Config)` — start the system and return with it **quiescent**. Anything
still running when `dst_run` takes over ran on the real scheduler, outside the
schedule, and that is exactly the nondeterminism the framework exists to remove.

`processes(Sut)` — every pid to schedule. Re-consulted after each operation, so
processes an operation creates are picked up; already-known pids are ignored, so
returning the full set every time is correct and expected.

`generate(Sut, Rand)` — the next operation, from the supplied `rand` state. Return
the advanced state; drawing entropy from anywhere else breaks replay.

`execute(Op, Sut)` — issue the operation. See the first rule above.

`check(Sut)` — the invariants, run against a frozen system. See the second rule.

`terminate(Sut)` — tear down. Called however the run ends, including on violation.
""".
-endif.

-export_type([sut/0, op/0, violation/0]).

-type sut() :: term().
-type op() :: term().
-type violation() :: term().

-callback init(Seed :: integer(), Config :: map()) -> {ok, sut()}.
-callback processes(sut()) -> [pid()].
-callback generate(sut(), rand:state()) -> {op(), rand:state()}.
-callback execute(op(), sut()) -> sut().
-callback check(sut()) -> ok | {violation, violation()}.
-callback terminate(sut()) -> ok.
