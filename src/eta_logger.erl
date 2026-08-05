-module(eta_logger).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
`logger` for a system under simulation — the same API, routed into `eta_log`.

## Why logging is a determinism hazard

A `logger` call does not just format a string. It hands the event to a handler,
and the default handler `logger_std_h` is a **process the scheduler does not
own**. Under load it switches from asynchronous to synchronous mode, at which
point a log call becomes a synchronous call into an OTP service process — the
same class of real-time dependency as `code_server`, and one of the ones
`docs/05` tells you to grep for. The handler then runs on the real scheduler and
makes the caller runnable again at a moment nothing chose.

So a chatty system under test can have its schedule decided by how busy its log
handler happens to be, and the symptom is a seed that mostly reproduces.

## What this does instead

While a run is collecting, a call lands in `eta_log` as a `{Level, Args}` event:
no process, no message, no blocking, and a sequence number from the same counter
as everything else. Your system's own logging becomes part of the narrative
`eta_log:analyze/0` prints, interleaved with the scheduler's decisions rather
than scrolling past in a separate stream.

With no run in progress it delegates to `logger` unchanged, so a module built
with the transform logs normally outside a simulation. That is the same bargain
`eta_time` makes with the real clock.

## What it does not do

It does not filter by level, consult `logger`'s configuration, or run formatters.
An event is recorded whatever the level, because the run's log is a record of
what happened rather than an operator's console, and deciding at record time what
a reader will want is how you lose the line that mattered.

*This documentation is LLM-generated. See the AI disclosure in `README.md`.*
""".
-endif.

-export([
    emergency/1,
    emergency/2,
    emergency/3,
    alert/1,
    alert/2,
    alert/3,
    critical/1,
    critical/2,
    critical/3,
    error/1,
    error/2,
    error/3,
    warning/1,
    warning/2,
    warning/3,
    notice/1,
    notice/2,
    notice/3,
    info/1,
    info/2,
    info/3,
    debug/1,
    debug/2,
    debug/3,
    log/2,
    log/3,
    log/4
]).

emergency(A1) -> emit(emergency, [A1]).
emergency(A1, A2) -> emit(emergency, [A1, A2]).
emergency(A1, A2, A3) -> emit(emergency, [A1, A2, A3]).
alert(A1) -> emit(alert, [A1]).
alert(A1, A2) -> emit(alert, [A1, A2]).
alert(A1, A2, A3) -> emit(alert, [A1, A2, A3]).
critical(A1) -> emit(critical, [A1]).
critical(A1, A2) -> emit(critical, [A1, A2]).
critical(A1, A2, A3) -> emit(critical, [A1, A2, A3]).
error(A1) -> emit(error, [A1]).
error(A1, A2) -> emit(error, [A1, A2]).
error(A1, A2, A3) -> emit(error, [A1, A2, A3]).
warning(A1) -> emit(warning, [A1]).
warning(A1, A2) -> emit(warning, [A1, A2]).
warning(A1, A2, A3) -> emit(warning, [A1, A2, A3]).
notice(A1) -> emit(notice, [A1]).
notice(A1, A2) -> emit(notice, [A1, A2]).
notice(A1, A2, A3) -> emit(notice, [A1, A2, A3]).
info(A1) -> emit(info, [A1]).
info(A1, A2) -> emit(info, [A1, A2]).
info(A1, A2, A3) -> emit(info, [A1, A2, A3]).
debug(A1) -> emit(debug, [A1]).
debug(A1, A2) -> emit(debug, [A1, A2]).
debug(A1, A2, A3) -> emit(debug, [A1, A2, A3]).

log(Level, A1) -> emit(Level, [A1]).
log(Level, A1, A2) -> emit(Level, [A1, A2]).
log(Level, A1, A2, A3) -> emit(Level, [A1, A2, A3]).

%% Recorded when a run is collecting, delegated when it is not. `eta_log:log/1`
%% is already inert without a table, but delegating explicitly is what keeps a
%% transformed module's logging visible outside a simulation.
emit(Level, Args) ->
    case eta_log:running() of
        true ->
            _ = eta_log:log({Level, Args}),
            ok;
        false ->
            apply(logger, Level, Args)
    end.
