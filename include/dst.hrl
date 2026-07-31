%% dst — the compile-time contract for a system under simulation.
%%
%%     -include_lib("dst/include/dst.hrl").
%%
%% Everything here is conditional on the `DST` define, which the simulation
%% build sets and a release build does not. That is the point: under a release
%% build this header contributes nothing at all — no parse transform, and no
%% calls into `dst` — so instrumentation can be left in the source of a module
%% that ships. `dst` is a test-only dependency, so a bare `dst_log:log/1` in a
%% shipped module would be an `undef` waiting to happen.
%%
%% Same shape as EUnit's `TEST`: one define promotes a set of macros from
%% nothing into something, and the guideline becomes a contract.
%%
%% In Mix:
%%
%%     defp erlc_options(:test), do: [:debug_info, {:d, :DST}]
%%     defp erlc_options(_), do: [:debug_info]

-ifndef(DST_HRL).
-define(DST_HRL, true).

-ifdef(DST).

%% Rewrites timer, clock and spawn calls to point at `dst_time` and `dst_sched`,
%% and puts `receive ... after` on the virtual clock. Including this header is
%% how a module opts in; `-dst_after(false)` and `-dst_observe(...)` tune it.
%% See `dst_transform`.
-compile({parse_transform, dst_transform}).

%% Name the calling process for `dst_log`. Call once, where the process starts.
-define(DST_ROLE(Role), dst_log:role(Role)).

%% Record an event, returning its sequence number. See `dst_log`.
-define(DST_LOG(Event), dst_log:log(Event)).

-else.

%% Release build. `Event` and `Role` are **not evaluated**, so anything you log
%% must be free of side effects — and a variable used *only* inside one of these
%% will be reported unused. Bind it with a leading underscore, or log something
%% the function already needs.
-define(DST_ROLE(Role), ok).
-define(DST_LOG(Event), 0).

-endif.
-endif.
