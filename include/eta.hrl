%% eta — the compile-time contract for a system under simulation.
%%
%%     -include_lib("eta/include/eta.hrl").
%%
%% Everything here is conditional on the `DST` define, which the simulation
%% build sets and a release build does not. That is the point: under a release
%% build this header contributes nothing at all — no parse transform, and no
%% calls into `eta` — so instrumentation can be left in the source of a module
%% that ships without affecting it.
%%
%% `eta_log:log/1` is inert at runtime when no collection is running, but inert
%% still costs a function call and a table lookup on every event, and it means a
%% release has to carry `eta` to satisfy the call. The macros cost nothing and
%% carry nothing.
%%
%% Same shape as EUnit's `TEST`: one define promotes a set of macros from
%% nothing into something, and the guideline becomes a contract.
%%
%% In Mix:
%%
%%     defp erlc_options(:test), do: [:debug_info, {:d, :DST}]
%%     defp erlc_options(_), do: [:debug_info]

-ifndef(ETA_HRL).
-define(ETA_HRL, true).

-ifdef(DST).

%% Rewrites timer, clock and spawn calls to point at `eta_time` and `eta_sched`,
%% and puts `receive ... after` on the virtual clock. Including this header is
%% how a module opts in; `-eta_after(false)` and `-eta_observe(...)` tune it.
%% See `eta_transform`.
-compile({parse_transform, eta_transform}).

%% Name the calling process for `eta_log`. Call once, where the process starts.
-define(ETA_LABEL(Label), eta_log:label(Label)).

%% Record an event, returning its sequence number. See `eta_log`.
-define(ETA_LOG(Event), eta_log:log(Event)).

-else.

%% Release build. `Event` and `Label` are **not evaluated**, so anything you log
%% must be free of side effects — and a variable used *only* inside one of these
%% will be reported unused. Bind it with a leading underscore, or log something
%% the function already needs.
-define(ETA_LABEL(Label), ok).
-define(ETA_LOG(Event), 0).

-endif.
-endif.
