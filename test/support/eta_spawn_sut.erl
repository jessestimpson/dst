%% Every spawn and start form the transform rewrites, called through the
%% transform so the test exercises the rewrite rather than `eta_sched` directly.
%%
%% Each function reports the pid it created back to the caller, so a test can
%% check the child was gated: a gated child cannot run until the scheduler sends
%% it a token, so before adoption it must still be alive and not yet have done
%% anything.
-module(eta_spawn_sut).

-include_lib("eta/include/eta.hrl").

-export([
    plain/1,
    plain_mfa/1,
    linked/1,
    linked_mfa/1,
    monitored/1,
    monitored_mfa/1,
    opted/1,
    opted_mfa/1,
    bare/1,
    bare_opt/1,
    plib/1,
    plib_mfa/1,
    plib_linked/1,
    plib_opted/1,
    plib_opted_mfa/1,
    announce/1
]).
-export([start_link/1, start_link_named/2, start_named/2, start_monitor_named/2]).
-export([remote/2, remote_mfa/2, remote_link/2, remote_opt/2, plib_remote/2]).
-export([say/1, say_fmt/2, say_report/1, say_level/2]).
-export([init/1, handle_call/3, handle_cast/2]).

%% What every spawned child runs. Sends its own pid so the caller can prove the
%% child actually ran, and when.
announce(To) ->
    To ! {ran, self()},
    ok.

plain(To) -> erlang:spawn(fun() -> announce(To) end).
plain_mfa(To) -> erlang:spawn(?MODULE, announce, [To]).
linked(To) -> erlang:spawn_link(fun() -> announce(To) end).
linked_mfa(To) -> erlang:spawn_link(?MODULE, announce, [To]).
monitored(To) -> erlang:spawn_monitor(fun() -> announce(To) end).
monitored_mfa(To) -> erlang:spawn_monitor(?MODULE, announce, [To]).
opted(To) -> erlang:spawn_opt(fun() -> announce(To) end, []).
opted_mfa(To) -> erlang:spawn_opt(?MODULE, announce, [To], [monitor]).

%% Unqualified, so these go through the auto-imported-BIF path rather than the
%% remote-call path.
bare(To) -> spawn_monitor(fun() -> announce(To) end).
bare_opt(To) -> spawn_opt(fun() -> announce(To) end, []).

plib(To) -> proc_lib:spawn(fun() -> announce(To) end).
plib_mfa(To) -> proc_lib:spawn(?MODULE, announce, [To]).
plib_linked(To) -> proc_lib:spawn_link(fun() -> announce(To) end).
plib_opted(To) -> proc_lib:spawn_opt(fun() -> announce(To) end, []).
plib_opted_mfa(To) -> proc_lib:spawn_opt(?MODULE, announce, [To], []).

%% The gen_server starts, 3-arity and 4-arity.
start_link(Owner) -> gen_server:start_link(?MODULE, Owner, []).
start_link_named(Name, Owner) -> gen_server:start_link(Name, ?MODULE, Owner, []).
start_named(Name, Owner) -> gen_server:start(Name, ?MODULE, Owner, []).
start_monitor_named(Name, Owner) -> gen_server:start_monitor(Name, ?MODULE, Owner, []).

%% The distributed forms. Every one of these must raise while a run is active.
remote(Node, To) -> erlang:spawn(Node, fun() -> announce(To) end).
remote_mfa(Node, To) -> erlang:spawn(Node, ?MODULE, announce, [To]).
remote_link(Node, To) -> erlang:spawn_link(Node, fun() -> announce(To) end).
remote_opt(Node, To) -> erlang:spawn_opt(Node, fun() -> announce(To) end, []).
plib_remote(Node, To) -> proc_lib:spawn(Node, fun() -> announce(To) end).

%% Ordinary logging, which the transform routes into `eta_log`.
say(Msg) -> logger:info(Msg).
say_fmt(Fmt, Args) -> logger:warning(Fmt, Args).
say_report(Report) -> logger:error(Report).
say_level(Level, Msg) -> logger:log(Level, Msg).

init(Owner) ->
    ok = ?ETA_LABEL(server),
    {ok, Owner}.

handle_call(who, _From, Owner) ->
    {reply, self(), Owner}.

handle_cast(_Msg, Owner) ->
    {noreply, Owner}.
