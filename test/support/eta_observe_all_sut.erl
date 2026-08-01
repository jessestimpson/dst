%% `-eta_observe(all)` against a state that is not a record at all.
%%
%% `all` used to be guarded on a record named `state`, so this module would have
%% published nothing and said nothing about it. It publishes whatever the
%% callback returned now, which is what `all` should have meant all along.
-module(eta_observe_all_sut).
-behaviour(gen_server).

-include_lib("eta/include/eta.hrl").
-eta_observe(all).

-export([start_link/0, bump/1, get/1]).
-export([init/1, handle_call/3, handle_cast/2]).

start_link() ->
    gen_server:start_link(?MODULE, [], []).

bump(Pid) ->
    gen_server:cast(Pid, bump).

get(Pid) ->
    gen_server:call(Pid, get).

init([]) ->
    {ok, #{epoch => 0}}.

handle_call(get, _From, St) ->
    {reply, maps:get(epoch, St), St}.

handle_cast(bump, St = #{epoch := E}) ->
    {noreply, St#{epoch := E + 1}}.
