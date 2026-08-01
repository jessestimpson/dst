%% A `gen_server` whose state record is deliberately **not** called `state`.
%%
%% That is the whole point of the module. `-eta_observe` used to accept a bare
%% field list that assumed `#state{}`, so a record named anything else was a
%% compile error that named a record the module had never declared. The form is
%% gone; this proves the surviving one carries the name.
-module(eta_observe_sut).
-behaviour(gen_server).

-include_lib("eta/include/eta.hrl").
-eta_observe({st, [epoch, leader]}).

-export([start_link/0, bump/1, get/1]).
-export([init/1, handle_call/3, handle_cast/2]).

-record(st, {
    epoch = 0 :: non_neg_integer(),
    leader = undefined :: term(),
    %% Declared but never observed, so a `read/1` result proves the field list is
    %% honoured rather than the whole record being published.
    scratch = [] :: [term()]
}).

start_link() ->
    gen_server:start_link(?MODULE, [], []).

bump(Pid) ->
    gen_server:cast(Pid, bump).

get(Pid) ->
    gen_server:call(Pid, get).

init([]) ->
    {ok, #st{}}.

handle_call(get, _From, St = #st{epoch = E}) ->
    {reply, E, St}.

handle_cast(bump, St = #st{epoch = E}) ->
    {noreply, St#st{epoch = E + 1, leader = {member, E + 1}, scratch = [E | St#st.scratch]}}.
