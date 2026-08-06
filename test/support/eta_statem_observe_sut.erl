%% `-eta_observe` against a `gen_statem`, where the observed term is the **data**
%% rather than the state name.
%%
%% The `gen_server` return shapes carry the state in a position the leading tag
%% fixes, and so do the state machine's — but they are different shapes and a
%% different set of callbacks produces them. This module is what proves the pass
%% reads the right ones.
-module(eta_statem_observe_sut).
-behaviour(gen_statem).

-include_lib("eta/include/eta.hrl").
-eta_observe({st, [epoch, leader]}).

-export([start_link/0, bump/1, get/1, stop/1]).
-export([callback_mode/0, init/1, handle_event/4]).

-record(st, {
    epoch = 0 :: non_neg_integer(),
    leader = undefined :: term(),
    %% Declared but never observed, as in `eta_observe_sut`.
    scratch = [] :: [term()]
}).

start_link() ->
    gen_statem:start_link(?MODULE, [], []).

bump(Pid) ->
    gen_statem:cast(Pid, bump).

get(Pid) ->
    gen_statem:call(Pid, get).

stop(Pid) ->
    gen_statem:stop(Pid).

callback_mode() ->
    handle_event_function.

init([]) ->
    {ok, ready, #st{}}.

handle_event({call, From}, get, ready, St = #st{epoch = E}) ->
    {keep_state, St, [{reply, From, E}]};
handle_event(cast, bump, ready, St = #st{epoch = E}) ->
    {next_state, ready, St#st{
        epoch = E + 1,
        leader = {member, E + 1},
        scratch = [E | St#st.scratch]
    }};
handle_event(_Type, _Content, _State, St) ->
    {keep_state, St}.
