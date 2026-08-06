-module(eta_statem_plain).
-behaviour(gen_statem).

%% A `gen_statem` **without** `eta.hrl`, and therefore without the transform —
%% the counterpart of `eta_net_plain`.
%%
%% Its reply action is handled inside `gen_statem`, which sends it through
%% `gen:reply/2`: a raw send from inside OTP that no rewrite reaches. That is the
%% one case `eta_net:call/3` cannot fix from the calling side, so it detects it
%% instead, and this module is what makes the detection testable for a state
%% machine.

-export([start_link/1, stop/1]).
-export([callback_mode/0, init/1, handle_event/4]).

start_link(Name) ->
    gen_statem:start_link({local, Name}, ?MODULE, [], []).

stop(Name) ->
    gen_statem:stop(Name).

callback_mode() ->
    handle_event_function.

init([]) ->
    {ok, ready, #{}}.

handle_event({call, From}, {echo, V}, ready, Data) ->
    {keep_state, Data, [{reply, From, {echoed, V}}]};
handle_event(_Type, _Content, _State, Data) ->
    {keep_state, Data}.
