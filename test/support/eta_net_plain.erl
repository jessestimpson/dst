-module(eta_net_plain).
-behaviour(gen_server).

%% A gen_server **without** `eta.hrl`, and therefore without the transform.
%%
%% It exists to be called. Its `handle_call/3` returns `{reply, ...}` the ordinary
%% way, so `gen_server` answers through `gen:reply/2` — a raw send from inside OTP
%% that no rewrite reaches. That is the one case `eta_net:call/3` cannot fix from
%% the calling side, so it detects it instead; this module is what makes the
%% detection testable.

-export([start_link/1, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [], []).

stop(Name) ->
    gen_server:stop(Name).

init([]) ->
    {ok, #{}}.

handle_call({echo, V}, _From, State) ->
    {reply, {echoed, V}, State};
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.
