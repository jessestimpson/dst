-module(eta_net_echo).
-behaviour(gen_server).

%% A minimal transformed module, for testing that `eta_transform`'s network pass
%% actually points a module's sends at `eta_net`.
%%
%% It sends four ways — the `!` operator, `erlang:send/2`, `gen_server:cast/2` and
%% a deferred `gen_server:reply/2` — because those are the four rewrites, and a
%% pass that catches three of them produces exactly the half-routed channel
%% `eta_net` warns about.

-include_lib("eta/include/eta.hrl").

-export([start_link/1, bang/2, qualified/2, cast/2, call/2, broadcast/2, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [], []).

%% `Dest ! Msg`, the operator form.
bang(Dest, Msg) ->
    Dest ! Msg.

%% The same thing named as a function.
qualified(Dest, Msg) ->
    erlang:send(Dest, Msg).

cast(Dest, Msg) ->
    gen_server:cast(Dest, Msg).

%% Answered out of band with `gen_server:reply/2` rather than by returning
%% `{reply, ...}`, so the reply goes through a call the transform can see.
%%
%% The call itself is rewritten too — to `eta_net:call/3`, which refuses when the
%% network would fault it. See `eta_net:call/3`.
call(Dest, Msg) ->
    gen_server:call(Dest, Msg, 5000).

%% One of the client-side functions eta_net does not implement. Rewritten to
%% `eta_net:unsupported/2`, which raises while a network is running.
broadcast(Name, Msg) ->
    gen_server:abcast(Name, Msg).

stop(Name) ->
    gen_server:stop(Name).

init([]) ->
    {ok, #{}}.

handle_call({echo, V}, From, State) ->
    gen_server:reply(From, {echoed, V}),
    {noreply, State};
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

%% Forwards whatever it is cast to the process named in the message, so a test can
%% observe a cast crossing the network rather than only its effect here.
handle_cast({forward, To, Msg}, State) ->
    To ! Msg,
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.
