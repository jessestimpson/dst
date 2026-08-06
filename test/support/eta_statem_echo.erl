-module(eta_statem_echo).
-behaviour(gen_statem).

%% The `gen_statem` counterpart of `eta_net_echo`, in `handle_event_function`
%% mode.
%%
%% It sends every way the network pass rewrites — `!`, `erlang:send/2`,
%% `gen_statem:cast/2`, and a reply both ways a state machine can produce one: as
%% a `{reply, From, Msg}` action and through `gen_statem:reply/1,2`. A pass that
%% caught the actions and not the explicit calls, or the other way round, would
%% produce exactly the half-routed channel `eta_net` warns about.

-include_lib("eta/include/eta.hrl").

-export([start_link/1, bang/2, qualified/2, cast/2, call/2, call_dirty/2, async/2, stop/1]).
-export([callback_mode/0, init/1, handle_event/4]).

start_link(Name) ->
    gen_statem:start_link({local, Name}, ?MODULE, [], []).

%% `Dest ! Msg`, the operator form.
bang(Dest, Msg) ->
    Dest ! Msg.

%% The same thing named as a function.
qualified(Dest, Msg) ->
    erlang:send(Dest, Msg).

cast(Dest, Msg) ->
    gen_statem:cast(Dest, Msg).

call(Dest, Msg) ->
    gen_statem:call(Dest, Msg, 5000).

%% `gen_statem:call/3` also takes `{clean_timeout, T}` and `{dirty_timeout, T}`,
%% which `gen_server:call/3` does not. Both name how the *caller* waits, and a
%% routed call waits in a receive of its own — so both have to reach the same
%% place as a plain number.
call_dirty(Dest, Msg) ->
    gen_statem:call(Dest, Msg, {dirty_timeout, 5000}).

%% One of the client-side functions eta_net does not implement. Rewritten to
%% `eta_net:unsupported/2`, which raises while a network is running.
async(Dest, Msg) ->
    gen_statem:send_request(Dest, Msg).

stop(Name) ->
    gen_statem:stop(Name).

callback_mode() ->
    handle_event_function.

init([]) ->
    {ok, ready, #{}}.

%% Answered by an action, which is the ordinary way and the one `gen_server` has
%% no equivalent of.
handle_event({call, From}, {echo, V}, ready, Data) ->
    {keep_state, Data, [{reply, From, {echoed, V}}]};
%% Answered out of band, so the reply goes through a call the transform rewrites
%% rather than through the action list.
handle_event({call, From}, {defer, V}, ready, Data) ->
    gen_statem:reply(From, {deferred, V}),
    {keep_state, Data};
%% The single-argument form, which takes a reply action rather than a `From`.
handle_event({call, From}, {defer_one, V}, ready, Data) ->
    gen_statem:reply({reply, From, {deferred, V}}),
    {keep_state, Data};
%% Replying on the way out. The action list is `Replies` here rather than
%% `Actions`, and it has to be routed too.
handle_event({call, From}, {bye, V}, ready, _Data) ->
    {stop_and_reply, normal, [{reply, From, {bye, V}}]};
%% An action list carrying something other than a reply, so a test can check the
%% rest of the list survives the rewrite. The `next_event` is the observable
%% part: it only happens if `gen_statem` was handed the action after the reply
%% was taken out of the list.
handle_event({call, From}, {echo_note, V}, ready, Data) ->
    {keep_state, Data, [{reply, From, {echoed, V}}, {next_event, internal, {note, V}}]};
handle_event(internal, {note, V}, ready, Data) ->
    {keep_state, Data#{notes => [V | maps:get(notes, Data, [])]}};
handle_event({call, From}, notes, ready, Data) ->
    {keep_state, Data, [{reply, From, lists:reverse(maps:get(notes, Data, []))}]};
%% Forwards whatever it is cast to the process named in the message, so a test can
%% observe a cast crossing the network rather than only its effect here.
handle_event(cast, {forward, To, Msg}, ready, Data) ->
    To ! Msg,
    {keep_state, Data};
handle_event(_Type, _Content, _State, Data) ->
    {keep_state, Data}.
