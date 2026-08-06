-module(eta_statem_sut).
-behaviour(gen_statem).

%% Every `gen_statem` start form the transform rewrites, called through the
%% transform so the test exercises the rewrite rather than `eta_sched` directly —
%% the counterpart of the `gen_server` starts in `eta_spawn_sut`.
%%
%% Written in `state_functions` mode, which is the other half of the reply pass:
%% the callbacks are named by the states, so there is no fixed list of names to
%% wrap and the pass has to work the set out from the export list and the
%% callback mode.

-include_lib("eta/include/eta.hrl").

-export([start_link/1, start_link_named/2, start_named/2, start_monitor_named/2]).
-export([who/1, state/1, flip/1, stop/1]).
-export([callback_mode/0, init/1, off/3, on/3, terminate/3]).

start_link(Owner) -> gen_statem:start_link(?MODULE, Owner, []).
start_link_named(Name, Owner) -> gen_statem:start_link(Name, ?MODULE, Owner, []).
start_named(Name, Owner) -> gen_statem:start(Name, ?MODULE, Owner, []).
start_monitor_named(Name, Owner) -> gen_statem:start_monitor(Name, ?MODULE, Owner, []).

who(Dest) -> gen_statem:call(Dest, who).
state(Dest) -> gen_statem:call(Dest, state).
flip(Dest) -> gen_statem:cast(Dest, flip).
stop(Dest) -> gen_statem:stop(Dest).

callback_mode() ->
    state_functions.

init(Owner) ->
    ok = ?ETA_LABEL(statem),
    {ok, off, #{owner => Owner, flips => 0}}.

off({call, From}, who, Data) ->
    {keep_state, Data, [{reply, From, self()}]};
off({call, From}, state, Data) ->
    {keep_state, Data, [{reply, From, off}]};
off(cast, flip, Data = #{flips := N}) ->
    {next_state, on, Data#{flips := N + 1}};
off(_Type, _Content, Data) ->
    {keep_state, Data}.

on({call, From}, who, Data) ->
    {keep_state, Data, [{reply, From, self()}]};
on({call, From}, state, Data) ->
    {keep_state, Data, [{reply, From, on}]};
on(cast, flip, Data = #{flips := N}) ->
    {next_state, off, Data#{flips := N + 1}};
on(_Type, _Content, Data) ->
    {keep_state, Data}.

%% The one exported arity-3 name in this module that is a callback and not a
%% state. The reply pass has to leave it alone.
terminate(_Reason, _State, _Data) ->
    ok.
