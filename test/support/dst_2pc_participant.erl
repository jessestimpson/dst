-module(dst_2pc_participant).

-include_lib("dst/include/dst.hrl").
-behaviour(gen_server).

%% A two-phase commit participant. Half of the second system under test for the
%% DST framework — see `dst_2pc`.
%%
%% All durable-ish state goes in a shared ETS table rather than the process
%% dictionary or gen_server state, because the invariants have to be readable
%% while every one of these processes is suspended. See `dst_harness` on why an
%% invariant that asks a process anything is an invariant that checks nothing.

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2]).

-export([plan/3, state/3, decisions/2]).

start_link(Tab, Index) ->
    gen_server:start_link(?MODULE, {Tab, Index}, []).

init({Tab, Index}) ->
    %% Names this process for `dst_log`. One call, at startup, and every event it
    %% records afterwards is attributed without any API having to carry a label.
    ok = ?DST_LABEL({participant, Index}),
    {ok, #{tab => Tab, index => Index}}.

handle_call(_Request, _From, St) ->
    {reply, ok, St}.

handle_cast({prepare, TxId, Coordinator}, St = #{tab := Tab, index := Index}) ->
    case plan(Tab, TxId, Index) of
        stall ->
            %% Votes are never sent. The coordinator's prepare timeout is the only
            %% thing that can resolve this transaction, which is what puts the
            %% virtual clock on the critical path of a run.
            _ = ?DST_LOG({stalling, TxId}),
            {noreply, St};
        Vote ->
            record(Tab, TxId, Index, {prepared, Vote}),
            _ = ?DST_LOG({voted, TxId, Vote}),
            gen_server:cast(Coordinator, {vote, TxId, Index, Vote}),
            {noreply, St}
    end;
handle_cast({decision, TxId, Decision}, St = #{tab := Tab, index := Index}) ->
    %% A participant that voted `no` does not commit, whatever it is told — that
    %% is what the vote meant. This is the only reason a broken coordinator
    %% produces a *visible* atomicity violation rather than a quietly wrong
    %% decision every participant agrees on.
    Final =
        case {Decision, plan(Tab, TxId, Index)} of
            {commit, no} -> aborted;
            {commit, _} -> committed;
            {abort, _} -> aborted
        end,
    record(Tab, TxId, Index, Final),
    _ = ?DST_LOG({decided, TxId, Decision, Final}),
    {noreply, St}.

%% ---------------------------------------------------------------------------
%% Shared state
%% ---------------------------------------------------------------------------

%% What this participant will do when asked to prepare `TxId`, written by the
%% driver before the transaction starts so a run is reproducible.
plan(Tab, TxId, Index) ->
    case ets:lookup(Tab, {plan, TxId, Index}) of
        [{_, Vote}] -> Vote;
        [] -> yes
    end.

state(Tab, TxId, Index) ->
    case ets:lookup(Tab, {state, TxId, Index}) of
        [{_, S}] -> S;
        [] -> unknown
    end.

%% Every participant's final word on `TxId`, for the atomicity invariant.
decisions(Tab, TxId) ->
    [
        S
     || {{state, T, _Index}, S} <- ets:tab2list(Tab),
        T =:= TxId,
        S =:= committed orelse S =:= aborted
    ].

record(Tab, TxId, Index, S) ->
    ets:insert(Tab, {{state, TxId, Index}, S}).
