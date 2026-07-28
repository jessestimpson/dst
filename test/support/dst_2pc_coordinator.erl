-module(dst_2pc_coordinator).
-behaviour(gen_server).

%% A two-phase commit coordinator. The other half of `dst_2pc`.
%%
%% Its prepare timeout must run on the virtual clock, or a run that depends on a
%% participant stalling would cost real seconds and stop being deterministic. Only
%% `dst_transform` is needed — the timeout is an `erlang:send_after/3`, not a
%% `receive ... after`, and a module must never carry two parse transforms (see
%% `dst_transform`'s module doc on why).
-compile({parse_transform, dst_transform}).

-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

%% Long enough that it never fires by accident, short enough to be interesting —
%% and free, because it is virtual.
-define(VOTE_TIMEOUT, 30000).

start_link(Tab, Participants, Mode) ->
    gen_server:start_link(?MODULE, {Tab, Participants, Mode}, []).

init({Tab, Participants, Mode}) ->
    {ok, #{tab => Tab, participants => Participants, mode => Mode, pending => #{}}}.

handle_call({run_tx, TxId}, From, St) ->
    #{participants := Participants, pending := Pending} = St,
    Self = self(),
    [gen_server:cast(P, {prepare, TxId, Self}) || {_Index, P} <- Participants],
    Timer = erlang:send_after(?VOTE_TIMEOUT, self(), {vote_timeout, TxId}),
    Tx = #{from => From, votes => #{}, timer => Timer},
    {noreply, St#{pending := Pending#{TxId => Tx}}}.

handle_cast({vote, TxId, Index, Vote}, St = #{pending := Pending}) ->
    case maps:find(TxId, Pending) of
        %% Already decided: a late vote, which `first_vote_wins` produces by
        %% construction. Dropping it is correct in both modes.
        error ->
            {noreply, St};
        {ok, Tx} ->
            on_vote(TxId, Index, Vote, Tx, St)
    end;
handle_cast(_Msg, St) ->
    {noreply, St}.

handle_info({vote_timeout, TxId}, St = #{pending := Pending}) ->
    case maps:is_key(TxId, Pending) of
        true -> {noreply, decide(TxId, abort, St)};
        false -> {noreply, St}
    end;
handle_info(_Msg, St) ->
    {noreply, St}.

%% ---------------------------------------------------------------------------
%% Deciding
%% ---------------------------------------------------------------------------

on_vote(TxId, _Index, Vote, _Tx, St = #{mode := first_vote_wins}) ->
    %% The planted defect. Deciding on the first vote to *arrive* is wrong for a
    %% reason that only an interleaving reveals: if a `no` arrives first the
    %% transaction aborts and everything agrees, so the bug is invisible. It shows
    %% only when a `yes` wins the race and a `no` follows — at which point the
    %% no-voter refuses to commit while everyone else already has.
    Decision =
        case Vote of
            yes -> commit;
            no -> abort
        end,
    {noreply, decide(TxId, Decision, St)};
on_vote(TxId, Index, Vote, Tx = #{votes := Votes}, St = #{participants := Ps, pending := Pending}) ->
    Votes1 = Votes#{Index => Vote},
    case maps:size(Votes1) =:= length(Ps) of
        false ->
            {noreply, St#{pending := Pending#{TxId => Tx#{votes := Votes1}}}};
        true ->
            Decision =
                case lists:all(fun(V) -> V =:= yes end, maps:values(Votes1)) of
                    true -> commit;
                    false -> abort
                end,
            {noreply, decide(TxId, Decision, St)}
    end.

decide(TxId, Decision, St = #{tab := Tab, participants := Ps, pending := Pending}) ->
    #{from := From, timer := Timer} = maps:get(TxId, Pending),
    _ = erlang:cancel_timer(Timer),
    ets:insert(Tab, {{tx, TxId}, Decision}),
    [gen_server:cast(P, {decision, TxId, Decision}) || {_Index, P} <- Ps],
    gen_server:reply(From, Decision),
    St#{pending := maps:remove(TxId, Pending)}.
