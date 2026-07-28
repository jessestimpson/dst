-module(dst_2pc).
-behaviour(dst_sut).

%% Two-phase commit as a `dst_sut` — the framework's second system under test.
%%
%% It exists to answer one question: is `dst_run` a framework, or is it
%% one system's simulation with the names changed? So it deliberately shares
%% nothing with the registry the framework grew out of — no leader, no replication, no durable backend, a
%% different failure mode and a different invariant.
%%
%% It also carries a planted defect, selectable by config, because a framework
%% that has only ever been pointed at correct systems has not been shown to find
%% anything. `mode => first_vote_wins` makes the coordinator decide on the first
%% vote to arrive; whether that is a violation depends entirely on the
%% interleaving, which is exactly the kind of bug the scheduler exists to reach.
%%
%% What it exercises, beyond the obvious:
%%
%% - **Operations that block.** Each transaction is a client process making a
%%   synchronous `gen_server:call`, so the client sits in the selective receive on
%%   a monitor ref that `dst_sched` was built to handle, and the driver never
%%   waits on a suspended process.
%% - **Virtual time on the critical path.** A `stall` participant never votes, so
%%   only the coordinator's prepare timeout can resolve the transaction. Under a
%%   frozen clock the run would deadlock; it is the idle callback that unblocks it.

-export([init/2, processes/1, generate/2, execute/2, check/1, terminate/1]).

-define(PARTICIPANTS, 3).

%% ---------------------------------------------------------------------------
%% dst_sut
%% ---------------------------------------------------------------------------

init(Seed, Config) ->
    Tab = ets:new(dst_2pc, [public, set]),
    N = maps:get(participants, Config, ?PARTICIPANTS),
    Mode = maps:get(mode, Config, correct),

    Participants = [
        begin
            {ok, Pid} = dst_2pc_participant:start_link(Tab, Index),
            {Index, Pid}
        end
     || Index <- lists:seq(1, N)
    ],
    {ok, Coordinator} = dst_2pc_coordinator:start_link(Tab, Participants, Mode),

    {ok, #{
        tab => Tab,
        coordinator => Coordinator,
        participants => Participants,
        clients => [],
        next_tx => 1,
        seed => Seed,
        %% A fixed vote plan, if the caller wants one. Pinning the workload is how
        %% a test isolates the schedule as the only variable: same votes, different
        %% seed, and any change in outcome is the interleaving and nothing else.
        plan => maps:get(plan, Config, random)
    }}.

processes(#{coordinator := Coordinator, participants := Participants, clients := Clients}) ->
    [Coordinator | [P || {_Index, P} <- Participants]] ++ Clients.

%% A transaction, plus what each participant will do when asked to prepare it.
%% Drawn entirely from the supplied rand state, so the workload replays.
generate(#{participants := Participants, next_tx := TxId, plan := Fixed}, Rand0) when
    Fixed =/= random
->
    {{run_tx, TxId, lists:zip([I || {I, _} <- Participants], Fixed)}, Rand0};
generate(#{participants := Participants, next_tx := TxId}, Rand0) ->
    {Plan, Rand} = lists:foldl(
        fun({Index, _Pid}, {Acc, R0}) ->
            {Roll, R1} = rand:uniform_s(R0),
            Vote = vote_for(Roll),
            {[{Index, Vote} | Acc], R1}
        end,
        {[], Rand0},
        Participants
    ),
    {{run_tx, TxId, lists:reverse(Plan)}, Rand}.

%% Mostly yes, so transactions usually commit and the interesting cases are rare
%% enough to need the scheduler to find them rather than falling out of every run.
vote_for(Roll) when Roll < 0.70 -> yes;
vote_for(Roll) when Roll < 0.90 -> no;
vote_for(_Roll) -> stall.

execute({run_tx, TxId, Plan}, Sut = #{tab := Tab, coordinator := Coordinator}) ->
    [ets:insert(Tab, {{plan, TxId, Index}, Vote}) || {Index, Vote} <- Plan],

    %% Spawned, not called: the coordinator is suspended, so a synchronous call
    %% from here would never be answered. The client becomes another process for
    %% the scheduler to interleave, which is the point.
    %%
    %% `dst_run:spawn_op/1` rather than `spawn/1` — a plain spawn lets the client
    %% run before the scheduler owns it, and two of those race each other to the
    %% coordinator's mailbox. That produced two different traces from one seed.
    Client = dst_run:spawn_op(fun() ->
        Result = gen_server:call(Coordinator, {run_tx, TxId}, infinity),
        ets:insert(Tab, {{client, TxId}, Result})
    end),

    Sut#{clients := [Client | maps:get(clients, Sut)], next_tx := TxId + 1}.

%% Atomicity: no two participants may reach opposite conclusions about the same
%% transaction. Read entirely from ETS — nothing here sends a message or waits.
check(#{tab := Tab, next_tx := Next}) ->
    first_violation(lists:seq(1, Next - 1), Tab).

terminate(Sut = #{coordinator := Coordinator, participants := Participants, tab := Tab}) ->
    %% Clients first, and by killing rather than asking. `dst_run` has already
    %% released the scheduler by this point, so anything still in flight is running
    %% again — and a client that reaches its `ets:insert` after the table is gone
    %% crashes with a badarg that has nothing to do with the run.
    [exit(C, kill) || C <- maps:get(clients, Sut)],
    [stop(P) || {_Index, P} <- Participants],
    stop(Coordinator),
    catch ets:delete(Tab),
    ok.

%% ---------------------------------------------------------------------------
%% Internals
%% ---------------------------------------------------------------------------

first_violation([], _Tab) ->
    ok;
first_violation([TxId | Rest], Tab) ->
    case lists:usort(dst_2pc_participant:decisions(Tab, TxId)) of
        Mixed when length(Mixed) > 1 ->
            {violation, #{
                property => atomicity,
                detail => <<"participants disagreed about a transaction">>,
                tx => TxId,
                decision => coordinator_decision(Tab, TxId),
                participants => [
                    {Index, dst_2pc_participant:state(Tab, TxId, Index)}
                 || Index <- participant_indexes(Tab, TxId)
                ]
            }};
        _ ->
            first_violation(Rest, Tab)
    end.

coordinator_decision(Tab, TxId) ->
    case ets:lookup(Tab, {tx, TxId}) of
        [{_, D}] -> D;
        [] -> undecided
    end.

participant_indexes(Tab, TxId) ->
    lists:usort([Index || {{state, T, Index}, _} <- ets:tab2list(Tab), T =:= TxId]).

stop(Pid) ->
    catch gen_server:stop(Pid),
    ok.
