-module(eta_net_faults).
-behaviour(eta_harness).

%% Three simulated nodes under `eta_run`, with link faults in the workload.
%%
%% The point of driving faults from `generate/2` rather than from a test body is
%% that a partition or a node kill then *is* an operation: it lands at a point the
%% seed chose, it is recorded in the trace, and a replay puts it back in the same
%% place. That is the only way to say "the same seed produces an identical trace
%% with partitions, node kills and monitors in play" and mean it.
%%
%% Each node carries two processes, which is the arrangement `attach/2` exists
%% for:
%%
%% - a **member**, placed — it exchanges traffic with the members on the other
%%   nodes, and that traffic is what the policy may drop or delay;
%% - a **connector**, attached — it holds monitors and receives the node's link
%%   events, and its own traffic is never faulted.
%%
%% The invariant is the asymmetry `kill_node/2` promises: a process on another
%% simulated node never learns the true exit reason of a process that died with
%% its node. It sees `noconnection`, as it would across real distribution. A
%% single `killed` anywhere in the log is a violation.

-export([init/2, processes/1, generate/2, execute/2, check/1, terminate/1, labels/1]).

-define(NODES, 3).

%% ---------------------------------------------------------------------------
%% eta_harness
%% ---------------------------------------------------------------------------

init(_Seed, Config) ->
    N = maps:get(nodes, Config, ?NODES),
    Tab = ets:new(eta_net_faults, [public, ordered_set]),

    Members = [{I, start_proc(Tab, {member, I})} || I <- lists:seq(1, N)],
    Connectors = [{I, start_proc(Tab, {connector, I})} || I <- lists:seq(1, N)],

    ok = topology(Members, Connectors),
    ok = introduce(Tab, Members),

    {ok, #{
        tab => Tab,
        count => N,
        members => Members,
        connectors => Connectors,
        killed => []
    }}.

%% The order is part of the contract — ids are assigned in registration order —
%% so it is built the same way every time regardless of what has died.
processes(#{members := Members, connectors := Connectors}) ->
    [P || {_I, P} <- Members] ++ [P || {_I, P} <- Connectors].

labels(#{members := Members, connectors := Connectors}) ->
    maps:from_list(
        [{P, {member, I}} || {I, P} <- Members] ++
            [{P, {connector, I}} || {I, P} <- Connectors]
    ).

%% Mostly traffic, with faults rare enough that a run has something to fault.
%% `watch` is an operation rather than something done at startup because a
%% monitor retired by a `noconnection` DOWN has to be re-created, and a system
%% that never re-monitors cannot show that the second one works.
generate(#{count := N}, Rand0) ->
    {Roll, Rand1} = rand:uniform_s(Rand0),
    if
        Roll < 0.50 -> two(fun(A, B) -> {ping, A, B} end, N, Rand1);
        Roll < 0.70 -> one(fun(A) -> {watch, A} end, N, Rand1);
        Roll < 0.85 -> two(fun(A, B) -> {partition, A, B} end, N, Rand1);
        Roll < 0.95 -> two(fun(A, B) -> {heal, A, B} end, N, Rand1);
        true -> one(fun(A) -> {kill, A} end, N, Rand1)
    end.

execute({ping, A, B}, Sut) ->
    _ = eta_net_node:ping(member(A, Sut), member(B, Sut)),
    Sut;
execute({watch, A}, Sut) ->
    _ = eta_net_node:watch(member(A, Sut)),
    _ = eta_net_node:watch(connector(A, Sut)),
    Sut;
execute({partition, A, B}, Sut) ->
    ok = eta_net:partition(node_name(A), node_name(B), #{signal => nodedown}),
    Sut;
execute({heal, A, B}, Sut) ->
    ok = eta_net:heal_partition(node_name(A), node_name(B), #{signal => nodeup}),
    Sut;
execute({kill, A}, Sut = #{killed := Killed}) ->
    %% Idempotent by construction — a node with nothing left on it is killed
    %% again without complaint — but recorded anyway, since `check/1` wants to
    %% know whether the run ever reached the interesting operation at all.
    ok = eta_net:kill_node(node_name(A), #{signal => nodedown}),
    Sut#{killed := lists:usort([A | Killed])}.

%% The asymmetry `kill_node/2` exists to produce. Read straight out of ETS: an
%% invariant that asked a process anything would be asking a suspended one.
check(#{tab := Tab, members := Members, connectors := Connectors}) ->
    Names =
        [{member, I} || {I, _} <- Members] ++
            [{connector, I} || {I, _} <- Connectors],
    first_violation(Names, Tab).

terminate(Sut = #{tab := Tab}) ->
    lists:foreach(fun stop/1, processes(Sut)),
    catch ets:delete(Tab),
    ok.

%% ---------------------------------------------------------------------------
%% Internals
%% ---------------------------------------------------------------------------

%% Both processes of a node are located on it; only the member is on the wire.
%% The connector's sends model something that is not a message, so faulting them
%% would inject a failure the real system cannot have — but it still has to die
%% with its node and hear its node's events, which is exactly `attach/2`.
topology(Members, Connectors) ->
    case eta_net:running() of
        false ->
            ok;
        true ->
            lists:foreach(
                fun({I, Pid}) -> ok = eta_net:place(node_name(I), [Pid]) end,
                Members
            ),
            lists:foreach(
                fun({I, Pid}) -> ok = eta_net:attach(node_name(I), [Pid]) end,
                Connectors
            )
    end.

%% Everybody watches the members on the *other* nodes, so every monitor in the
%% run crosses a link. Published to the table rather than sent: `init/2` has to
%% return with the system quiescent, and a message queued here would be consumed
%% at a moment wall clock chose rather than one the schedule did.
introduce(Tab, Members) ->
    lists:foreach(
        fun({I, _Pid}) ->
            Peers = [P || {J, P} <- Members, J =/= I],
            ok = eta_net_node:set_peers(Tab, {member, I}, Peers),
            ok = eta_net_node:set_peers(Tab, {connector, I}, Peers)
        end,
        Members
    ),
    ok.

first_violation([], _Tab) ->
    ok;
first_violation([Name | Rest], Tab) ->
    Events = eta_net_node:events(Tab, Name),
    case [R || {down, R} <- Events, R =/= noconnection, R =/= noproc] of
        [] ->
            first_violation(Rest, Tab);
        [Reason | _] ->
            {violation, #{
                property => remote_monitors_see_noconnection,
                detail => <<
                    "a process on another simulated node learned the real exit reason "
                    "of a process that died with its node; real distribution reports "
                    "noconnection"
                >>,
                who => Name,
                reason => Reason,
                events => Events
            }}
    end.

start_proc(Tab, Name) ->
    {ok, Pid} = eta_net_node:start(Tab, Name),
    Pid.

member(I, #{members := Members}) -> proplists:get_value(I, Members).
connector(I, #{connectors := Connectors}) -> proplists:get_value(I, Connectors).

node_name(I) -> list_to_atom("node_" ++ integer_to_list(I)).

%% Two distinct node indexes, so a partition never names one node twice.
two(Make, N, Rand0) ->
    {A, Rand1} = rand:uniform_s(N, Rand0),
    {Off, Rand2} = rand:uniform_s(N - 1, Rand1),
    {Make(A, 1 + ((A - 1 + Off) rem N)), Rand2}.

one(Make, N, Rand0) ->
    {A, Rand1} = rand:uniform_s(N, Rand0),
    {Make(A), Rand1}.

stop(Pid) ->
    catch gen_server:stop(Pid),
    ok.
