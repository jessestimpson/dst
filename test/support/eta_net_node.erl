-module(eta_net_node).
-behaviour(gen_server).

%% One process of a simulated node, for exercising `eta_net`'s link events.
%%
%% It is deliberately the shape of the system those events exist for: a peer that
%% talks to its counterparts on other nodes, watches them with monitors, and
%% hears about a node that has gone. Everything it records goes into an ETS table
%% the harness reads without asking it anything, since a suspended process cannot
%% answer.
%%
%% The monitor is written **bare** — `monitor(process, Peer)` rather than
%% `erlang:monitor/2` — because that is the form most Erlang is written in, and
%% it is the form `eta_transform` has to catch for a partition to be visible at
%% all.

-include_lib("eta/include/eta.hrl").

-export([start/2, set_peers/3, watch/1, ping/2, events/2, watching/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-record(st, {
    tab,
    name,
    %% peer => monitor ref. A monitor is created once and re-created only after
    %% it has fired, which is what a real system does with a terminal DOWN.
    watched = #{},
    seen = 0
}).

%% Started unlinked. A linked peer would take the driver down with it the first
%% time a node was killed, which is not a property of the system under test.
start(Tab, Name) ->
    gen_server:start(?MODULE, {Tab, Name}, []).

%% Peers are published to the table rather than sent, because this is called
%% while the harness is still building the system: a message here would sit in a
%% mailbox no scheduler owns yet and be consumed at a moment wall clock chose.
set_peers(Tab, Name, Peers) ->
    true = ets:insert(Tab, {{peers, Name}, Peers}),
    ok.

watch(Pid) -> gen_server:cast(Pid, watch).
ping(Pid, Peer) -> gen_server:cast(Pid, {ping, Peer}).

%% What the harness reads: every event this process saw, oldest first.
events(Tab, Name) ->
    [E || {{event, N, _Seq}, E} <- lists:sort(ets:tab2list(Tab)), N =:= Name].

watching(Tab, Name) ->
    case ets:lookup(Tab, {watching, Name}) of
        [{_, Peers}] -> Peers;
        [] -> []
    end.

init({Tab, Name}) ->
    ok = ?ETA_LABEL(Name),
    {ok, #st{tab = Tab, name = Name}}.

handle_call(_Req, _From, St) ->
    {reply, ok, St}.

handle_cast(watch, St = #st{tab = Tab, name = Name, watched = Watched}) ->
    Watched1 = lists:foldl(
        fun(Peer, Acc) ->
            case maps:is_key(Peer, Acc) of
                true -> Acc;
                false -> Acc#{Peer => monitor(process, Peer)}
            end
        end,
        Watched,
        peers_of(Tab, Name)
    ),
    {noreply, note_watching(St#st{watched = Watched1})};
handle_cast({ping, Peer}, St) ->
    gen_server:cast(Peer, {echo, self()}),
    {noreply, St};
handle_cast({echo, From}, St) ->
    gen_server:cast(From, pong),
    {noreply, St};
handle_cast(pong, St) ->
    {noreply, record(St, pong)};
handle_cast(_Msg, St) ->
    {noreply, St}.

%% A DOWN is terminal, so the peer comes out of the watch set and a later `watch`
%% monitors it again — which is what makes "no second DOWN for one cut" a
%% property this system can actually observe.
handle_info({'DOWN', Ref, process, _Pid, Reason}, St = #st{watched = Watched}) ->
    Watched1 = maps:filter(fun(_Peer, R) -> R =/= Ref end, Watched),
    {noreply, record(note_watching(St#st{watched = Watched1}), {down, Reason})};
handle_info({nodedown, Node}, St) ->
    {noreply, record(St, {nodedown, Node})};
handle_info({nodeup, Node}, St) ->
    {noreply, record(St, {nodeup, Node})};
handle_info(_Msg, St) ->
    {noreply, St}.

peers_of(Tab, Name) ->
    case ets:lookup(Tab, {peers, Name}) of
        [{_, Peers}] -> Peers;
        [] -> []
    end.

record(St = #st{tab = Tab, name = Name, seen = Seen}, Event) ->
    _ = ?ETA_LOG(Event),
    true = ets:insert(Tab, {{event, Name, Seen}, Event}),
    St#st{seen = Seen + 1}.

note_watching(St = #st{tab = Tab, name = Name, watched = Watched}) ->
    true = ets:insert(Tab, {{watching, Name}, lists:sort(maps:keys(Watched))}),
    St.
