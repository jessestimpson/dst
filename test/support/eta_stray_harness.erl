-module(eta_stray_harness).

%% Drives `eta_stray`. See that module for what is being reproduced and why.
%%
%% Carries no parse transform, which is the point made in `eta_harness`: a harness
%% needs no compile-time features, so it calls `eta_log` directly rather than
%% through the macros. It also means the real-time polling in `strand/1` stays on
%% the real clock, which it has to — `init/2` runs before the driver exists, so a
%% virtualised sleep here would wait on a clock nobody is advancing and never
%% return. That trap is the same one the receive in `eta_stray:await_release/1`
%% falls into, approached from the other side.
%%
%% `config.stray` selects what `init/2` leaves behind:
%%
%%   `none`    — nothing. The control.
%%   `dead`    — a process killed while blocked, so its deadline outlives it.
%%   `unowned` — a process left alive and blocked, which the scheduler never
%%               adopted because it was spawned before there was a scheduler.

-behaviour(eta_harness).

-export([init/2, processes/1, generate/2, execute/2, check/1, terminate/1, labels/1]).

-define(WORKERS, 3).

init(_Seed, Config) ->
    Workers = [
        begin
            {ok, P} = eta_stray:start_link(I),
            P
        end
     || I <- lists:seq(1, ?WORKERS)
    ],
    Stray = strand(maps:get(stray, Config, none)),
    {ok, #{workers => Workers, clients => [], next_op => 1, stray => Stray}}.

%% Leave the system in the state the run is supposed to start from.
%%
%% Every branch returns only once the waiter has actually armed, checked against
%% `eta_time:pending/0` rather than by sleeping a guessed interval — a race here
%% would make the reproduction itself nondeterministic, which would rather defeat
%% the purpose.
strand(none) ->
    undefined;
strand(Kind) ->
    Before = eta_time:pending(),
    Pid = eta_stray:spawn_waiter(eta_stray:deadline()),
    ok = await_armed(Before + 1, 500),
    case Kind of
        dead ->
            %% `disarm_after/2` runs at the head of a receive clause. A process
            %% killed before it reaches one never disarms, so the row stays.
            exit(Pid, kill),
            ok = await_dead(Pid, 500);
        unowned ->
            ok
    end,
    Pid.

await_armed(_N, 0) ->
    error(waiter_never_armed);
await_armed(N, Tries) ->
    case eta_time:pending() >= N of
        true -> ok;
        false -> timer:sleep(1), await_armed(N, Tries - 1)
    end.

await_dead(_Pid, 0) ->
    error(waiter_never_died);
await_dead(Pid, Tries) ->
    case is_process_alive(Pid) of
        false -> ok;
        true -> timer:sleep(1), await_dead(Pid, Tries - 1)
    end.

%% The workers and the client operations, and **deliberately not the stray**.
%%
%% That omission is the whole reproduction. A harness cannot list a process it
%% does not know about, and a system that spawns during startup produces exactly
%% those. Adding it here would not be a fix so much as a demonstration that the
%% problem is knowing which processes exist.
processes(#{workers := Workers, clients := Clients}) ->
    Workers ++ [C || C <- lists:reverse(Clients), is_process_alive(C)].

generate(#{next_op := N}, Rand0) ->
    {Index, Rand} = rand:uniform_s(?WORKERS, Rand0),
    {{tick, N, Index}, Rand}.

execute({tick, N, Index}, Sut = #{workers := Workers, clients := Clients}) ->
    Worker = lists:nth(Index, Workers),
    Client = eta_run:spawn_op(fun() ->
        ok = eta_log:label({client, N}),
        _ = eta_log:log({ticking, Index}),
        eta_stray:tick(Worker)
    end),
    Sut#{clients := [Client | Clients], next_op := N + 1}.

%% Nothing to assert. This system has no invariant to violate — the defect it
%% reproduces is in the *trace*, not in what the system computes, which is
%% precisely why it survived so long in the system it came from. `check/1`
%% returning `ok` forever is the honest statement of that.
check(_Sut) ->
    ok.

labels(#{workers := Workers}) ->
    maps:from_list([{P, {worker, I}} || {I, P} <- lists:enumerate(Workers)]).

terminate(#{workers := Workers, clients := Clients, stray := Stray}) ->
    [exit(C, kill) || C <- Clients],
    case Stray of
        undefined -> ok;
        Pid -> exit(Pid, kill)
    end,
    [eta_stray:stop(P) || P <- Workers, is_process_alive(P)],
    ok.
