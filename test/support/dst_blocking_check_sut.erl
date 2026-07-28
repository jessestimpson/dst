-module(dst_blocking_check_sut).
-behaviour(dst_sut).

%% A system under test whose `check/1` does the wrong thing on purpose: it asks a
%% scheduled process a question. Every process the scheduler owns is suspended
%% between steps, so the call cannot be served.
%%
%% This exists so the framework's diagnosis of that mistake is itself tested. The
%% mistake is an easy one — asking a process for its state is what an invariant
%% naturally wants to do — and without `dst_run` bounding the call it presents as
%% a run that never returns.
%%
%% Note what this does *not* protect against, and cannot: a client API that
%% catches its own call timeout and answers plausibly. A `status/1` that returns
%% `undefined` on `exit:_` leaves an invariant built on it computing over
%% "no members believe they lead" and pass, having checked nothing. Only reading
%% state out of band avoids that. See `dst_sut`.

-export([init/2, processes/1, generate/2, execute/2, check/1, terminate/1]).

init(_Seed, _Config) ->
    Pid = spawn(fun idle/0),
    {ok, #{pid => Pid}}.

processes(#{pid := Pid}) ->
    [Pid].

generate(_Sut, Rand) ->
    {noop, Rand}.

execute(noop, Sut) ->
    Sut.

check(#{pid := Pid}) ->
    Pid ! {question, self()},
    receive
        {answer, _} -> ok
    end.

terminate(#{pid := Pid}) ->
    exit(Pid, kill),
    ok.

idle() ->
    receive
        {question, From} ->
            From ! {answer, fine},
            idle()
    end.
