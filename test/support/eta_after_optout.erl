-module(eta_after_optout).

%% The escape hatch, and the only thing that tests it.
%%
%% `eta_transform` virtualises `receive ... after` by default, because a module
%% that quietly wakes on the wall clock under simulation is exactly the silent
%% failure the framework exists to remove. `-eta_after(false)` turns that pass
%% off for a module where the cost has been measured and found to matter.
%%
%% Everything else about the transform still applies here, which is the point:
%% opting out of one pass must not opt out of the others. `armed/1` proves it by
%% using a rewritten *call* in the same module whose `after` is left alone.
-compile({parse_transform, eta_transform}).
-eta_after(false).

-export([wait/1, armed/0]).

%% Left on the real clock by the opt-out.
wait(Timeout) ->
    receive
        {msg, V} -> {got, V}
    after Timeout -> timed_out
    end.

%% Still rewritten: the call pass is unconditional.
armed() ->
    erlang:send_after(60_000, self(), tick).
