-module(eta_timer_sut).

%% A system under test for the Phase 1 virtual-clock tests.
%%
%% Compiled with the transform unconditionally (rather than behind `-ifdef(DST)`)
%% because it exists only to be simulated — its whole point is that its
%% `erlang:send_after/3` and `erlang:monotonic_time/1` calls land in `eta_time`.
%% Production modules guard the transform; see `eta_transform`'s module doc.
-compile({parse_transform, eta_transform}).

%% `both/2` carries both rewrites in one function — a rewritten *call* and a
%% rewritten *`after`*. That combination was impossible while the receive-timeout
%% rewrite lived in a second `parse_transform`, since Mix cannot reliably order a
%% module that names two.

-export([start/2, start_periodic/3, elapsed/1, both/2]).

%% A process that sets a one-shot timer and records when it fires, according to
%% the clock it can see.
start(Trace, DelayMs) ->
    spawn(fun() ->
        Started = erlang:monotonic_time(millisecond),
        erlang:send_after(DelayMs, self(), fired),
        loop(Trace, Started)
    end).

loop(Trace, Started) ->
    receive
        fired ->
            Now = erlang:monotonic_time(millisecond),
            ets:insert(Trace, {erlang:unique_integer([monotonic]), {fired, Now - Started}}),
            loop(Trace, Started);
        {ping, From} ->
            From ! {pong, erlang:monotonic_time(millisecond)},
            loop(Trace, Started)
    end.

%% A process that re-arms its timer N times — the shape of a heartbeat, and the
%% case where a real clock would make a test wait N * IntervalMs.
start_periodic(Trace, IntervalMs, Count) ->
    spawn(fun() ->
        erlang:send_after(IntervalMs, self(), tick),
        periodic(Trace, IntervalMs, Count)
    end).

periodic(_Trace, _IntervalMs, 0) ->
    receive
        stop -> ok
    end;
periodic(Trace, IntervalMs, Count) ->
    receive
        tick ->
            ets:insert(
                Trace,
                {erlang:unique_integer([monotonic]),
                    {tick, Count, erlang:monotonic_time(millisecond)}}
            ),
            %% Re-arm only while there are ticks left. Arming on the final tick
            %% too would leave a timer pending that nothing consumes, so the
            %% clock would end one interval past the last recorded tick — true to
            %% a real heartbeat, but it makes "how much time did N ticks take?"
            %% ambiguous in the tests below.
            case Count > 1 of
                true -> erlang:send_after(IntervalMs, self(), tick);
                false -> ok
            end,
            periodic(Trace, IntervalMs, Count - 1)
    end.

%% Reports the module's own view of elapsed time, to prove reads are virtualised
%% and not merely that timers fire.
elapsed(Since) ->
    erlang:monotonic_time(millisecond) - Since.

%% Every source of real time in one function. `erlang:monotonic_time/1` is
%% rewritten by the call pass, and the `receive ... after` by the timeout pass.
%% Nothing here can observe the wall clock, so the elapsed time it reports is
%% exactly the interval the driver advanced.
both(Trace, TimeoutMs) ->
    spawn(fun() ->
        Started = erlang:monotonic_time(millisecond),
        Result =
            receive
                {ping, From} -> From ! pong, got_message
            after TimeoutMs -> timed_out
            end,
        Now = erlang:monotonic_time(millisecond),
        ets:insert(Trace, {erlang:unique_integer([monotonic]), {Result, Now - Started}})
    end).
