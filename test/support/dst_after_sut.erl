-module(dst_after_sut).

%% A system under test for `dst_after_transform`, in the same spirit as
%% `dst_timer_sut` is for `dst_transform`: compiled with the transform
%% unconditionally, because being rewritten is its entire purpose.
%%
%% Every function here is one property of the rewrite. `test/dst_after_test.exs`
%% drives them; `loop_control/1` in that file's helper module is the untransformed
%% twin of `loop/1`, which is the only way to state the tail-position property.
-compile({parse_transform, dst_after_transform}).

-export([
    wait/1,
    poll/0,
    poll_var/1,
    forever/0,
    exports/1,
    nested/2,
    escaping/1,
    loop/1
]).

%% The plain case: a timeout that must become virtual.
wait(Timeout) ->
    receive
        {msg, X} -> {got, X}
    after Timeout -> timed_out
    end.

%% A literal `after 0` — a mailbox poll, which the transform must leave alone.
poll() ->
    receive
        {msg, X} -> {got, X}
    after 0 -> empty
    end.

%% The same poll behind a variable, so the transform cannot see the zero and
%% `dst_time:arm_after/2` has to handle it.
poll_var(Timeout) ->
    receive
        {msg, X} -> {got, X}
    after Timeout -> empty
    end.

forever() ->
    receive
        {msg, X} -> {got, X}
    after infinity -> impossible
    end.

%% Erlang exports variables bound in *all* branches of a receive, including the
%% after body. Moving that body into a clause must not change it.
exports(Timeout) ->
    receive
        {msg, X} -> ok
    after Timeout -> X = defaulted, ok
    end,
    X.

nested(Outer, Inner) ->
    receive
        {msg, X} -> {got, X}
    after Outer ->
        In =
            receive
                {inner, Y} -> {inner_got, Y}
            after Inner -> inner_timed_out
            end,
        {outer_timed_out, In}
    end.

%% A non-local exit from the after body skips whatever follows the receive.
escaping(Timeout) ->
    receive
        {msg, X} -> {got, X}
    after Timeout -> exit(deadline)
    end.

%% Recursion from inside a receive clause, with the receive in tail position of
%% the function — the standard process-loop idiom. Reports its own stack so a test
%% can compare against the untransformed twin.
loop(0) ->
    element(2, erlang:process_info(self(), stack_size));
loop(N) ->
    receive
        {msg, _} -> loop(N - 1)
    after 60000 -> timed_out
    end.
