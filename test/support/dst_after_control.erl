-module(dst_after_control).

%% The untransformed twin of `dst_after_sut`. Deliberately carries no
%% `parse_transform` attribute: several properties of the rewrite can only be
%% stated as a comparison against the code the compiler would otherwise produce.
%%
%% `loop/1` is the important one. That the transformed version runs in constant
%% stack means nothing on its own — it has to be the *same* constant as this.

-export([wait/1, loop/1]).

wait(Timeout) ->
    receive
        {msg, X} -> {got, X}
    after Timeout -> timed_out
    end.

loop(0) ->
    element(2, erlang:process_info(self(), stack_size));
loop(N) ->
    receive
        {msg, _} -> loop(N - 1)
    after 60000 -> timed_out
    end.
