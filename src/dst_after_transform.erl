-module(dst_after_transform).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
A `parse_transform` that puts `receive ... after` timeouts on the virtual clock.

`dst_transform` rewrites *calls* — `erlang:send_after/3`, `erlang:monotonic_time/1`
— which leaves the one timeout a transform was assumed not to be able to reach: the
`after` block of a receive. `after` is a language construct, so there is no call to
redirect, and Phase 5 was planned around working without it.

That assumption was wrong, and measurably so. `receive Cs after T -> B end` is the
five-element abstract form `{'receive', Anno, Cs, T, B}`: fully visible, and
rewritable into a form with no real-time dependence at all.

## The rewrite

```erlang
begin
    Ref  = make_ref(),
    TRef = dst_time:arm_after(T, Ref),
    receive
        Pat1 -> dst_time:disarm_after(TRef, Ref), Body1;
        ...
        {'$dst_after', G} when G =:= Ref -> B
    end
end
```

The `after` block becomes an ordinary clause, so the wait is on a message and the
virtual clock can deliver it. Under `dst_sched` the waiting process is correctly
unrunnable until then — it genuinely has nothing to do — and becomes runnable the
moment the timer fires. A 60-second timeout costs microseconds.

## Three things the obvious rewrite gets wrong

Each was found by running the transform, not by reading it; each looks like a
detail and is not.

**A zero timeout must not go through the timer wheel.** `after 0` is a mailbox
poll, not a wait — it never blocks. Route it through a timer and it becomes a wait
on something nothing may advance: measured at 500ms and a leaked timer where the
original cost nothing. `dst_time:arm_after/2` therefore delivers a zero timeout
straight to the caller's own mailbox, which also keeps the ordering right, since a
selective receive scans in arrival order and so still prefers anything queued.
That runtime handling is the load-bearing part — verified by removing it, which
fails both `after 0` tests. Skipping the rewrite when the literal `0` is visible
here is only an optimisation on top of it.

**The disarm goes at the head of each clause, not after the receive.** Saving the
receive's value to disarm afterwards forces a block whose last expression is that
saved value, which takes the receive out of tail position. For the standard
`loop() -> receive ... -> loop() end` idiom that converts constant stack into
unbounded growth: measured at 200,005 words over 50,000 iterations, against 5 for
the untransformed original. With the disarm in the clause heads a block is
transparent to tail position and the bodies stay exactly where they were — 5 words.
It also makes the disarm exception-safe for free, since it has already run before
any body that might throw.

**The timeout clause matches a fresh variable and compares in a guard.** Matching
the bound `Ref` directly is correct, but emits "variable is already bound" on every
transformed receive, which breaks any build using `--warnings-as-errors`.

## What it costs

A timer is created and cancelled even when a matching message was already waiting,
which the VM's native `after` does not pay. Two ETS operations per receive —
negligible for an occasional timeout, worth thinking about before putting it on a
hot receive loop.

Not yet applied to anything: adoption is a Phase 5 decision, and this
module exists so that decision can be made against measurements. See
`docs/design.md`.
""".
-endif.

-export([parse_transform/2]).

-spec parse_transform(Forms, [compile:option()]) -> Forms when Forms :: [erl_parse:abstract_form()].
parse_transform(Forms, _Options) ->
    {Forms1, _Counter} = walk(Forms, 0),
    Forms1.

%% Postorder walk of the abstract format, threading a counter so the variables
%% introduced for each rewritten receive are unique within the module.
walk({'receive', Anno, Clauses, {integer, _, 0} = Zero, AfterBody}, N) ->
    %% An optimisation, not a correctness guard — `dst_time:arm_after/2` handles a
    %% zero timeout correctly however it arrives. Skipping the rewrite when the
    %% zero is visible at compile time just saves a ref, a send and a flush on
    %% every poll, and `after 0` tends to sit on hot paths.
    {Clauses1, N1} = walk(Clauses, N),
    {AfterBody1, N2} = walk(AfterBody, N1),
    {{'receive', Anno, Clauses1, Zero, AfterBody1}, N2};
walk({'receive', Anno, Clauses, Timeout, AfterBody}, N) ->
    {Clauses1, N1} = walk(Clauses, N),
    {Timeout1, N2} = walk(Timeout, N1),
    {AfterBody1, N3} = walk(AfterBody, N2),
    {expand(Anno, Clauses1, Timeout1, AfterBody1, N3), N3 + 1};
walk(Tuple, N) when is_tuple(Tuple) ->
    {List, N1} = walk(tuple_to_list(Tuple), N),
    {list_to_tuple(List), N1};
walk(List, N) when is_list(List) ->
    lists:mapfoldl(fun walk/2, N, List);
walk(Other, N) ->
    {Other, N}.

expand(Anno, Clauses, Timeout, AfterBody, N) ->
    RefV = var(Anno, "__DstAfterRef", N),
    TRefV = var(Anno, "__DstAfterTRef", N),
    GotV = var(Anno, "__DstAfterGot", N),

    TimeoutClause =
        {clause, Anno, [{tuple, Anno, [{atom, Anno, '$dst_after'}, GotV]}],
            [[{op, Anno, '=:=', GotV, RefV}]], AfterBody},

    Disarm = call(Anno, dst_time, disarm_after, [TRefV, RefV]),
    Clauses1 = [prepend(Disarm, C) || C <- Clauses],

    {block, Anno, [
        {match, Anno, RefV, call(Anno, erlang, make_ref, [])},
        {match, Anno, TRefV, call(Anno, dst_time, arm_after, [Timeout, RefV])},
        {'receive', Anno, Clauses1 ++ [TimeoutClause]}
    ]}.

prepend(Expr, {clause, Anno, Pats, Guards, Body}) ->
    {clause, Anno, Pats, Guards, [Expr | Body]}.

var(Anno, Prefix, N) ->
    {var, Anno, list_to_atom(Prefix ++ integer_to_list(N))}.

call(Anno, Mod, Fun, Args) ->
    {call, Anno, {remote, Anno, {atom, Anno, Mod}, {atom, Anno, Fun}}, Args}.
