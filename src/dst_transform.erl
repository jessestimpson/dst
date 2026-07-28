-module(dst_transform).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
A `parse_transform` that points a module's timer and clock calls at `dst_time` —
Phase 1 of the DST framework (design: `docs/design.md`).

Nothing else about the module changes, and the rewrite is a pure call-target
substitution: `erlang:send_after(T, D, M)` becomes `dst_time:send_after(T, D, M)`,
and so on for the table below.

## Two passes, one attribute

This module runs two passes, and a module opts into the second by declaring an
attribute rather than by naming a second transform:

1. **Timer and clock rewriting** — the table below, applied always.
2. **State observability** — applied only to a module declaring `-dst_observe(...)`,
   which republishes the named state fields on every `gen_server` callback return
   so a simulation can read them while the process is suspended. See `dst_observe`.

The second pass lives here rather than in a module of its own for a build reason,
and it is worth recording because it cost a broken CI run to find. **Mix does not
reliably order a module after more than one `parse_transform`.** With two
attributes on one module, an incremental build where both the transform and that
module changed failed with `undefined parse transform` roughly two times in
three — nondeterministically, so it looked intermittent before it was pinned down.
One attribute is ordered correctly; two are not.

Delegating from here to a separate module would not have helped: the compiler loads
a transform module from the code path when it runs, so `dst_transform` calling
`dst_observe_transform:parse_transform/2` has exactly the same ordering hazard one
level down. The passes have to be in the module the attribute names.

So: **never give a module two parse transforms.** If a module needs another pass,
add it here behind an attribute.

## Enabling it

Guard it so it is absent from ordinary builds:

```erlang
-ifdef(DST).
-compile({parse_transform, dst_transform}).
-endif.
```

and compile the simulation build with `{d, 'DST'}`. Per-module and opt-in, so
there is no way to ship it by accident and no runtime cost when it is off.

Even when it *is* on, `dst_time` delegates to `erlang` unless a virtual clock is
running — so a module built with the transform behaves normally outside a
simulation.

## What is rewritten

| From | To |
|---|---|
| `erlang:send_after/3,4` | `dst_time:send_after/3,4` |
| `erlang:start_timer/3,4` | `dst_time:start_timer/3,4` |
| `erlang:cancel_timer/1,2` | `dst_time:cancel_timer/1,2` |
| `erlang:read_timer/1` | `dst_time:read_timer/1` |
| `erlang:monotonic_time/0,1` | `dst_time:monotonic_time/0,1` |
| `erlang:system_time/0,1` | `dst_time:system_time/0,1` |
| `erlang:timestamp/0` | `dst_time:timestamp/0` |
| `os:system_time/0,1` | `dst_time:system_time/0,1` |
| `os:timestamp/0` | `dst_time:timestamp/0` |

## Only *qualified* calls are rewritten

`erlang:monotonic_time()` is rewritten; a bare `monotonic_time()` is not. None of
these are auto-imported, so an unqualified call is a call to a function the module
defines itself — rewriting it would break the module. Code that wants to be
simulated must therefore qualify its time calls, which is the prevailing style
anyway.

`timer:sleep/1` is deliberately **not** rewritten. Sleeping is not a timer; a
process that sleeps is neither runnable nor blocked on a receive, so it cannot be
scheduled. Code under simulation should not sleep at all, and silently virtualising
it would hide that rather than surface it.
""".
-endif.

-export([parse_transform/2]).

%% {Module, Function, Arity} => target module.
-define(REWRITES, #{
    {erlang, send_after, 3} => dst_time,
    {erlang, send_after, 4} => dst_time,
    {erlang, start_timer, 3} => dst_time,
    {erlang, start_timer, 4} => dst_time,
    {erlang, cancel_timer, 1} => dst_time,
    {erlang, cancel_timer, 2} => dst_time,
    {erlang, read_timer, 1} => dst_time,
    {erlang, monotonic_time, 0} => dst_time,
    {erlang, monotonic_time, 1} => dst_time,
    {erlang, system_time, 0} => dst_time,
    {erlang, system_time, 1} => dst_time,
    {erlang, timestamp, 0} => dst_time,
    {os, system_time, 0} => dst_time,
    {os, system_time, 1} => dst_time,
    {os, timestamp, 0} => dst_time,
    %% Spawning. A child spawned plainly runs on the real scheduler until
    %% `set_on_spawn` adoption catches it; `dst_sched:spawn/1` starts it blocked
    %% instead, so there is no window. See `dst_sched:spawn/1`.
    {erlang, spawn, 1} => dst_sched,
    {erlang, spawn, 3} => dst_sched,
    {erlang, spawn_link, 1} => dst_sched,
    {erlang, spawn_link, 3} => dst_sched,
    {proc_lib, spawn, 1} => dst_sched,
    {proc_lib, spawn, 3} => dst_sched,
    {proc_lib, spawn_link, 1} => dst_sched,
    {proc_lib, spawn_link, 3} => dst_sched,
    %% `gen_server` starts its child inside OTP, where no transform reaches — the
    %% chain is gen:do_spawn/5 -> proc_lib:start_monitor/5 -> erlang:spawn_opt.
    %% `dst_sched` builds an equivalent child instead, gated from birth. See
    %% `dst_sched:start_monitor/3` for why not a shadowed `proc_lib`.
    {gen_server, start_monitor, 3} => dst_sched,
    {gen_server, start_link, 3} => dst_sched,
    {gen_server, start, 3} => dst_sched
}).

-if(?DOCATTRS).
-doc "The `parse_transform` entry point. See the module doc.".
-endif.
-spec parse_transform(Forms, list()) -> Forms when Forms :: [erl_parse:abstract_form()].
parse_transform(Forms, _Options) ->
    Locals = local_functions(Forms),
    observe_pass([walk(Form, Locals) || Form <- Forms]).

%% Every `{Name, Arity}` the module defines itself. Needed to decide whether an
%% unqualified `spawn(F)` is the auto-imported BIF or the module's own function.
local_functions(Forms) ->
    sets:from_list([{N, A} || {function, _, N, A, _} <- Forms]).

%% A generic structural walk. The Erlang abstract format is nested tuples and
%% lists, so rewriting a call node needs no understanding of the surrounding
%% syntax — only the shape of the node itself. Rewriting bottom-up (children
%% first) means arguments are already transformed when the call is considered,
%% which matters for nested calls like
%% `erlang:send_after(erlang:monotonic_time(), P, M)`.
walk(Node, Locals) when is_tuple(Node) ->
    rewrite(list_to_tuple([walk(E, Locals) || E <- tuple_to_list(Node)]), Locals);
walk(Nodes, Locals) when is_list(Nodes) ->
    [walk(E, Locals) || E <- Nodes];
walk(Node, _Locals) ->
    Node.

%% The auto-imported spawns. These are the one place the "only qualified calls"
%% rule does not apply, and the reason is specific rather than a matter of taste:
%% that rule exists because `monotonic_time` and the rest are *not* auto-imported,
%% so a bare call must be to a function the module defines and rewriting it would
%% break the module. `spawn/1,3` and `spawn_link/1,3` **are** auto-imported, so a
%% bare `spawn(Fun)` is `erlang:spawn(Fun)` — and leaving it alone means a system
%% under simulation keeps spawning ungated children. The registry this was built
%% against writes every one of its five spawns unqualified, so this is not a corner
%% case.
%%
%% Guarded on the module not defining the name itself, which is the condition the
%% original rule was really protecting.
-define(AUTO_SPAWNS, [{spawn, 1}, {spawn, 3}, {spawn_link, 1}, {spawn_link, 3}]).

%% A remote call whose target is in the rewrite table has its module replaced.
%% Arity is taken from the argument list, so a rewrite only applies to the exact
%% arities declared.
rewrite(
    {call, Anno, {remote, RAnno, {atom, MAnno, Mod}, {atom, FAnno, Fun}}, Args}, _Locals
) ->
    case maps:find({Mod, Fun, length(Args)}, ?REWRITES) of
        {ok, Target} ->
            {call, Anno, {remote, RAnno, {atom, MAnno, Target}, {atom, FAnno, Fun}}, Args};
        error ->
            {call, Anno, {remote, RAnno, {atom, MAnno, Mod}, {atom, FAnno, Fun}}, Args}
    end;
rewrite({call, Anno, {atom, FAnno, Fun}, Args}, Locals) ->
    Key = {Fun, length(Args)},
    case lists:member(Key, ?AUTO_SPAWNS) andalso not sets:is_element(Key, Locals) of
        true ->
            {call, Anno, {remote, Anno, {atom, Anno, dst_sched}, {atom, FAnno, Fun}}, Args};
        false ->
            {call, Anno, {atom, FAnno, Fun}, Args}
    end;
rewrite(Node, _Locals) ->
    Node.

%% ---------------------------------------------------------------------------
%% Observability pass — see the module doc's "Two passes, one attribute"
%% ---------------------------------------------------------------------------

%% The callbacks whose return value carries the state.
-define(WRAPPED, [
    {init, 1},
    {handle_call, 3},
    {handle_cast, 2},
    {handle_info, 2},
    {handle_continue, 2},
    {code_change, 3}
]).

-define(PUBLISH_FUN, '$dst_observe').
-define(PUT_FUN, '$dst_observe_put').

%% Applied only when the module declares `-dst_observe(...)`; otherwise the forms
%% pass through untouched.
observe_pass(Forms) ->
    case spec(Forms) of
        none ->
            Forms;
        Spec ->
            {Record, Fields} = resolve(Spec, records(Forms)),
            Anno = erl_anno:new(0),
            Wrapped = [wrap(F) || F <- Forms],
            insert_before_eof(Wrapped, helpers(Anno, Record, Fields))
    end.

%% ---------------------------------------------------------------------------
%% Reading the declaration
%% ---------------------------------------------------------------------------

spec([{attribute, _, dst_observe, Spec} | _]) -> Spec;
spec([_ | Rest]) -> spec(Rest);
spec([]) -> none.

records(Forms) ->
    maps:from_list([
        {Name, [field_name(F) || F <- FieldDecls]}
     || {attribute, _, record, {Name, FieldDecls}} <- Forms
    ]).

field_name({record_field, _, {atom, _, Name}}) -> Name;
field_name({record_field, _, {atom, _, Name}, _Default}) -> Name;
field_name({typed_record_field, Inner, _Type}) -> field_name(Inner).

%% `all`, a field list (assuming `#state{}`), or an explicit `{Record, Fields}`.
resolve(all, _Records) ->
    {state, all};
resolve({Record, Fields}, Records) when is_atom(Record), is_list(Fields) ->
    {Record, positions(Record, Fields, Records)};
resolve(Fields, Records) when is_list(Fields) ->
    {state, positions(state, Fields, Records)}.

positions(Record, Fields, Records) ->
    Declared =
        case maps:find(Record, Records) of
            {ok, D} ->
                D;
            error ->
                error({dst_observe, {no_such_record, Record}})
        end,
    [{F, position(F, Record, Declared)} || F <- Fields].

%% A field that does not exist is a compile-time error rather than a silently
%% wrong `element/2` offset.
position(Field, Record, Declared) ->
    case index_of(Field, Declared, 2) of
        none -> error({dst_observe, {no_such_field, Record, Field, Declared}});
        N -> N
    end.

index_of(_F, [], _N) -> none;
index_of(F, [F | _], N) -> N;
index_of(F, [_ | Rest], N) -> index_of(F, Rest, N + 1).

%% ---------------------------------------------------------------------------
%% Rewriting
%% ---------------------------------------------------------------------------

wrap({function, Anno, Name, Arity, Clauses}) ->
    case lists:member({Name, Arity}, ?WRAPPED) of
        true -> {function, Anno, Name, Arity, [wrap_clause(C) || C <- Clauses]};
        false -> {function, Anno, Name, Arity, Clauses}
    end;
wrap(Form) ->
    Form.

wrap_clause({clause, Anno, Pats, Guards, Body}) ->
    Call = {call, Anno, {atom, Anno, ?PUBLISH_FUN}, [{block, Anno, Body}]},
    {clause, Anno, Pats, Guards, [Call]}.

insert_before_eof([{eof, _} = Eof | Rest], Helpers) ->
    Helpers ++ [Eof | Rest];
insert_before_eof([Form | Rest], Helpers) ->
    [Form | insert_before_eof(Rest, Helpers)];
insert_before_eof([], Helpers) ->
    Helpers.

%% ---------------------------------------------------------------------------
%% Generated code
%% ---------------------------------------------------------------------------

helpers(Anno, Record, Fields) ->
    [publish_fun(Anno), put_fun(Anno, Record, Fields)].

%% '$dst_observe'(Ret) -> _ = case Ret of ... end, Ret.
%%
%% Every gen_server return shape carries its state in a position the leading tag
%% disambiguates: {reply,_,S} against {stop,_,S} at arity three, {reply,_,S,_}
%% against {stop,_,_,S} at arity four.
publish_fun(Anno) ->
    Ret = {var, Anno, 'Ret'},
    S = {var, Anno, 'S'},
    Put = fun(Pats) ->
        {clause, Anno, [{tuple, Anno, Pats}], [], [
            {call, Anno, {atom, Anno, ?PUT_FUN}, [S]}
        ]}
    end,
    U = {var, Anno, '_'},
    Clauses = [
        Put([{atom, Anno, reply}, U, S]),
        Put([{atom, Anno, reply}, U, S, U]),
        Put([{atom, Anno, noreply}, S]),
        Put([{atom, Anno, noreply}, S, U]),
        Put([{atom, Anno, stop}, U, S]),
        Put([{atom, Anno, stop}, U, U, S]),
        Put([{atom, Anno, ok}, S]),
        Put([{atom, Anno, ok}, S, U]),
        {clause, Anno, [U], [], [{atom, Anno, ok}]}
    ],
    Body = [
        {match, Anno, U, {'case', Anno, Ret, Clauses}},
        Ret
    ],
    {function, Anno, ?PUBLISH_FUN, 1, [{clause, Anno, [Ret], [], Body}]}.

%% '$dst_observe_put'(S) when is_record(S, Record) -> put(Key, Observed), ok;
%% '$dst_observe_put'(_) -> ok.
put_fun(Anno, Record, Fields) ->
    S = {var, Anno, 'S'},
    Guard = [
        [
            {call, Anno, {atom, Anno, is_tuple}, [S]},
            {op, Anno, '=:=', {call, Anno, {atom, Anno, element}, [{integer, Anno, 1}, S]},
                {atom, Anno, Record}}
        ]
    ],
    Observed =
        case Fields of
            all ->
                S;
            _ ->
                {map, Anno, [
                    {map_field_assoc, Anno, {atom, Anno, F},
                        {call, Anno, {atom, Anno, element}, [{integer, Anno, N}, S]}}
                 || {F, N} <- Fields
                ]}
        end,
    Put =
        {call, Anno, {remote, Anno, {atom, Anno, erlang}, {atom, Anno, put}}, [
            {atom, Anno, dst_observe:key()}, Observed
        ]},
    {function, Anno, ?PUT_FUN, 1, [
        {clause, Anno, [S], Guard, [Put, {atom, Anno, ok}]},
        {clause, Anno, [{var, Anno, '_'}], [], [{atom, Anno, ok}]}
    ]}.
