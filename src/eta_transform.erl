-module(eta_transform).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
A `parse_transform` that points a module's timer and clock calls at `eta_time` —
Phase 1 of the DST framework (design: `docs/design.md`).

Nothing else about the module changes, and the rewrite is a pure call-target
substitution: `erlang:send_after(T, D, M)` becomes `eta_time:send_after(T, D, M)`,
and so on for the table below.

## Four passes, one transform

A module never names more than one `parse_transform`. The extra passes are
controlled by attributes rather than by naming another transform:

1. **Timer and clock rewriting** — the table below, applied always.
2. **Message sending** — points `!` and the `cast`/`reply` functions at
   `eta_net`, so a simulated network can drop, delay or cut them. Applied unless
   the module declares `-eta_net(false)`; see below.
3. **Receive timeouts** — puts `receive ... after T` on the virtual clock.
   Applied unless the module declares `-eta_after(false)`; see below.
4. **State observability** — applied only to a module declaring
   `-eta_observe(all)` or `-eta_observe({Record, Fields})`, which republishes the
   state on every `gen_server` or `gen_statem` callback return so a simulation can
   read it while the process is suspended. See `eta_observe`.

They live here rather than in modules of their own for a build reason, and it is
worth recording because it cost a broken CI run to find. **Mix does not reliably
order a module after more than one `parse_transform`.** With two attributes on
one module, an incremental build where both the transform and that module
changed failed with `undefined parse transform` roughly two times in three —
nondeterministically, so it looked intermittent before it was pinned down. One
attribute is ordered correctly; two are not.

Delegating from here to a separate module does not reliably help either. Mix
must compile a transform before anything that uses it and does not track a
runtime call as a dependency, so within this project the hazard just moves down
a level. Merging is what removes it.

So: **never give a module two parse transforms.** If a module needs another
pass, add it here and gate it on an attribute.

## Virtualising `receive ... after`

On by default. The rewrite turns

```erlang
receive Pat -> Body after T -> Timeout end
```

into a receive whose timeout is an ordinary message clause, armed through
`eta_time:arm_after/2`. Under `eta_sched` the waiting process is correctly
unrunnable until the virtual clock delivers it, so a 60-second timeout costs
microseconds instead of a minute — and, more importantly, fires at a point the
schedule chose.

`after 0` is left alone. It is a mailbox poll rather than a wait, never blocks,
and never consults a clock; routing it through the timer wheel would turn a
non-blocking poll into a blocking one.

**Why it is on by default.** `after` is the one real-time dependence a system
can hold without naming a function, so leaving it to an attribute means a module
compiled with this transform can still wake on the wall clock with nothing
reporting it. The call-rewriting pass is unconditional for the same reason;
there is no principled line that puts `erlang:send_after/3` on one side of it
and `after` on the other.

**`-eta_after(false).` opts out**, for a module where the cost is measurable. It
is a ref, an `update_counter` and two inserts to arm, and a `take` to cancel.
No mailbox scan: `eta_time:disarm_after/2` skips its flush when the cancel
succeeded, because a timer removed before it fired never sent anything. Worth
knowing about before putting it on a hot receive loop, and not worth thinking
about otherwise.

### Why the timeout being an ordinary clause does not change the semantics

Worth stating, because it looks as though it should. Native `after` is a
*fallback*: it fires only when no queued message matches. The rewrite makes the
timeout an ordinary clause, and a `receive` takes the first message matching
*any* clause — so it appears to compete on arrival order rather than deferring.

It does not diverge, because arrival order and time order are the same order.
The `{'$eta_after', Ref}` message is appended *at* the deadline, so any message
that would have satisfied native `after` before it elapsed is necessarily older
in the mailbox and is scanned first. A message arriving after the deadline sits
behind a timeout that native would already have fired. Stale timeouts from an
earlier receive cannot match, because of the ref guard, and `disarm_after/2`
flushes one that fired just as a real message landed.

A two-stage rewrite — poll with `after 0` first, arm only on a miss — would make
that structural rather than emergent, and would skip arming when a message is
already waiting. It is deliberately not done. The poll still walks the whole
mailbox before failing, so a miss scans twice, and a miss is the deep selective
receive `gen_server:call/3` produces. It also duplicates every clause body into
both receives. With the flush gone from the hit path there is little left to
win.

## Enabling it

Include the header, which carries the attribute behind the `DST` define:

```erlang
-include_lib("eta/include/eta.hrl").
```

and compile the simulation build with `{d, 'DST'}`. Per-module and opt-in, so
there is no way to ship it by accident and no runtime cost when it is off. The
same header defines `?ETA_LOG` and `?ETA_LABEL`; see `eta_log`.

Even when it *is* on, `eta_time` delegates to `erlang` unless a virtual clock is
running — so a module built with the transform behaves normally outside a
simulation.

## What is rewritten

| From | To |
|---|---|
| `erlang:send_after/3,4` | `eta_time:send_after/3,4` |
| `erlang:start_timer/3,4` | `eta_time:start_timer/3,4` |
| `erlang:cancel_timer/1,2` | `eta_time:cancel_timer/1,2` |
| `erlang:read_timer/1` | `eta_time:read_timer/1` |
| `erlang:monotonic_time/0,1` | `eta_time:monotonic_time/0,1` |
| `erlang:system_time/0,1` | `eta_time:system_time/0,1` |
| `erlang:timestamp/0` | `eta_time:timestamp/0` |
| `os:system_time/0,1` | `eta_time:system_time/0,1` |
| `os:timestamp/0` | `eta_time:timestamp/0` |

## Sending

The network pass rewrites, unless the module declares `-eta_net(false)`:

| From | To |
|---|---|
| `Dest ! Msg` | `eta_net:send(Dest, Msg)` |
| `erlang:send/2,3` | `eta_net:send/2,3` |
| `gen_server:cast/2` | `eta_net:cast/2` |
| `gen_statem:cast/2` | `eta_net:cast/2` |
| `gen_server:reply/2` | `eta_net:reply/2` |
| `gen_statem:reply/1,2` | `eta_net:reply/1,2` |
| `erlang:monitor/2,3` | `eta_net:monitor/2,3` |
| `erlang:demonitor/1,2` | `eta_net:demonitor/1,2` |

`!` is the one rewrite that is not a call.

**Why monitors are in this pass.** A link failure fires every monitor held across
it, and a monitor is made by a BIF inside the system under test — so a simulated
network can only know one exists by being what creates it. Without the rewrite a
partition is invisible to the code that detects peers, which is most of the code
that matters. `eta_net:monitor/2` simulates only a monitor whose ends are placed
on different simulated nodes and leaves every other one a plain `erlang:monitor`,
so the rewrite changes nothing for a module that is not part of a topology.

The monitor BIFs are auto-imported, so a bare `monitor(process, Pid)` is rewritten
too — on the same terms as the bare spawns below: only when the module does not
define that name itself. `eta_net:send/2` returns `Msg`, which
is what `!` evaluates to, so the substitution is value-preserving.

`eta_net` delegates to `erlang:send/2` unless a network is running, so this pass
changes nothing about a run that does not start one. What it *does* change is who
can be faulted later: routing has to be uniform per channel, because a direct
send can overtake a delayed one, and module-granular rewriting is what makes that
safe. See `eta_net`.

`gen_server:call/2,3` and `gen_statem:call/2,3` are both rewritten to
`eta_net:call/2,3`, which routes the request. The **reply** leg is reached from
the other end, and how depends on the behaviour:

- In a module declaring `-behaviour(gen_server)`, every `handle_call/3` clause is
  wrapped so a `{reply, R, S}` return becomes a routed `eta_net:reply(From, R)`
  and a `{noreply, S}`.
- In a module declaring `-behaviour(gen_statem)`, every state callback is wrapped
  so a `{reply, From, R}` action in the returned action list is sent through
  `eta_net:reply/2` and removed from the list. See `eta_net:statem_return/1`.

Both legs are then ordinary network traffic and can be faulted independently —
which is what makes "the work happened, the caller never learned it did" a
reachable state. See `eta_net:call/3`.

`gen_statem`'s starts are gated like `gen_server`'s, through
`eta_sched:statem_start_link/3` and friends.

**What `gen_statem` support does not include.** The asynchronous
`send_request`/`wait_response` interface, and any state-machine feature whose
behaviour under simulation has not been established — state time-outs and
`hibernate` in particular. `init/1` returning something other than
`{ok, State, Data}` or `{ok, State, Data, Actions}` is refused by
`eta_sched:statem_start_link/3` rather than guessed at.

## What raises rather than being rewritten

Everything `eta_net` cannot yet put on the network raises, so the boundary is
discoverable by using it instead of by reading a table.

| | when |
|---|---|
| `gen_server:abcast`, `multi_call`, `send_request`/`wait_response`/`receive_response`/`check_response` | at run time, while a network is running |
| `gen_statem`'s `send_request`/`wait_response`/`receive_response`/`check_response` | the same |
| every client-side function of `gen_event` | the same |
| a module declaring `-behaviour(gen_event)` | at **compile** time |

The last one is a property of the module rather than of a code path: a handler's
reply is produced inside the event manager, which is neither the module this
transform rewrote nor a process it can reach — so every call answered by such a
module would come around the network, in every run. `-eta_net(false).` opts the
module out of this pass and keeps the rest.

The runtime ones are runtime because a module may hold one on a path no
simulation reaches, and refusing to compile it would block adoption over code the
run never executes. Both are inert without a network.

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

*This documentation is LLM-generated. See the AI disclosure in `README.md`.*
""".
-endif.

-export([parse_transform/2]).

%% `{Module, Function, Arity} => Target`, where `Target` is either a module (the
%% function name is kept) or `{Module, Function}` (both are replaced).
%%
%% The distributed forms — `spawn(Node, Fun)` and friends — are rewritten too, but
%% to functions that **raise while a run is active**. `eta` does not simulate
%% distribution, and there is no honest local rewrite: running the child here puts
%% it on the wrong node, and letting it through puts it outside the schedule. See
%% `eta_sched:no_dist/3`.
-define(REWRITES, #{
    {erlang, send_after, 3} => eta_time,
    {erlang, send_after, 4} => eta_time,
    {erlang, start_timer, 3} => eta_time,
    {erlang, start_timer, 4} => eta_time,
    {erlang, cancel_timer, 1} => eta_time,
    {erlang, cancel_timer, 2} => eta_time,
    {erlang, read_timer, 1} => eta_time,
    {erlang, monotonic_time, 0} => eta_time,
    {erlang, monotonic_time, 1} => eta_time,
    {erlang, system_time, 0} => eta_time,
    {erlang, system_time, 1} => eta_time,
    {erlang, timestamp, 0} => eta_time,
    {os, system_time, 0} => eta_time,
    {os, system_time, 1} => eta_time,
    {os, timestamp, 0} => eta_time,
    %% Spawning. A child spawned plainly runs on the real scheduler until
    %% `set_on_spawn` adoption catches it; `eta_sched:spawn/1` starts it blocked
    %% instead, so there is no window. See `eta_sched:spawn/1`.
    {erlang, spawn, 1} => eta_sched,
    {erlang, spawn, 3} => eta_sched,
    {erlang, spawn_link, 1} => eta_sched,
    {erlang, spawn_link, 3} => eta_sched,
    {erlang, spawn_monitor, 1} => eta_sched,
    {erlang, spawn_monitor, 3} => eta_sched,
    {erlang, spawn_opt, 2} => eta_sched,
    {erlang, spawn_opt, 4} => eta_sched,
    %% `proc_lib`'s spawns additionally give the child `$ancestors` and
    %% `$initial_call`, so they get their own targets rather than sharing the
    %% plain ones — see `eta_sched:plib_spawn/1`.
    {proc_lib, spawn, 1} => {eta_sched, plib_spawn},
    {proc_lib, spawn, 3} => {eta_sched, plib_spawn},
    {proc_lib, spawn_link, 1} => {eta_sched, plib_spawn_link},
    {proc_lib, spawn_link, 3} => {eta_sched, plib_spawn_link},
    {proc_lib, spawn_opt, 2} => {eta_sched, plib_spawn_opt},
    {proc_lib, spawn_opt, 4} => {eta_sched, plib_spawn_opt},
    %% `gen_server` starts its child inside OTP, where no transform reaches — the
    %% chain is gen:do_spawn/5 -> proc_lib:start_monitor/5 -> erlang:spawn_opt.
    %% `eta_sched` builds an equivalent child instead, gated from birth. See
    %% `eta_sched:start_monitor/3` for why not a shadowed `proc_lib`.
    %% The distributed forms. Rewritten to raise rather than to run, see above.
    {erlang, spawn, 2} => eta_sched,
    {erlang, spawn, 4} => eta_sched,
    {erlang, spawn_link, 2} => eta_sched,
    {erlang, spawn_link, 4} => eta_sched,
    {erlang, spawn_monitor, 2} => eta_sched,
    {erlang, spawn_monitor, 4} => eta_sched,
    {erlang, spawn_opt, 3} => eta_sched,
    {erlang, spawn_opt, 5} => eta_sched,
    {proc_lib, spawn, 2} => {eta_sched, plib_spawn},
    {proc_lib, spawn, 4} => {eta_sched, plib_spawn},
    {proc_lib, spawn_link, 2} => {eta_sched, plib_spawn_link},
    {proc_lib, spawn_link, 4} => {eta_sched, plib_spawn_link},
    {proc_lib, spawn_opt, 3} => {eta_sched, plib_spawn_opt},
    {proc_lib, spawn_opt, 5} => {eta_sched, plib_spawn_opt},
    %% `logger` hands every event to a handler process the scheduler does not own,
    %% and under load that handoff turns synchronous. See `eta_logger`.
    {logger, emergency, 1} => eta_logger,
    {logger, emergency, 2} => eta_logger,
    {logger, emergency, 3} => eta_logger,
    {logger, alert, 1} => eta_logger,
    {logger, alert, 2} => eta_logger,
    {logger, alert, 3} => eta_logger,
    {logger, critical, 1} => eta_logger,
    {logger, critical, 2} => eta_logger,
    {logger, critical, 3} => eta_logger,
    {logger, error, 1} => eta_logger,
    {logger, error, 2} => eta_logger,
    {logger, error, 3} => eta_logger,
    {logger, warning, 1} => eta_logger,
    {logger, warning, 2} => eta_logger,
    {logger, warning, 3} => eta_logger,
    {logger, notice, 1} => eta_logger,
    {logger, notice, 2} => eta_logger,
    {logger, notice, 3} => eta_logger,
    {logger, info, 1} => eta_logger,
    {logger, info, 2} => eta_logger,
    {logger, info, 3} => eta_logger,
    {logger, debug, 1} => eta_logger,
    {logger, debug, 2} => eta_logger,
    {logger, debug, 3} => eta_logger,
    {logger, log, 2} => eta_logger,
    {logger, log, 3} => eta_logger,
    {logger, log, 4} => eta_logger,
    {gen_server, start_monitor, 3} => eta_sched,
    {gen_server, start_monitor, 4} => eta_sched,
    {gen_server, start_link, 3} => eta_sched,
    {gen_server, start_link, 4} => eta_sched,
    {gen_server, start, 3} => eta_sched,
    {gen_server, start, 4} => eta_sched,
    %% `gen_statem` starts its child through the same `gen:do_spawn/5`, so it
    %% needs the same treatment. Separate targets rather than shared ones,
    %% because the gated child has to run the right `init/1` shape and enter the
    %% right loop. See `eta_sched:statem_start_link/3`.
    {gen_statem, start_monitor, 3} => {eta_sched, statem_start_monitor},
    {gen_statem, start_monitor, 4} => {eta_sched, statem_start_monitor},
    {gen_statem, start_link, 3} => {eta_sched, statem_start_link},
    {gen_statem, start_link, 4} => {eta_sched, statem_start_link},
    {gen_statem, start, 3} => {eta_sched, statem_start},
    {gen_statem, start, 4} => {eta_sched, statem_start}
}).

-if(?DOCATTRS).
-doc "The `parse_transform` entry point. See the module doc.".
-endif.
-spec parse_transform(Forms, list()) -> Forms when Forms :: [erl_parse:abstract_form()].
parse_transform(Forms, _Options) ->
    Locals = local_functions(Forms),
    Rewritten = [walk(Form, Locals) || Form <- Forms],
    observe_pass(after_pass(net_pass(Rewritten, Locals))).

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
-define(AUTO_SPAWNS, [
    {spawn, 1},
    {spawn, 3},
    {spawn_link, 1},
    {spawn_link, 3},
    {spawn_monitor, 1},
    {spawn_monitor, 3},
    {spawn_opt, 2},
    {spawn_opt, 4}
]).

%% A remote call whose target is in the rewrite table has its module replaced.
%% Arity is taken from the argument list, so a rewrite only applies to the exact
%% arities declared.
rewrite(
    {call, Anno, {remote, RAnno, {atom, MAnno, Mod}, {atom, FAnno, Fun}}, Args}, _Locals
) ->
    case maps:find({Mod, Fun, length(Args)}, ?REWRITES) of
        {ok, {Target, TargetFun}} ->
            {call, Anno, {remote, RAnno, {atom, MAnno, Target}, {atom, FAnno, TargetFun}}, Args};
        {ok, Target} ->
            {call, Anno, {remote, RAnno, {atom, MAnno, Target}, {atom, FAnno, Fun}}, Args};
        error ->
            {call, Anno, {remote, RAnno, {atom, MAnno, Mod}, {atom, FAnno, Fun}}, Args}
    end;
rewrite({call, Anno, {atom, FAnno, Fun}, Args}, Locals) ->
    Key = {Fun, length(Args)},
    case lists:member(Key, ?AUTO_SPAWNS) andalso not sets:is_element(Key, Locals) of
        true ->
            {call, Anno, {remote, Anno, {atom, Anno, eta_sched}, {atom, FAnno, Fun}}, Args};
        false ->
            {call, Anno, {atom, FAnno, Fun}, Args}
    end;
rewrite(Node, _Locals) ->
    Node.

%% ---------------------------------------------------------------------------
%% Network pass — points sends at `eta_net`, unless `-eta_net(false)`
%% ---------------------------------------------------------------------------

%% The sends that name a function, and so can be rewritten the ordinary way.
%%
%% `gen_server:call/2,3` is deliberately absent. Its request could be rewritten
%% here, but the reply is sent by `gen:reply/2` from inside OTP where no transform
%% reaches — so routing one leg and not the other would produce a channel that
%% carries some of a conversation and not the rest, which is exactly the
%% mixed-routing hazard `eta_net`'s moduledoc warns about. `gen_server:reply/2` is
%% rewritten instead, which covers the reply a callback defers rather than returns.
%% Name of the generated helper that routes a `handle_call` reply.
-define(REPLY_FUN, '$eta_net_reply').

-define(NET_REWRITES, #{
    {erlang, send, 2} => eta_net,
    {erlang, send, 3} => eta_net,
    {gen_server, cast, 2} => {eta_net, cast},
    {gen_statem, cast, 2} => {eta_net, cast},
    {gen_server, reply, 2} => {eta_net, reply},
    %% `gen_statem:reply/2` is `gen_server:reply/2` under another name.
    %% `reply/1` is the list-or-single form, and gets its own target.
    {gen_statem, reply, 1} => {eta_net, reply},
    {gen_statem, reply, 2} => {eta_net, reply},
    %% Monitors. A partition has to fire the `noconnection` DOWNs real
    %% distribution would, and a monitor is created by a BIF inside the system
    %% under test — so the only way this module can know one exists is to be the
    %% thing that creates it. Same seam as the sends, and it belongs to the *net*
    %% pass rather than the unconditional one for the same reason: a module that
    %% takes no part in the network should keep its monitors exactly as they are.
    %% `eta_net` leaves everything but a monitor across a declared link as a plain
    %% `erlang:monitor`. See `eta_net:monitor/2`.
    {erlang, monitor, 2} => eta_net,
    {erlang, monitor, 3} => eta_net,
    {erlang, demonitor, 1} => eta_net,
    {erlang, demonitor, 2} => eta_net,
    %% The request leg. The reply leg is brought onto the network from the other
    %% end, by `handle_call_pass/1` below — routing only one of them would leave
    %% the pair half-covered, and a reply that goes around the network can
    %% overtake a message that went through it. See `eta_net:call/3`.
    {gen_server, call, 2} => {eta_net, call},
    {gen_server, call, 3} => {eta_net, call},
    %% The same request on the wire, so the same target. The reply leg comes back
    %% through `statem_return_pass/1` below rather than through
    %% `handle_call_pass/1`.
    {gen_statem, call, 2} => {eta_net, call},
    {gen_statem, call, 3} => {eta_net, call}
}).

%% Client-side messaging `eta_net` does not implement yet.
%%
%% Each is rewritten to `eta_net:unsupported/2`, which raises **while a network is
%% running** and otherwise calls the original. Same position `eta_sched` takes on
%% the distributed spawn forms, and for the same reason: there is no honest local
%% rewrite, and silently letting the call through would mean a run states a fault
%% model that quietly does not cover a channel.
%%
%% Raising at runtime rather than at compile time is deliberate. A module may hold
%% one of these on a path no simulation ever reaches, and refusing to compile it
%% would block adoption over code the run never executes.
%%
%% What is *not* here is as much of a statement as what is: `call/2,3`, `cast/2`
%% and `reply/1,2` are supported on both `gen_server` and `gen_statem`.
%% Everything else in these three interfaces is a gap with a name.
-define(NET_UNSUPPORTED, [
    %% Broadcasts and multi-node calls: distribution, which `eta` does not
    %% simulate at all.
    {gen_server, abcast, 2},
    {gen_server, abcast, 3},
    {gen_server, multi_call, 2},
    {gen_server, multi_call, 3},
    {gen_server, multi_call, 4},
    %% The asynchronous request/response interface. Routable in principle — the
    %% request is an ordinary send and the reply comes back through the same
    %% `handle_call` return this transform already rewrites — but the reply is
    %% matched by a `ReqId` the caller holds rather than by a receive this module
    %% controls, so it needs its own design.
    {gen_server, send_request, 2},
    {gen_server, send_request, 4},
    {gen_server, wait_response, 2},
    {gen_server, wait_response, 3},
    {gen_server, receive_response, 2},
    {gen_server, receive_response, 3},
    {gen_server, check_response, 2},
    {gen_server, check_response, 3},
    %% `gen_statem:call/2,3` is *not* here: its reply leg is brought onto the
    %% network by `statem_return_pass/1`, the same way `handle_call_pass/1` does
    %% it for `gen_server`. What remains is the asynchronous interface, which has
    %% the same `ReqId` problem as `gen_server`'s.
    {gen_statem, send_request, 2},
    {gen_statem, send_request, 4},
    {gen_statem, wait_response, 1},
    {gen_statem, wait_response, 2},
    {gen_statem, wait_response, 3},
    {gen_statem, receive_response, 1},
    {gen_statem, receive_response, 2},
    {gen_statem, receive_response, 3},
    {gen_statem, check_response, 2},
    {gen_statem, check_response, 3},
    %% `gen_event` the same: a handler's reply is produced inside the event
    %% manager, out of reach.
    {gen_event, notify, 2},
    {gen_event, sync_notify, 2},
    {gen_event, call, 3},
    {gen_event, call, 4},
    {gen_event, send_request, 3},
    {gen_event, send_request, 5},
    {gen_event, wait_response, 2},
    {gen_event, wait_response, 3},
    {gen_event, receive_response, 2},
    {gen_event, receive_response, 3},
    {gen_event, check_response, 2},
    {gen_event, check_response, 3}
]).

%% On unless a module says otherwise, for the same reason the `after` pass is:
%% a knob you have to know about is a silent hole waiting to happen. A module
%% built for simulation whose sends bypass the network makes the network *look*
%% installed while a channel it should own goes unrouted — and a half-routed
%% channel can reorder, which manufactures findings.
net_pass(Forms, Locals) ->
    case net_enabled(Forms) of
        false ->
            Forms;
        true ->
            ok = behaviour_unsupported(Forms),
            statem_return_pass(handle_call_pass(net_walk(Forms, Locals)))
    end.

%% A `gen_event` module cannot have its replies put on the network, so it is
%% refused at **compile time** rather than at run time.
%%
%% The difference from `?NET_UNSUPPORTED` is that this is a property of the module
%% rather than of a code path. A handler's reply to `gen_event:call/3` is produced
%% inside the event manager — a process this transform never sees, running a
%% module it never rewrites — so *every* call answered by this module would come
%% around the network, and there is no run in which that is not true. A runtime
%% raise would report it once per call site instead of once, and only for the
%% paths a run happened to take.
%%
%% `gen_statem` used to be refused here too, on the grounds that it replies from
%% inside its own action handling rather than from a `handle_call` return. That
%% is true and it is not an obstacle: the action naming the caller is in the value
%% the callback *returns*, so `statem_return_pass/1` can take it out and route it
%% before `gen_statem` ever sees it. A `gen_event` handler has no such seam.
%%
%% `-eta_net(false).` is the opt-out, and it is a real one: the module keeps timer,
%% clock and spawn rewriting and simply takes no part in the network. That is the
%% right answer for an event manager that is not a peer.
behaviour_unsupported(Forms) ->
    case lists:member(gen_event, behaviours(Forms)) of
        false ->
            ok;
        true ->
            error(
                {eta_net,
                    {behaviour_unsupported, gen_event, <<
                        "eta_net cannot route this behaviour's replies: they are sent "
                        "from inside the event manager rather than from anything this "
                        "transform rewrites, so a call answered here would always come "
                        "around the network. Add `-eta_net(false).` to keep this module "
                        "out of the network pass (timer, clock and spawn rewriting are "
                        "unaffected)"
                    >>}}
            )
    end.

behaviours(Forms) ->
    [B || {attribute, _, A, B} <- Forms, A =:= behaviour orelse A =:= behavior].

%% Puts the *reply* leg of a `gen_server:call` on the network.
%%
%% `gen_server:call/3`'s request is an ordinary send this transform can point at
%% `eta_net`. Its reply is not: `gen:reply/2` runs inside OTP and every one of its
%% clauses ends in a raw send from the server process, so no rewrite at the call
%% site can reach it. Routing one leg and not the other is worse than routing
%% neither — the request is then subject to the network's ordering clamp and the
%% reply is not, so a reply can be delivered ahead of a message sent before it.
%%
%% The answer is to rewrite the *callee*. A `handle_call/3` clause body is wrapped
%% so that its return passes through a generated helper:
%%
%%     handle_call(Req, From, S) -> '$eta_net_reply'(From, begin OriginalBody end).
%%
%% and the helper turns a reply-carrying return into a routed reply plus a
%% `noreply`:
%%
%%     {reply, R, S}        ->  eta_net:reply(From, R), {noreply, S}
%%     {reply, R, S, X}     ->  eta_net:reply(From, R), {noreply, S, X}
%%     {stop, Reason, R, S} ->  eta_net:reply(From, R), {stop, Reason, S}
%%
%% Matching on the value at runtime rather than on `{reply, ...}` literals in the
%% source is deliberate, and it is the same choice `-eta_observe`'s pass makes: a
%% callback that builds its return in a helper function has no literal here to
%% rewrite, and a pass that only caught the literals would cover most of a module
%% and quietly miss the rest.
%%
%% Only for a module that declares `-behaviour(gen_server)`. `handle_call/3` is a
%% name anyone may use, and these three shapes mean something specific only in
%% that contract.
handle_call_pass(Forms) ->
    case is_gen_server(Forms) andalso lists:any(fun is_handle_call/1, Forms) of
        false ->
            Forms;
        true ->
            Anno = erl_anno:new(0),
            insert_before_eof([wrap_handle_call(F) || F <- Forms], [reply_fun(Anno)])
    end.

is_gen_server([{attribute, _, behaviour, gen_server} | _]) -> true;
is_gen_server([{attribute, _, behavior, gen_server} | _]) -> true;
is_gen_server([_ | Rest]) -> is_gen_server(Rest);
is_gen_server([]) -> false.

is_handle_call({function, _, handle_call, 3, _}) -> true;
is_handle_call(_) -> false.

wrap_handle_call({function, Anno, handle_call, 3, Clauses}) ->
    {function, Anno, handle_call, 3, [wrap_handle_call_clause(C) || C <- Clauses]};
wrap_handle_call(Form) ->
    Form.

%% The `From` argument is whatever the clause called it, and it may be a pattern
%% rather than a variable — `handle_call(Req, {Pid, _} = From, S)` is ordinary, and
%% so is a clause that ignores it. A fresh variable is bound to it instead, which
%% works whatever the original pattern was and cannot collide with the clause's
%% own names.
wrap_handle_call_clause({clause, Anno, [Req, From, State], Guards, Body}) ->
    V = {var, Anno, '$eta_net_from'},
    {clause, Anno, [Req, {match, Anno, V, From}, State], Guards, [
        {call, Anno, {atom, Anno, ?REPLY_FUN}, [V, {block, Anno, Body}]}
    ]};
wrap_handle_call_clause(Clause) ->
    Clause.

%% '$eta_net_reply'(From, Ret) -> case Ret of ... end.
reply_fun(Anno) ->
    From = {var, Anno, 'From'},
    Ret = {var, Anno, 'Ret'},
    R = {var, Anno, 'R'},
    S = {var, Anno, 'S'},
    X = {var, Anno, 'X'},
    Reason = {var, Anno, 'Reason'},
    Send = {call, Anno, {remote, Anno, {atom, Anno, eta_net}, {atom, Anno, reply}}, [From, R]},
    Tup = fun(Es) -> {tuple, Anno, Es} end,
    A = fun(Name) -> {atom, Anno, Name} end,
    Clauses = [
        {clause, Anno, [Tup([A(reply), R, S])], [], [Send, Tup([A(noreply), S])]},
        {clause, Anno, [Tup([A(reply), R, S, X])], [], [Send, Tup([A(noreply), S, X])]},
        {clause, Anno, [Tup([A(stop), Reason, R, S])], [], [
            Send, Tup([A(stop), Reason, S])
        ]},
        {clause, Anno, [Ret], [], [Ret]}
    ],
    {function, Anno, ?REPLY_FUN, 2, [
        {clause, Anno, [From, Ret], [], [{'case', Anno, Ret, Clauses}]}
    ]}.

%% ---------------------------------------------------------------------------
%% The `gen_statem` reply pass
%% ---------------------------------------------------------------------------

%% The `gen_statem` counterpart of `handle_call_pass/1`, and the same job: get the
%% reply leg of a call onto the network so it can be faulted apart from the
%% request.
%%
%% Where a `gen_server` returns `{reply, R, S}` and leaves OTP to work out who
%% asked, a `gen_statem` says so itself — the reply is a `{reply, From, Msg}`
%% action in the value the callback returns. That makes the rewrite simpler than
%% the `gen_server` one rather than harder: nothing from the clause head is
%% needed, so there is no generated per-module helper. The body is passed to
%% `eta_net:statem_return/1`, which sends each reply action and takes it out of
%% the list:
%%
%%     handle_event(T, C, S, D) -> eta_net:statem_return(begin OriginalBody end).
%%
%% Inspecting the value at runtime rather than the `{reply, ...}` literals in the
%% source is the same choice `handle_call_pass/1` and the observability pass make,
%% for the same reason: a callback that builds its actions in a helper has no
%% literal here to rewrite.
%%
%% ## Which functions are wrapped
%%
%% Whichever ones `gen_statem` will call, which depends on the callback mode:
%% `handle_event/4` in `handle_event_function` mode, and every state function in
%% `state_functions` mode. A state function is named by the state, so there is no
%% fixed list — but `gen_statem` invokes it as `Module:StateName/3`, so it must be
%% **exported**, and that is the filter used. `terminate/3` is the one exported
%% arity-3 name that is a callback and not a state, so it is excluded by name.
%%
%% The mode is read from a literal `callback_mode/0`, which is how essentially
%% every module writes it. A module that computes it gets both sets wrapped,
%% which is the safe direction: `eta_net:statem_return/1` returns anything it does
%% not recognise untouched, and it is idempotent, so wrapping a function that
%% turns out not to be a callback costs a function call.
statem_return_pass(Forms) ->
    case lists:member(gen_statem, behaviours(Forms)) of
        false ->
            Forms;
        true ->
            Callbacks = statem_callbacks(Forms),
            [wrap_statem(F, Callbacks) || F <- Forms]
    end.

wrap_statem({function, Anno, Name, Arity, Clauses}, Callbacks) ->
    case sets:is_element({Name, Arity}, Callbacks) of
        true -> {function, Anno, Name, Arity, [wrap_statem_clause(C) || C <- Clauses]};
        false -> {function, Anno, Name, Arity, Clauses}
    end;
wrap_statem(Form, _Callbacks) ->
    Form.

wrap_statem_clause({clause, Anno, Pats, Guards, Body}) ->
    Call =
        {call, Anno, {remote, Anno, {atom, Anno, eta_net}, {atom, Anno, statem_return}}, [
            {block, Anno, Body}
        ]},
    {clause, Anno, Pats, Guards, [Call]}.

%% Every `{Name, Arity}` `gen_statem` may call as a state callback. Shared with
%% the observability pass, which needs the same set for the same reason.
statem_callbacks(Forms) ->
    Exported = exported(Forms),
    HandleEvent = sets:from_list([{handle_event, 4}]),
    States = sets:from_list([
        {N, 3}
     || {N, 3} <- sets:to_list(Exported), N =/= terminate
    ]),
    case statem_callback_mode(Forms) of
        handle_event_function -> sets:intersection(Exported, HandleEvent);
        state_functions -> States;
        unknown -> sets:union(States, sets:intersection(Exported, HandleEvent))
    end.

%% Only a literal is read. `callback_mode() -> state_functions.` and
%% `callback_mode() -> [handle_event_function, state_enter].` both resolve;
%% anything computed is `unknown`.
statem_callback_mode(Forms) ->
    case [Cs || {function, _, callback_mode, 0, Cs} <- Forms] of
        [[{clause, _, [], [], [Body]}]] -> mode_of(Body);
        _ -> unknown
    end.

mode_of({atom, _, state_functions}) ->
    state_functions;
mode_of({atom, _, handle_event_function}) ->
    handle_event_function;
mode_of({cons, _, H, T}) ->
    case mode_of(H) of
        unknown -> mode_of(T);
        Mode -> Mode
    end;
mode_of(_) ->
    unknown.

exported(Forms) ->
    case lists:any(fun is_export_all/1, Forms) of
        true -> local_functions(Forms);
        false -> sets:from_list(lists:append([FAs || {attribute, _, export, FAs} <- Forms]))
    end.

is_export_all({attribute, _, compile, export_all}) -> true;
is_export_all({attribute, _, compile, Opts}) when is_list(Opts) -> lists:member(export_all, Opts);
is_export_all(_) -> false.

net_enabled([{attribute, _, eta_net, false} | _]) -> false;
net_enabled([_ | Rest]) -> net_enabled(Rest);
net_enabled([]) -> true.

%% The same bottom-up structural walk `walk/2` uses. Kept separate rather than
%% folded into `?REWRITES` because that table is unconditional and this pass is
%% not: `-eta_net(false)` has to be able to turn it off without taking the timer
%% and spawn rewriting with it.
net_walk(Node, Locals) when is_tuple(Node) ->
    net_rewrite(list_to_tuple([net_walk(E, Locals) || E <- tuple_to_list(Node)]), Locals);
net_walk(Nodes, Locals) when is_list(Nodes) ->
    [net_walk(E, Locals) || E <- Nodes];
net_walk(Node, _Locals) ->
    Node.

%% The auto-imported monitor BIFs, handled the same way and for the same reason
%% as `?AUTO_SPAWNS`: `monitor(process, Pid)` written bare is `erlang:monitor/2`,
%% and leaving it alone means a monitor this module cannot see — which is a
%% partition that silently fails to fire it. Guarded on the module not defining
%% the name itself, since `monitor/2` is a name anyone may use.
-define(NET_AUTO, [
    {monitor, 2},
    {monitor, 3},
    {demonitor, 1},
    {demonitor, 2}
]).

%% `Dest ! Msg` is `{op, Anno, '!', Dest, Msg}` — the one rewrite here that is not
%% a call, and the one that matters most, because a raw send is how most Erlang
%% protocol code talks to a peer. `eta_net:send/2` returns `Msg`, which is what
%% `!` evaluates to, so the substitution is value-preserving and safe in any
%% expression position.
net_rewrite({op, Anno, '!', Dest, Msg}, _Locals) ->
    {call, Anno, {remote, Anno, {atom, Anno, eta_net}, {atom, Anno, send}}, [Dest, Msg]};
net_rewrite({call, Anno, {remote, RAnno, {atom, MAnno, Mod}, {atom, FAnno, Fun}}, Args}, _Locals) ->
    MFA = {Mod, Fun, length(Args)},
    case lists:member(MFA, ?NET_UNSUPPORTED) of
        true -> unsupported_call(Anno, MFA, Args);
        false -> net_rewrite_known(Anno, RAnno, MAnno, FAnno, Mod, Fun, Args)
    end;
net_rewrite({call, Anno, {atom, FAnno, Fun}, Args}, Locals) ->
    Key = {Fun, length(Args)},
    case lists:member(Key, ?NET_AUTO) andalso not sets:is_element(Key, Locals) of
        true ->
            {call, Anno, {remote, Anno, {atom, Anno, eta_net}, {atom, FAnno, Fun}}, Args};
        false ->
            {call, Anno, {atom, FAnno, Fun}, Args}
    end;
net_rewrite(Node, _Locals) ->
    Node.

%% `M:F(A, B)` becomes `eta_net:unsupported({M, F, 2}, [A, B])`.
%%
%% One target for every arity of every unsupported function, because it carries
%% the original MFA rather than encoding it in the target's name — which is also
%% what lets it call the original when no network is running.
unsupported_call(Anno, {M, F, A}, Args) ->
    MFA =
        {tuple, Anno, [
            {atom, Anno, M},
            {atom, Anno, F},
            {integer, Anno, A}
        ]},
    List = lists:foldr(
        fun(Arg, Acc) -> {cons, Anno, Arg, Acc} end,
        {nil, Anno},
        Args
    ),
    {call, Anno, {remote, Anno, {atom, Anno, eta_net}, {atom, Anno, unsupported}}, [MFA, List]}.

net_rewrite_known(Anno, RAnno, MAnno, FAnno, Mod, Fun, Args) ->
    case maps:find({Mod, Fun, length(Args)}, ?NET_REWRITES) of
        {ok, {Target, TargetFun}} ->
            {call, Anno, {remote, RAnno, {atom, MAnno, Target}, {atom, FAnno, TargetFun}}, Args};
        {ok, Target} ->
            {call, Anno, {remote, RAnno, {atom, MAnno, Target}, {atom, FAnno, Fun}}, Args};
        error ->
            {call, Anno, {remote, RAnno, {atom, MAnno, Mod}, {atom, FAnno, Fun}}, Args}
    end.

%% ---------------------------------------------------------------------------
%% Receive-timeout pass — applied only to a module declaring `-eta_after(true)`
%% ---------------------------------------------------------------------------

%% `receive Cs after T -> B end` is the five-element abstract form
%% `{'receive', Anno, Cs, T, B}`, so it is fully visible to a transform and can
%% be rewritten into a form with no real-time dependence at all:
%%
%%     begin
%%         Ref  = make_ref(),
%%         TRef = eta_time:arm_after(T, Ref),
%%         receive
%%             Pat1 -> eta_time:disarm_after(TRef, Ref), Body1;
%%             ...
%%             {'$eta_after', G} when G =:= Ref -> B
%%         end
%%     end
%%
%% The `after` block becomes an ordinary clause, so the wait is on a message and
%% the virtual clock can deliver it. Under `eta_sched` the waiting process is
%% correctly unrunnable until then, and a 60-second timeout costs microseconds.
after_pass(Forms) ->
    case after_enabled(Forms) of
        false ->
            Forms;
        true ->
            {Forms1, _Counter} = after_walk(Forms, 0),
            Forms1
    end.

%% On unless a module says otherwise. A knob you have to know about is a silent
%% determinism leak waiting to happen: without this, a module compiled with the
%% transform and holding one `receive ... after 5000` gets a real-clock timeout
%% under simulation, and nothing reports it. Opting out is for someone who has
%% measured a problem, not something to get right in advance.
after_enabled([{attribute, _, eta_after, false} | _]) -> false;
after_enabled([_ | Rest]) -> after_enabled(Rest);
after_enabled([]) -> true.

%% Postorder walk threading a counter, so the variables introduced for each
%% rewritten receive are unique within the module.
after_walk({'receive', Anno, Clauses, {integer, _, 0} = Zero, AfterBody}, N) ->
    %% An optimisation, not a correctness guard — `eta_time:arm_after/2` handles a
    %% zero timeout correctly however it arrives. Skipping the rewrite when the
    %% zero is visible at compile time just saves a ref, a send and a flush on
    %% every poll, and `after 0` tends to sit on hot paths.
    {Clauses1, N1} = after_walk(Clauses, N),
    {AfterBody1, N2} = after_walk(AfterBody, N1),
    {{'receive', Anno, Clauses1, Zero, AfterBody1}, N2};
after_walk({'receive', Anno, Clauses, Timeout, AfterBody}, N) ->
    {Clauses1, N1} = after_walk(Clauses, N),
    {Timeout1, N2} = after_walk(Timeout, N1),
    {AfterBody1, N3} = after_walk(AfterBody, N2),
    {after_expand(Anno, Clauses1, Timeout1, AfterBody1, N3), N3 + 1};
after_walk(Tuple, N) when is_tuple(Tuple) ->
    {List, N1} = after_walk(tuple_to_list(Tuple), N),
    {list_to_tuple(List), N1};
after_walk(List, N) when is_list(List) ->
    lists:mapfoldl(fun after_walk/2, N, List);
after_walk(Other, N) ->
    {Other, N}.

after_expand(Anno, Clauses, Timeout, AfterBody, N) ->
    RefV = after_var(Anno, "__DstAfterRef", N),
    TRefV = after_var(Anno, "__DstAfterTRef", N),
    GotV = after_var(Anno, "__DstAfterGot", N),

    %% A fresh variable compared in a guard, rather than matching the bound
    %% `Ref` directly. Matching it is correct and emits "variable is already
    %% bound" on every rewritten receive, which breaks `--warnings-as-errors`.
    TimeoutClause =
        {clause, Anno, [{tuple, Anno, [{atom, Anno, '$eta_after'}, GotV]}],
            [[{op, Anno, '=:=', GotV, RefV}]], AfterBody},

    %% The disarm goes at the head of each clause rather than after the receive.
    %% Saving the receive's value to disarm afterwards forces a block whose last
    %% expression is that saved value, which takes the receive out of tail
    %% position — for `loop() -> receive ... -> loop() end` that turns constant
    %% stack into unbounded growth. It also makes the disarm exception-safe for
    %% free, since it has already run before any body that might throw.
    Disarm = after_call(Anno, eta_time, disarm_after, [TRefV, RefV]),
    Clauses1 = [after_prepend(Disarm, C) || C <- Clauses],

    {block, Anno, [
        {match, Anno, RefV, after_call(Anno, erlang, make_ref, [])},
        {match, Anno, TRefV, after_call(Anno, eta_time, arm_after, [Timeout, RefV])},
        {'receive', Anno, Clauses1 ++ [TimeoutClause]}
    ]}.

after_prepend(Expr, {clause, Anno, Pats, Guards, Body}) ->
    {clause, Anno, Pats, Guards, [Expr | Body]}.

after_var(Anno, Prefix, N) ->
    {var, Anno, list_to_atom(Prefix ++ integer_to_list(N))}.

after_call(Anno, Mod, Fun, Args) ->
    {call, Anno, {remote, Anno, {atom, Anno, Mod}, {atom, Anno, Fun}}, Args}.

%% ---------------------------------------------------------------------------
%% Observability pass — applied only to a module declaring `-eta_observe(...)`
%% ---------------------------------------------------------------------------

%% The `gen_server` callbacks whose return value carries the state.
-define(WRAPPED, [
    {init, 1},
    {handle_call, 3},
    {handle_cast, 2},
    {handle_info, 2},
    {handle_continue, 2},
    {code_change, 3}
]).

-define(PUBLISH_FUN, '$eta_observe').
-define(PUT_FUN, '$eta_observe_put').

%% Applied only when the module declares `-eta_observe(...)`; otherwise the forms
%% pass through untouched.
observe_pass(Forms) ->
    case spec(Forms) of
        none ->
            Forms;
        Spec ->
            {Record, Fields} = resolve(Spec, records(Forms)),
            Anno = erl_anno:new(0),
            {Callbacks, Shapes} = observe_target(Forms),
            Wrapped = [wrap(F, Callbacks) || F <- Forms],
            insert_before_eof(Wrapped, helpers(Anno, Shapes, Record, Fields))
    end.

%% Which callbacks to republish from, and which return shapes carry the state.
%%
%% For a `gen_statem` the observed term is the **data**, not the state name: the
%% state name is one atom a run can read off the trace, and the data is what an
%% invariant is actually about. The callbacks are the same set the reply pass
%% wraps, plus `init/1` — a machine that publishes nothing until its first event
%% is one an invariant cannot check at step zero.
observe_target(Forms) ->
    case lists:member(gen_statem, behaviours(Forms)) of
        true -> {sets:add_element({init, 1}, statem_callbacks(Forms)), statem};
        false -> {sets:from_list(?WRAPPED), server}
    end.

%% ---------------------------------------------------------------------------
%% Reading the declaration
%% ---------------------------------------------------------------------------

spec([{attribute, _, eta_observe, Spec} | _]) -> Spec;
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

%% Two forms, and neither of them guesses a record name.
%%
%% `all` publishes whatever state the callback returned, whatever its shape.
%% `{Record, Fields}` publishes named fields, and is guarded on the record tag so
%% a callback returning something else publishes nothing rather than a wrong
%% `element/2` offset.
%%
%% There used to be a third form, a bare field list, which assumed the record was
%% called `state`. It is gone: a module whose record is `#st{}` got a compile
%% error naming a record it had never declared, and the reader has to know the
%% convention to understand why.
resolve(all, _Records) ->
    {any, all};
resolve({Record, Fields}, Records) when is_atom(Record), is_list(Fields) ->
    {Record, positions(Record, Fields, Records)};
resolve(Spec, _Records) ->
    error({eta_observe, {bad_spec, Spec, <<"expected all or {RecordName, [Field]}">>}}).

positions(Record, Fields, Records) ->
    Declared =
        case maps:find(Record, Records) of
            {ok, D} ->
                D;
            error ->
                error({eta_observe, {no_such_record, Record}})
        end,
    [{F, position(F, Record, Declared)} || F <- Fields].

%% A field that does not exist is a compile-time error rather than a silently
%% wrong `element/2` offset.
position(Field, Record, Declared) ->
    case index_of(Field, Declared, 2) of
        none -> error({eta_observe, {no_such_field, Record, Field, Declared}});
        N -> N
    end.

index_of(_F, [], _N) -> none;
index_of(F, [F | _], N) -> N;
index_of(F, [_ | Rest], N) -> index_of(F, Rest, N + 1).

%% ---------------------------------------------------------------------------
%% Rewriting
%% ---------------------------------------------------------------------------

wrap({function, Anno, Name, Arity, Clauses}, Callbacks) ->
    case sets:is_element({Name, Arity}, Callbacks) of
        true -> {function, Anno, Name, Arity, [wrap_clause(C) || C <- Clauses]};
        false -> {function, Anno, Name, Arity, Clauses}
    end;
wrap(Form, _Callbacks) ->
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

helpers(Anno, Shapes, Record, Fields) ->
    [publish_fun(Anno, Shapes), put_fun(Anno, Record, Fields)].

%% '$eta_observe'(Ret) -> _ = case Ret of ... end, Ret.
%%
%% Every gen_server return shape carries its state in a position the leading tag
%% disambiguates: {reply,_,S} against {stop,_,S} at arity three, {reply,_,S,_}
%% against {stop,_,_,S} at arity four.
%%
%% The gen_statem shapes are read the same way, with the **data** in the observed
%% position. `keep_state_and_data` and `repeat_state_and_data` carry nothing new
%% and fall through to the catch-all, which is right: they say the data is
%% unchanged, so the last publish still stands.
publish_fun(Anno, Shapes) ->
    Ret = {var, Anno, 'Ret'},
    S = {var, Anno, 'S'},
    Put = fun(Pats) ->
        {clause, Anno, [{tuple, Anno, Pats}], [], [
            {call, Anno, {atom, Anno, ?PUT_FUN}, [S]}
        ]}
    end,
    U = {var, Anno, '_'},
    A = fun(Name) -> {atom, Anno, Name} end,
    Clauses =
        case Shapes of
            server ->
                [
                    Put([A(reply), U, S]),
                    Put([A(reply), U, S, U]),
                    Put([A(noreply), S]),
                    Put([A(noreply), S, U]),
                    Put([A(stop), U, S]),
                    Put([A(stop), U, U, S]),
                    Put([A(ok), S]),
                    Put([A(ok), S, U])
                ];
            statem ->
                [
                    Put([A(next_state), U, S]),
                    Put([A(next_state), U, S, U]),
                    Put([A(keep_state), S]),
                    Put([A(keep_state), S, U]),
                    Put([A(repeat_state), S]),
                    Put([A(repeat_state), S, U]),
                    Put([A(stop), U, S]),
                    Put([A(stop_and_reply), U, U, S]),
                    %% init/1.
                    Put([A(ok), U, S]),
                    Put([A(ok), U, S, U])
                ]
        end,
    Body = [
        {match, Anno, U, {'case', Anno, Ret, Clauses ++ [{clause, Anno, [U], [], [A(ok)]}]}},
        Ret
    ],
    {function, Anno, ?PUBLISH_FUN, 1, [{clause, Anno, [Ret], [], Body}]}.

%% For `all`, one unguarded clause:
%%
%%     '$eta_observe_put'(S) -> put(Key, S), ok.
%%
%% For named fields, guarded on the record tag, with a fallback that publishes
%% nothing:
%%
%%     '$eta_observe_put'(S) when element(1, S) =:= Record -> put(Key, #{...}), ok;
%%     '$eta_observe_put'(_) -> ok.
put_fun(Anno, _Record, all) ->
    S = {var, Anno, 'S'},
    {function, Anno, ?PUT_FUN, 1, [
        {clause, Anno, [S], [], [put_call(Anno, S), {atom, Anno, ok}]}
    ]};
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
        {map, Anno, [
            {map_field_assoc, Anno, {atom, Anno, F},
                {call, Anno, {atom, Anno, element}, [{integer, Anno, N}, S]}}
         || {F, N} <- Fields
        ]},
    {function, Anno, ?PUT_FUN, 1, [
        {clause, Anno, [S], Guard, [put_call(Anno, Observed), {atom, Anno, ok}]},
        {clause, Anno, [{var, Anno, '_'}], [], [{atom, Anno, ok}]}
    ]}.

put_call(Anno, Term) ->
    {call, Anno, {remote, Anno, {atom, Anno, erlang}, {atom, Anno, put}}, [
        {atom, Anno, eta_observe:key()}, Term
    ]}.
