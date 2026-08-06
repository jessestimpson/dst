-module(eta_net).

%% `monitor/2,3` and `demonitor/1,2` are the API names, shadowing the
%% auto-imported BIFs. The originals are always reached qualified.
-compile({no_auto_import, [monitor/2, monitor/3, demonitor/1, demonitor/2]}).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
A simulated network: seeded message loss, delay and partitions between processes
in one VM.

`eta_sched` decides *who runs* and `eta_time` decides *when*. Neither decides
whether a message arrives, so a client that sends to three peers reaches all
three inside one scheduler step. This module owns delivery, and can drop, delay
or cut it.

Inert unless a network is running: every function here delegates to its ordinary
counterpart, so a module built with `eta_transform` behaves normally outside a
simulation.

## Reaching it

`send/2` is an ordinary exported function, and there are three ways to call it:

```erlang
Peer ! {replicate, Batch}.                     %% eta_transform rewrites this
eta_net:send(Peer, {replicate, Batch}).        %% or write it yourself
myapp_link:send(Peer, {replicate, Batch}).     %% or from your own transport module
```

Use the transform unless your system already has a transport module, in which
case call this from it and skip the header.

**Routing must be uniform per channel.** If some sends between two processes come
through here and others go direct, a direct message can overtake a delayed one —
reordering within an ordered pair, introduced by the harness and blamed on the
system. The transform is module-granular so a peer-facing module is either in or
out; a hand-written seam is call-site granular, and one `Pid ! ack` left unwrapped
in an error branch is enough to break it.

## The fault model

Only faults real Erlang can produce. Distribution guarantees that messages between
one ordered pair arrive in send order, and does not guarantee delivery at all. So
per ordered pair this module may **deliver**, **drop**, **delay**, or **cut** the
channel until healed. It never reorders within a pair; injecting that would
manufacture counterexamples the real system cannot produce.

A perfect policy is exactly the behaviour of no network at all, which is what
makes `drop_p => 0.0` usable as a control run.

**Loss comes with a signal.** A real link failure delivers `nodedown`/`nodeup` to
both ends and systems hang recovery off those, so `partition/3` and
`heal_partition/3` take one. Dropping messages without it injects something no
network produces. See `partition/3`.

## Where the network is: *located* and *faultable*

Two different questions, and `place/2` used to answer both at once.

- **Located** — which simulated node a process is on. A located process receives
  that node's link events (`partition/3`'s signal, a synthetic `noconnection`
  `DOWN`, `kill_node/2`'s signal) and dies with the node.
- **Faultable** — whether the network may drop or delay this process's messages.

`place/2` makes a process both. `attach/2` makes it located only.

The distinction is not a nicety, and the case that forces it is the common one.
Take a node that runs a member, an elector and a connector under one supervisor.
Only member-to-member traffic crosses the wire: the elector coordinates with its
peers through a durable store and the connector talks to nothing, so their
"messages" are store operations wearing the costume of a send. Dropping one
models a failure the store cannot produce — a database commits or errors, it does
not silently evaporate — and the divergence that follows looks exactly like the
replication defect a suite is hunting.

But the connector is where node-level failure detection lives. It owns
`nodedown`/`nodeup` and it must die when its node does. So it has to be *on* the
node without being *on the wire*, which is `attach/2`.

The alternative — place everything and narrow `scope` until the store traffic is
excluded — states the same thing in a form that has to be kept correct by hand as
the system grows a message. Topology is the durable statement; `scope` is what a
particular run is entitled to break.

With no topology declared at all, every link is faultable, so `place/2` is opt-in.

## Link events

A link failure is not only lost messages. Real distribution delivers two things
the schedule has to carry:

- **Node signals.** `partition/3`, `heal_partition/3` and `kill_node/2` take a
  `signal`, which reaches every *located* process on the affected side. See
  `partition/3` for the derived and literal forms.
- **Synthetic `noconnection` DOWNs.** Every monitor held across a failing link
  fires `{'DOWN', Ref, process, Object, noconnection}`. Under `eta_net` the target
  is still alive in the same VM, so the VM's own monitor never fires and nothing
  substitutes for it — which leaves every system whose failure detection runs on
  monitors untestable against a partition, even though the link fault itself
  works. `monitor/2` fixes that. See `monitor/2` and `kill_node/2`.

Both are delivered **directly** rather than routed, because a cut channel must not
be able to swallow the event that announces the cut, and both are enqueued
synchronously by the calling driver in a deterministic order — so the schedule
owns when they are *seen*, exactly as it does for any other message. Neither
draws from the fault RNG: adding a signal must not shift the fault schedule for
the traffic that can be faulted.

## Not `net_kernel`

A simulated node is a name in a table, not a node. `nodes()` will not list one,
`net_kernel:monitor_nodes/1` will not report one, `connect_node/1` will not reach
one, and `node(Pid)` still answers with the real node. This module delivers the
*events* a link or node failure produces and nothing else; a system under test
receives its `nodedown` because a harness sent it, not because distribution did.

A connector-shaped process that calls `net_kernel:monitor_nodes/1` therefore has
to be told about simulated nodes some other way — a `-ifdef(DST)` seam, or a
subscription function the harness can call. There is no way around it: emulating
`net_kernel` would mean emulating distribution, which `eta` does not do.

Delay is virtual — a delayed message becomes a deadline in `eta_time`'s wheel — so
waiting one out costs no real time.

*This documentation is LLM-generated. See the AI disclosure in `README.md`.*
""".
-endif.

-export([
    start/0,
    start/1,
    stop/0,
    running/0
]).

-export([
    send/2,
    send/3,
    cast/2,
    call/2,
    call/3,
    reply/1,
    reply/2,
    statem_return/1,
    unsupported/2
]).

%% Policy.
-export([
    set_policy/1,
    policy/0
]).

%% Topology — which simulated node each process is on. See `place/2`.
-export([
    place/2,
    attach/2,
    node_of/1,
    faultable/1,
    inherit/2
]).

%% Monitors. What `eta_transform` points a system's `erlang:monitor/2,3` at, so a
%% severed link can fire the `noconnection` DOWNs real distribution would.
-export([
    monitor/2,
    monitor/3,
    demonitor/1,
    demonitor/2,
    notify_exit/2
]).

%% Targeted faults.
-export([
    cut/2,
    heal/2,
    partition/2,
    partition/3,
    heal_partition/2,
    heal_partition/3,
    heal_all/0,
    kill_node/1,
    kill_node/2,
    drop_next/3,
    drop_matching/5
]).

%% Observation.
-export([
    stats/0,
    in_flight/0
]).

-export_type([policy/0, scope/0, signal/0, learns/0, event_opts/0]).

-define(STATE, eta_net_state).
-define(CHAN, eta_net_chan).
-define(PLACE, eta_net_place).
-define(CALLS, eta_net_calls).
-define(MONS, eta_net_mons).
%% Pids the scheduler has reported as exited. See `simulate_monitor/4` for why
%% this exists rather than asking the VM.
-define(DEAD, eta_net_dead).

-type dest() :: pid() | atom().

%% What a link event says. An atom is a *kind*, derived per side into
%% `{Kind, Other}`; anything else is delivered as written. See `partition/3`.
-type signal() :: atom() | {literal, term()} | term().

%% Which side of a partition observes the failure. See `partition/3`.
-type learns() :: both | a | b.

-type event_opts() :: #{signal => signal(), learns => learns()}.

-type scope() :: all | {tags, [term()]} | fun((pid(), dest(), term()) -> boolean()).

-type policy() :: #{
    drop_p => float(),
    delay_p => float(),
    max_delay => pos_integer(),
    scope => scope()
}.

-define(DEFAULT_POLICY, #{
    drop_p => 0.0,
    delay_p => 0.0,
    max_delay => 20,
    scope => all
}).

%% `signalled` and `noconnection` count link *events* rather than traffic, and are
%% what a non-vacuity guard on a partition asserts on: a run whose partition
%% reached nobody exercised no recovery path.
-define(COUNTERS, [delivered, dropped, delayed, cancelled, unmanaged, signalled, noconnection]).

%% ---------------------------------------------------------------------------
%% Lifecycle
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc "Starts a network with a perfect policy. See `start/1`.".
-endif.
-spec start() -> ok.
start() ->
    start(#{}).

-if(?DOCATTRS).
-doc """
Starts the network.

Options:

- `seed` — seeds the fault schedule, so a run replays.
- `policy` — see `set_policy/1`. Defaults to a perfect network, which is what a
  cluster needs while it is still starting up.

One network per VM, as with `eta_time` and `eta_log`, so runs must be serial.
Starting over a running network resets it; starting over one another live process
owns raises.
""".
-endif.
-spec start(#{seed => integer(), policy => policy()}) -> ok.
start(Opts) ->
    ok = claim(),
    stop(),
    ?STATE = ets:new(?STATE, [named_table, public, set]),
    ?CHAN = ets:new(?CHAN, [named_table, public, set]),
    ?PLACE = ets:new(?PLACE, [named_table, public, set]),
    ?CALLS = ets:new(?CALLS, [named_table, public, set]),
    ?MONS = ets:new(?MONS, [named_table, public, set]),
    ?DEAD = ets:new(?DEAD, [named_table, public, set]),
    Seed = maps:get(seed, Opts, 0),
    ets:insert(?STATE, [
        {rand, rand:seed_s(exsss, {Seed, Seed + 11, Seed + 23})},
        %% Not the fault RNG and not a clock: a monotonic counter, stamped onto
        %% every placement and every monitor, so that a fan-out of link events can
        %% be ordered by something the run itself decided. See `endpoints/1`.
        {seq, 0},
        {policy, merge_policy(maps:get(policy, Opts, #{}))}
        | [{C, 0} || C <- ?COUNTERS]
    ]),
    ok.

%% The existence of the named table is the lock: the VM destroys it when its owner
%% exits. `stop/0` is deliberately unguarded, so an `on_exit` running in another
%% process still cleans up.
claim() ->
    case ets:info(?STATE, owner) of
        undefined ->
            ok;
        Self when Self =:= self() ->
            ok;
        Other ->
            error(
                {eta_net,
                    {network_in_use, Other, <<
                        "one per VM; keep runs serial (`async: false`) and do "
                        "not drive two at once"
                    >>}}
            )
    end.

-if(?DOCATTRS).
-doc """
Stops the network. Safe to call when none is running.

Messages already in flight are ordinary `eta_time` timers by then, and are still
delivered by whatever advances the clock next.
""".
-endif.
-spec stop() -> ok.
stop() ->
    lists:foreach(
        fun(Tab) ->
            try
                ets:delete(Tab)
            catch
                error:badarg -> ok
            end
        end,
        [?STATE, ?CHAN, ?PLACE, ?CALLS, ?MONS, ?DEAD]
    ),
    ok.

-if(?DOCATTRS).
-doc "Whether a network is running. When false, every function here delegates.".
-endif.
-spec running() -> boolean().
running() ->
    ets:whereis(?STATE) =/= undefined.

%% ---------------------------------------------------------------------------
%% The seam
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc """
Sends a message, subject to the network.

Returns `Msg`, so it is a drop-in for both `Dest ! Msg` and `erlang:send/2`.

A destination this module cannot identify as a local process — `{global, _}`,
`{via, _, _}`, a remote node — is passed through untouched. `eta` does not
simulate distribution.
""".
-endif.
-spec send(dest() | term(), term()) -> term().
send(Dest, Msg) ->
    case running() andalso normalize(Dest) of
        To when is_pid(To) -> route(self(), To, Msg);
        _ -> erlang:send(Dest, Msg)
    end.

-if(?DOCATTRS).
-doc """
`erlang:send/3`. The options are for distribution and have no meaning between two
processes in one VM, so a routed send ignores them.
""".
-endif.
-spec send(dest() | term(), term(), list()) -> ok | nosuspend | noconnect.
send(Dest, Msg, Opts) ->
    case running() andalso normalize(Dest) of
        To when is_pid(To) ->
            _ = route(self(), To, Msg),
            ok;
        _ ->
            erlang:send(Dest, Msg, Opts)
    end.

-if(?DOCATTRS).
-doc """
`gen_server:cast/2`, routed. Never raises, including for an unregistered name.
""".
-endif.
-spec cast(dest() | term(), term()) -> ok.
cast(Dest, Msg) ->
    case running() andalso normalize(Dest) of
        To when is_pid(To) ->
            %% Tagged by the payload rather than by OTP's `'$gen_cast'` envelope,
            %% which every cast carries and so would select all of them or none.
            catch route(self(), To, {'$gen_cast', Msg}, tag(Msg)),
            ok;
        _ ->
            gen_server:cast(Dest, Msg)
    end.

-if(?DOCATTRS).
-doc """
`gen_server:call/2,3` and `gen_statem:call/2,3`, with **both legs on the
network**. One target for both, because the request is the same
`{'$gen_call', From, Request}` either way.

The request is an ordinary send this module routes. The reply cannot be reached
from here — `gen:reply` sends it from inside OTP — so `eta_transform` brings it
on from the other end, rewriting the callee's returns: a `gen_server`'s
`{reply, R, S}` becomes `eta_net:reply(From, R)` plus `{noreply, S}`, and a
`gen_statem`'s `{reply, From, R}` action is taken out of the action list and sent
through here instead. See `statem_return/1`.

Both legs can therefore be dropped or delayed independently, which is what makes
the asymmetric fault reachable: the work happened, the caller never learned it
did.

Monitors the callee and takes a timeout, as `gen_server:call/3` does, with two
differences:

- **The timeout is virtual**, so waiting one out on a dropped request costs no
  real time and fires where the schedule chose.
- **The monitor is `eta_net`'s own and is deliberately left real**, rather than
  going through `monitor/2`. It exists to notice a callee that died, and it has
  to work whatever the scheduler is or is not reporting — this is the path that
  turns a dead callee into an exit instead of a hang. The cost is that a call
  outstanding across a `partition/3` ends in its (virtual) timeout rather than in
  `noconnection`, which is a known simplification. Monitors the *system* holds
  are simulated and do get `noconnection`; see `monitor/2`.

**Raises `unrouted_reply` if the callee was not built with the transform**, since
its reply then comes around the network and one direction of the channel is
silently unfaultable.
""".
-endif.
-spec call(dest() | term(), term()) -> term().
call(Dest, Req) ->
    call(Dest, Req, 5000).

-spec call(dest() | term(), term(), timeout() | {clean_timeout | dirty_timeout, timeout()}) ->
    term().
call(Dest, Req, Timeout) ->
    case running() andalso normalize(Dest) of
        To when is_pid(To) -> do_call(To, Dest, Req, call_timeout(Timeout));
        _ -> gen_server:call(Dest, Req, call_timeout(Timeout))
    end.

%% `gen_statem:call/3` accepts `{clean_timeout, T}` and `{dirty_timeout, T}` as
%% well as a plain `timeout()`. Both choose how the *caller* waits — whether the
%% wait happens in a proxy process — and neither is meaningful here: this module
%% waits in a receive of its own, on the virtual clock. Unwrapping to the number
%% is the honest reading, and it keeps `call/3` a single target for both
%% behaviours.
call_timeout({clean_timeout, T}) -> T;
call_timeout({dirty_timeout, T}) -> T;
call_timeout(T) -> T.

do_call(To, Dest, Req, Timeout) ->
    Tag = make_ref(),
    Mon = erlang:monitor(process, To),
    %% Before the request goes out, so a reply cannot beat the registration and be
    %% mistaken for an unrouted one.
    true = ets:insert(?CALLS, {Tag, outstanding}),
    _ = route(self(), To, {'$gen_call', {self(), Tag}, Req}, tag(Req)),
    AfterRef = make_ref(),
    TRef = eta_time:arm_after(Timeout, AfterRef),
    receive
        {Tag, Reply} ->
            ok = eta_time:disarm_after(TRef, AfterRef),
            true = erlang:demonitor(Mon, [flush]),
            ok = check_routed(Tag, To, Req),
            Reply;
        {'DOWN', Mon, process, _, Reason} ->
            ok = eta_time:disarm_after(TRef, AfterRef),
            true = ets:delete(?CALLS, Tag),
            exit({Reason, {eta_net, call, [Dest, Req, Timeout]}});
        {'$eta_after', G} when G =:= AfterRef ->
            true = erlang:demonitor(Mon, [flush]),
            true = ets:delete(?CALLS, Tag),
            exit({timeout, {eta_net, call, [Dest, Req, Timeout]}})
    end.

%% `reply/2` takes the tag out on its way past, so a tag still here when the reply
%% lands means the callee answered by a path this module never saw.
check_routed(Tag, To, Req) ->
    case ets:take(?CALLS, Tag) of
        [] ->
            ok;
        [_] ->
            error(
                {eta_net,
                    {unrouted_reply, To, tag(Req), <<
                        "the callee was not built with eta_transform, so its reply "
                        "came around the network and cannot be faulted. Build it with "
                        "the transform, or place both ends on one node"
                    >>}}
            )
    end.

-if(?DOCATTRS).
-doc """
`gen_server:reply/2` and `gen_statem:reply/2`, routed — so a reply can be lost
independently of the request that caused it.

Addressed to the caller's pid rather than to the alias in the tag. Both land in
the same mailbox and the caller's selective receive matches on the tag either
way.

`eta_transform` also rewrites a `gen_server`'s `handle_call/3` returns and a
`gen_statem`'s reply actions to reach this, which is how the reply leg of
`call/3` gets onto the network.
""".
-endif.
-spec reply(term(), term()) -> ok.
reply(From = {To, Tag}, Reply) when is_pid(To) ->
    case running() of
        true ->
            %% Tagged `'$gen_reply'` rather than by the caller's tag, which is a
            %% fresh reference per call and so names nothing a policy could scope
            %% on. Deleting marks the call answered through the network — `call/3`
            %% uses the absence to detect a reply that came around it.
            true = ets:delete(?CALLS, Tag),
            _ = route(self(), To, {Tag, Reply}, '$gen_reply'),
            ok;
        false ->
            gen_server:reply(From, Reply)
    end;
reply(From, Reply) ->
    gen_server:reply(From, Reply).

-if(?DOCATTRS).
-doc """
`gen_statem:reply/1`, routed. Takes one `{reply, From, Reply}` or a list of them,
and puts each on the network through `reply/2`.
""".
-endif.
-spec reply([{reply, term(), term()}] | {reply, term(), term()}) -> ok.
reply({reply, From, Reply}) ->
    reply(From, Reply);
reply(Replies) when is_list(Replies) ->
    lists:foreach(fun(R) -> reply(R) end, Replies).

-if(?DOCATTRS).
-doc """
Puts the **reply** leg of a `gen_statem:call` on the network, given a state
callback's return value.

The `gen_server` half of this job needs the `From` out of the callback's
arguments, so `eta_transform` generates a per-module helper for it. A
`gen_statem` names its caller inside the return instead — a `{reply, From, Msg}`
action — so the whole job is a function of the return value alone and lives
here.

Every action-carrying return shape is recognised. Each `{reply, From, Msg}`
action is sent through `reply/2` and removed from the list, so `gen_statem` does
not also send it directly:

```erlang
{next_state, S, D, [{reply, From, R}, Other]}  %% -> eta_net:reply(From, R),
                                               %%    {next_state, S, D, [Other]}
{stop_and_reply, Reason, [{reply, From, R}]}   %% -> eta_net:reply(From, R),
                                               %%    {stop_and_reply, Reason, []}
```

Anything else is returned untouched, including a return with no actions and a
return shape this does not know. Idempotent, so a return that has already passed
through — from a helper function the transform also wrapped — is unchanged the
second time.

Actions may be a single action rather than a list, which is why the bare
`{reply, _, _}` clause exists.
""".
-endif.
-spec statem_return(term()) -> term().
statem_return({next_state, S, D, A}) -> {next_state, S, D, statem_actions(A)};
statem_return({keep_state, D, A}) -> {keep_state, D, statem_actions(A)};
statem_return({keep_state_and_data, A}) -> {keep_state_and_data, statem_actions(A)};
statem_return({repeat_state, D, A}) -> {repeat_state, D, statem_actions(A)};
statem_return({repeat_state_and_data, A}) -> {repeat_state_and_data, statem_actions(A)};
statem_return({stop_and_reply, R, A}) -> {stop_and_reply, R, statem_actions(A)};
statem_return({stop_and_reply, R, A, D}) -> {stop_and_reply, R, statem_actions(A), D};
statem_return(Ret) -> Ret.

statem_actions([{reply, From, Msg} | T]) ->
    ok = reply(From, Msg),
    statem_actions(T);
statem_actions([H | T]) ->
    [H | statem_actions(T)];
statem_actions([]) ->
    [];
statem_actions({reply, From, Msg}) ->
    ok = reply(From, Msg),
    [];
statem_actions(Other) ->
    Other.

-if(?DOCATTRS).
-doc """
What `eta_transform` points the messaging functions this module does not
implement at: broadcasts and multi-node calls, the asynchronous
request/response interface, and the `gen_event` client API. The list is
`?NET_UNSUPPORTED` in `eta_transform`.

Raises while a network is running, and calls the original otherwise — so a module
holding one on a path no simulation reaches still builds and behaves normally.

Raising rather than passing through is deliberate: a run states a fault model,
and a channel the model silently fails to cover makes the suite green for the
wrong reason.
""".
-endif.
-spec unsupported({module(), atom(), arity()}, [term()]) -> term().
unsupported({M, F, _A} = MFA, Args) ->
    case running() of
        false ->
            apply(M, F, Args);
        true ->
            error(
                {eta_net,
                    {unsupported, MFA, <<
                        "eta_net cannot put this function's traffic on the network yet, "
                        "so a run that used it would be faulting less than it claims. "
                        "Use call/2,3, cast/2 or reply/2 on gen_server or gen_statem, "
                        "which are routed, "
                        "or run without a network"
                    >>}}
            )
    end.

%% A channel is a pair of pids, and every way of addressing a local process
%% normalizes to one. Required for correctness rather than convenience: a system
%% that sometimes sends to `Peer` and sometimes to `{peer, node()}` would
%% otherwise open two channels to one process, each with its own ordering clamp.
%%
%% A name resolves **at send time**, unlike `erlang:send/2` which resolves on
%% delivery. Only observable if the name is re-registered while a message to it is
%% in flight.
%%
%% `undefined` means "not a local process this module can identify", and the
%% caller falls back to sending directly.
normalize(Pid) when is_pid(Pid) -> Pid;
normalize(Name) when is_atom(Name) -> erlang:whereis(Name);
normalize({Name, Node}) when is_atom(Name), Node =:= node() -> erlang:whereis(Name);
normalize(_) -> undefined.

%% ---------------------------------------------------------------------------
%% Policy
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc """
Sets the random fault policy. Anything omitted keeps its current value.

- `drop_p` — probability that an in-scope message is lost.
- `delay_p` — probability that it is instead delivered late.
- `max_delay` — upper bound, in **virtual** milliseconds, on a delay.
- `scope` — which traffic the probabilities apply to:
  - `all` (the default),
  - `{tags, [Tag]}` — messages carrying one of these tags,
  - `fun(From, Dest, Tag) -> boolean()`.

A message's **tag** is its leading element, except that `cast/2` and `reply/2`
name the payload rather than OTP's envelope: a cast of `{prepare, TxId}` is
tagged `prepare`, not `'$gen_cast'`, and every reply is tagged `'$gen_reply'`.

**Scope states which recovery paths a run is entitled to exercise**, rather than
tuning how hard it tries. Loss on a channel whose recovery depends on a node
event cannot be asserted against mid-run, because nothing delivers that event
until the heal; a run that checks a property *during* a fault has to restrict
loss to a channel that repairs itself, such as a replication stream a follower
rejoins from a version discontinuity.

An out-of-scope message draws nothing from the RNG, so narrowing the scope does
not shift the fault schedule for the traffic still inside it.
""".
-endif.
-spec set_policy(policy()) -> ok.
set_policy(Policy) ->
    ok = require_running(set_policy),
    Merged = maps:merge(policy(), maps:with(maps:keys(?DEFAULT_POLICY), Policy)),
    true = ets:insert(?STATE, {policy, Merged}),
    ok.

-if(?DOCATTRS).
-doc "The policy currently in force.".
-endif.
-spec policy() -> policy().
policy() ->
    ets:lookup_element(?STATE, policy, 2).

-if(?DOCATTRS).
-doc """
Puts processes on a simulated node, **on the wire** — how a system says where its
network is.

Faults apply to a send only when both ends are placed and their nodes differ, so
the fault model follows the topology rather than a predicate the harness has to
keep correct.

```erlang
eta_net:place(node_a, [MemberA]),
eta_net:place(node_b, [MemberB]).
```

A placed process is both *located* and *faultable*: it receives its node's link
events and dies with it (`kill_node/2`), and its traffic can be dropped, delayed
or cut. `attach/2` gives the first without the second; see the module doc on why
those come apart.

**Unplaced means not on the network**: never faulted, in either direction, and it
receives no link events. That is the safe default and also keeps a network inert
for processes it does not know about, such as leftovers from an earlier test in
the same VM. It is *not* the way to say "on this node but not on the wire" —
`attach/2` is, because an unplaced process is not on any node at all and so
learns nothing when one fails.

If nothing at all is placed, every link is faultable, so `place/2` is opt-in.

**Children inherit** their parent's node *and its faultability* as `eta_sched`
adopts them, so a worker spawned mid-run does not send across a link the network
was never told about — nor acquire one its parent was deliberately kept off.

Placing a process that is already placed moves it and keeps its position in the
link-event fan-out; see `partition/3`.
""".
-endif.
-spec place(term(), [dest()]) -> ok.
place(Node, Pids) ->
    do_place(place, Node, Pids, true).

-if(?DOCATTRS).
-doc """
Puts processes on a simulated node **without putting them on the wire**: they
receive the node's link events and die with it, and their traffic is never
faulted.

```erlang
eta_net:place(node_a, [MemberA]),          %% talks to peers over the network
eta_net:attach(node_a, [ElectorA, ConnA]). %% on the node, off the wire
```

This is the honest way to model a process whose "messages" are not messages. An
elector that coordinates through a durable store, a connector that only watches
node liveness — dropping their sends injects a failure the real system cannot
have, and the divergence that follows is an artefact that reads exactly like a
replication defect. But both must still learn when their node's peers go away,
and both must die when the node does, so leaving them unplaced is wrong too.

Faultability is per process, not per channel: a send is faulted only when **both**
ends are faultable, so an attached process is safe in either direction.

Otherwise identical to `place/2` — same node names, same inheritance, same
participation in `partition/3` and `kill_node/2`.
""".
-endif.
-spec attach(term(), [dest()]) -> ok.
attach(Node, Pids) ->
    do_place(attach, Node, Pids, false).

do_place(What, Node, Pids, Faultable) ->
    ok = require_running(What),
    true = ets:insert(?PLACE, {{node, Node}, true}),
    lists:foreach(
        fun(D) ->
            case normalize(D) of
                P when is_pid(P) ->
                    true = ets:insert(?PLACE, {{pid, P}, Node, Faultable, seq_of(P)});
                _ ->
                    ok
            end
        end,
        Pids
    ),
    ok.

%% A process keeps the sequence number it was first given, so re-placing one — a
%% harness restating its topology after a restart, say — does not reshuffle the
%% order link events are delivered in.
seq_of(Pid) ->
    case ets:lookup(?PLACE, {pid, Pid}) of
        [{_, _Node, _F, Seq}] -> Seq;
        [] -> next_seq()
    end.

next_seq() ->
    ets:update_counter(?STATE, seq, 1).

-if(?DOCATTRS).
-doc "The simulated node a process is on, or `undefined`. See `place/2`.".
-endif.
-spec node_of(dest()) -> term() | undefined.
node_of(Dest) ->
    case placement(normalize(Dest)) of
        {Node, _Faultable} -> Node;
        undefined -> undefined
    end.

-if(?DOCATTRS).
-doc """
Whether the network may fault this process's traffic.

`false` for an unplaced process and for one added with `attach/2`; `true` for a
placed one, and for everything when no topology has been declared at all. See
`place/2`.
""".
-endif.
-spec faultable(dest()) -> boolean().
faultable(Dest) ->
    case ets:info(?PLACE, size) of
        0 ->
            true;
        _ ->
            case placement(normalize(Dest)) of
                {_Node, Faultable} -> Faultable;
                undefined -> false
            end
    end.

%% `{Node, Faultable}` or `undefined`. The one place the table's row shape is
%% read, so the rest of the module does not care what it is.
placement(Pid) ->
    case ets:lookup(?PLACE, {pid, Pid}) of
        [{_, Node, Faultable, _Seq}] -> {Node, Faultable};
        [] -> undefined
    end.

-if(?DOCATTRS).
-doc """
Places `Child` where `Parent` is, with the same faultability. Called by
`eta_sched` as it adopts a process; a harness does not call this. See `place/2`.
""".
-endif.
-spec inherit(pid(), pid()) -> ok.
inherit(Parent, Child) ->
    case running() andalso placement(Parent) of
        {Node, Faultable} ->
            true = ets:insert(?PLACE, {{pid, Child}, Node, Faultable, seq_of(Child)}),
            ok;
        _ ->
            ok
    end.

%% Whether a send crosses a simulated link, and so is something the policy may
%% fault. Both ends must be located, on different nodes, and faultable — an
%% attached process is on the node but not on the wire, so nothing it sends or
%% receives is ever a candidate. No topology at all means every link is faultable.
crosses_link(From, Dest) ->
    case ets:info(?PLACE, size) of
        0 ->
            true;
        _ ->
            case {placement(normalize(From)), placement(normalize(Dest))} of
                {{A, true}, {B, true}} when A =/= B -> true;
                {_, _} -> false
            end
    end.

%% A reference to cut or partition: a node if it names one, otherwise a process.
%% Only an atom is ambiguous — a node named the same as a registered process — and
%% nodes win; pass a pid to disambiguate.
resolve(Ref) when is_atom(Ref) ->
    case ets:member(?PLACE, {node, Ref}) of
        true -> {node, Ref};
        false -> {pid, normalize(Ref)}
    end;
resolve(Ref) ->
    {pid, normalize(Ref)}.

%% Every process on a node, for delivering a link signal, **in placement order**.
%%
%% `ets:match/2` returns rows in whatever order the table hands them over, which
%% is neither specified nor stable, so the raw list is unusable for anything the
%% schedule depends on: a fan-out of link events delivered in table order would
%% put a different sequence of messages in different mailboxes from run to run,
%% and a seed would stop reproducing itself. Sorting by the placement sequence
%% orders it by something the run decided instead — the order the harness placed
%% them in, and for children the order they were spawned in.
on_node(Node) ->
    in_seq_order(ets:match(?PLACE, {{pid, '$1'}, Node, '_', '$2'})).

%% Every located process, in placement order. See `on_node/1`.
located() ->
    in_seq_order(ets:match(?PLACE, {{pid, '$1'}, '_', '_', '$2'})).

in_seq_order(Matched) ->
    [P || {_Seq, P} <- lists:sort([{Seq, P} || [P, Seq] <- Matched])].

merge_policy(Policy) ->
    maps:merge(?DEFAULT_POLICY, maps:with(maps:keys(?DEFAULT_POLICY), Policy)).

%% Configuring or inspecting a network needs one to exist. `send/2` and friends
%% are deliberately *not* guarded: their contract is to be inert without one.
require_running(What) ->
    case running() of
        true ->
            ok;
        false ->
            error(
                {eta_net,
                    {no_network, What, <<
                        "no network is running; pass `net => true` (or "
                        "`net => #{policy => ...}`) to eta_run:run/2, or call "
                        "eta_net:start/1 first"
                    >>}}
            )
    end.

%% ---------------------------------------------------------------------------
%% Monitors
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc """
`erlang:monitor/2`, aware of the simulated network.

Most systems detect a failed peer with a monitor rather than with a timeout, so
without this a partition is invisible to exactly the code it should be exercising:
the target is alive in the same VM, the VM's monitor stays armed, and the link
fault drops messages into a system that never learns why.

`eta_transform` points a module's `erlang:monitor/2,3` and `demonitor/1,2` here,
so ordinary code needs no change.

## What is simulated, and what is left to the VM

A monitor is **simulated** when, at the moment it is created, watcher and target
are located on *different* simulated nodes. Everything else — same node, either
end unplaced, no topology at all, a monitor on a port or a `time_offset`, or one
asking for an `alias` — is a plain `erlang:monitor` and behaves exactly as it
always did.

That line is deliberately narrow, and the reason is which way each choice fails.
A simulated monitor is the only kind that can be made to fire `noconnection`,
because the VM will not let one process cancel another's monitor and will not let
anyone rewrite a `DOWN` already on its way — so a monitor that stayed real would
report `killed` to a remote watcher, or report twice. But a simulated monitor
depends on this module being *told* the target died (see `notify_exit/2`), and if
that ever fails to arrive the watcher waits forever. Simulating only the monitors
that need it keeps that exposure to the pairs a partition can actually sever, and
leaves everything else on machinery that cannot go wrong.

The consequence worth stating: a monitor created **before** its ends are placed is
a real one and will not fire `noconnection`. Place the topology before the system
starts monitoring — `init/2`, alongside registration — which is where it belongs
anyway.

## When a simulated monitor fires

Exactly once, then it is gone, as a real one is:

- `{'DOWN', Ref, process, Object, noconnection}` when `partition/3` or
  `kill_node/2` severs the link between watcher and target.
- `{'DOWN', Ref, process, Object, Reason}` when the target exits, with the real
  reason, reported by `eta_sched`. See `notify_exit/2`.
- `{'DOWN', Ref, process, Object, noproc}` immediately, if the target is already
  dead when the monitor is created.

`Object` follows `erlang:monitor/2`: the pid for a pid, `{Name, node()}` for a
registered name. `{tag, Tag}` in the options replaces `'DOWN'`, as it does for the
BIF.

**A `noconnection` DOWN is terminal.** Real Erlang does not re-arm a monitor
after one, and neither does this: `heal_partition/3` resurrects nothing, and a
system that wants to keep watching must monitor again. By the same rule a monitor
created *while* a link is already cut is not retroactively severed — the DOWN
marks the moment the link failed, and it has already passed.
""".
-endif.
-spec monitor(process | port | time_offset, term()) -> reference().
monitor(Type, Item) ->
    monitor(Type, Item, []).

-spec monitor(process | port | time_offset, term(), list()) -> reference().
monitor(process, Item, Opts) ->
    case running() andalso not lists:keymember(alias, 1, Opts) of
        true -> do_monitor(Item, Opts);
        false -> erlang:monitor(process, Item, Opts)
    end;
monitor(Type, Item, Opts) ->
    erlang:monitor(Type, Item, Opts).

do_monitor(Item, Opts) ->
    Watcher = self(),
    case normalize(Item) of
        Target when is_pid(Target) ->
            case simulated_pair(Watcher, Target) of
                true -> simulate_monitor(Watcher, Target, Item, Opts);
                false -> erlang:monitor(process, Item, Opts)
            end;
        undefined ->
            %% An unregistered name, or a destination this module cannot identify
            %% as a local process. `erlang:monitor/2` already does the right thing
            %% with both — an immediate `noproc` for the first, a real distributed
            %% monitor for the second — and neither is something to reimplement.
            erlang:monitor(process, Item, Opts)
    end.

simulate_monitor(Watcher, Target, Item, Opts) ->
    Ref = make_ref(),
    Mon = {Ref, Watcher, Target, object(Item), down_tag(Opts), next_seq()},
    true = ets:insert(?MONS, Mon),
    %% `erlang:monitor/2` on a dead process delivers `noproc` at once rather than
    %% never, and code that waits for a DOWN it will not otherwise get relies on
    %% it.
    %%
    %% **Liveness comes from `notify_exit/2`'s record, not from the VM**, and
    %% neither obvious BIF will do.
    %%
    %% `erlang:is_process_alive/1` and `erlang:process_info/2` are both
    %% signal-based against a live target: to keep their answer ordered against
    %% signals the caller has already sent it, they ask the target and wait for a
    %% reply — and a reply is something the target produces by *running*. Every
    %% target here is one `eta_sched` owns, so it is suspended, and the watcher is
    %% exactly the kind of process that has been sending it things. The watcher's
    %% progress therefore came to depend on when its target was next stepped, which
    %% is the VM ordering two processes where the schedule is supposed to. It cost
    %% about one seed in forty its reproducibility, and showed up as a member
    %% parked in `erts_internal:await_result/1` at a choice point — first under
    %% `is_process_alive/1`, then in the same place under `process_info/2`, which
    %% is what ruled out swapping one for the other.
    %%
    %% `eta_sched` reports every exit it sees, and every process that can be a
    %% target here is one it owns — `simulated_pair/2` requires both ends placed —
    %% so that report is the authority. Reading it asks the target for nothing.
    case target_dead(Target) of
        true -> fire(Mon, noproc);
        false -> ok
    end,
    Ref.

%% Whether a monitor's target has already exited.
%%
%% Which answer is authoritative depends on whether a scheduler is running, and
%% that is the subtlety. Under one, nothing may ask a suspended process a question
%% (see above) and nothing needs to, because the scheduler reports every exit
%% through `notify_exit/2`. Without one — driving a network by hand, which this
%% module supports — no exit is ever reported, but nothing is suspended either, so
%% the VM is safe to ask.
target_dead(Target) ->
    ets:member(?DEAD, Target) orelse
        (eta_sched:current() =:= undefined andalso not erlang:is_process_alive(Target)).

%% Whether this pair is one a severed link could separate: both located, on
%% different simulated nodes. Faultability is not consulted — it decides whose
%% *messages* may be dropped, and a monitor is not a message. An attached process
%% is behind the same wire as everything else on its node and its monitors fail
%% with it.
simulated_pair(Watcher, Target) ->
    case {placement(Watcher), placement(Target)} of
        {{A, _}, {B, _}} when A =/= B -> true;
        {_, _} -> false
    end.

%% What `erlang:monitor/2` puts in the DOWN message's `Object` position.
object(Pid) when is_pid(Pid) -> Pid;
object(Name) when is_atom(Name) -> {Name, node()};
object({Name, Node}) when is_atom(Name), is_atom(Node) -> {Name, Node};
object(Other) -> Other.

down_tag(Opts) ->
    case lists:keyfind(tag, 1, Opts) of
        {tag, Tag} -> Tag;
        false -> 'DOWN'
    end.

-if(?DOCATTRS).
-doc "`erlang:demonitor/1`. See `monitor/2`.".
-endif.
-spec demonitor(reference()) -> true.
demonitor(Ref) ->
    _ = demonitor(Ref, []),
    true.

-if(?DOCATTRS).
-doc """
`erlang:demonitor/2`, including `flush` and `info`. See `monitor/2`.

A simulated monitor that has already fired behaves as a real fired one does:
`true`, or `false` under `info`, and never a raise.
""".
-endif.
-spec demonitor(reference(), list()) -> boolean().
demonitor(Ref, Opts) ->
    case running() andalso ets:take(?MONS, Ref) of
        [{Ref, _W, _T, _O, Tag, _Seq}] ->
            flush_down(lists:member(flush, Opts), Tag, Ref),
            result(Opts, true);
        _ ->
            demonitor_real(Ref, Opts)
    end.

demonitor_real(Ref, Opts) ->
    try
        erlang:demonitor(Ref, Opts)
    catch
        error:badarg when is_reference(Ref) ->
            %% A simulated monitor that already fired: its row is gone and the
            %% ref was never a VM monitor, so the BIF cannot recognise it. Real
            %% Erlang does not raise for a fired monitor and neither may we, or a
            %% system tidying up after a DOWN would crash on the tidying.
            flush_down(lists:member(flush, Opts), 'DOWN', Ref),
            result(Opts, false)
    end.

result(Opts, Found) ->
    case lists:member(info, Opts) of
        true -> Found;
        false -> true
    end.

flush_down(false, _Tag, _Ref) ->
    ok;
flush_down(true, Tag, Ref) ->
    receive
        {Tag, Ref, _Type, _Object, _Reason} -> ok
    after 0 -> ok
    end.

-if(?DOCATTRS).
-doc """
Tells the network a process has exited, so simulated monitors on it can fire.

Called by `eta_sched` from the exit trace event, not by a harness. Inert when no
network is running.

## Why the scheduler is the source

A simulated monitor has no VM monitor behind it, so something has to notice the
target died. Every candidate but this one is worse:

- A **broker process** holding real monitors would learn at the right moment and
  forward at the wrong one — it is not a process the scheduler owns, so its
  forwarded DOWN lands whenever the BEAM happens to run it. That is a message
  ordered by wall clock, which is the thing this framework exists to remove.
- A **real monitor in the watcher**, under a private tag, would land at exactly
  the right moment in exactly the wrong shape, and no one can rewrite another
  process's mailbox.
- **Polling** would fire on whichever driver call happened next.

`eta_sched` is already the tracer for every process in a run and already calls
`inherit/2` from the same handler, so it learns of an exit synchronously with the
step that caused it, in a process whose mailbox is read at points the schedule
fixes. Delivery from there is exact: only one process runs during a step, so a
DOWN raised at the end of it sits behind every message the dying process sent
during it, which is the order the VM would have produced.

**The boundary this leaves**: a target the scheduler does not own reports nothing,
so a simulated monitor on one never fires. Every process a run schedules is owned
by construction — that is `processes/1`'s contract — so this is a statement about
processes outside the run, and it is another reason simulation is confined to
monitors that cross a declared link.
""".
-endif.
-spec notify_exit(pid(), term()) -> ok.
notify_exit(Pid, Reason) ->
    case running() of
        true ->
            %% Recorded before the fan-out, so a monitor created later on a target
            %% that has already exited answers `noproc` without asking the VM. See
            %% `simulate_monitor/4`.
            true = ets:insert(?DEAD, {Pid}),
            _ = [fire(M, Reason) || M <- monitors_on(Pid)],
            %% Whatever it was watching, it is not watching any more.
            true = ets:match_delete(?MONS, {'_', Pid, '_', '_', '_', '_'}),
            ok;
        false ->
            ok
    end.

%% Every simulated monitor whose target is `Pid`, oldest first. Ordered for the
%% same reason `on_node/1` is: a fan-out whose order came from the table is a
%% fan-out that differs between runs of one seed.
monitors_on(Pid) ->
    by_seq(ets:match_object(?MONS, {'_', '_', Pid, '_', '_', '_'})).

by_seq(Mons) ->
    lists:sort(fun({_, _, _, _, _, S1}, {_, _, _, _, _, S2}) -> S1 =< S2 end, Mons).

%% Delivers a DOWN and retires the monitor, in that order for no reason other
%% than that the row must be gone before anything can ask about it again. Direct
%% rather than routed: a DOWN is a signal, and a cut channel must not be able to
%% swallow the announcement of the cut.
fire({Ref, Watcher, _Target, Object, Tag, _Seq}, Reason) ->
    true = ets:delete(?MONS, Ref),
    _ = (catch erlang:send(Watcher, {Tag, Ref, process, Object, Reason})),
    case Reason of
        noconnection -> bump(noconnection);
        _ -> ok
    end,
    _ = eta_log:log({net, {down, Reason}, Watcher, Tag}),
    ok.

%% ---------------------------------------------------------------------------
%% Targeted faults
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc """
Drops everything sent from `From` to `To` until `heal/2`. One direction.

Messages already in flight on that channel are cancelled, as a failing link loses
what was on it, and counted as both `dropped` and `cancelled`.

**No link events.** This is a message fault, not a node failure: nothing is
signalled and no monitor fires. A one-way loss of messages is not something a
lost connection produces — real distribution tears the whole connection down
when either end's tick times out, and both ends then find out. `partition/3` is
that event; `cut/2` is the narrower fault of a channel that swallows traffic
while both ends still believe the link is up. Use it when that is what you mean,
and `partition/3` when a node has gone.
""".
-endif.
-spec cut(dest(), dest()) -> ok.
cut(From0, To0) ->
    ok = require_running(cut),
    case {resolve(From0), resolve(To0)} of
        {{node, A}, {node, B}} ->
            true = ets:insert(?CHAN, {{node_cut, A, B}}),
            %% Everything already in flight across the link is lost with it.
            _ = [cancel_pending(P, Q) || P <- on_node(A), Q <- on_node(B)],
            ok;
        {{pid, From}, {pid, To}} ->
            true = ets:insert(?CHAN, {{cut, From, To}}),
            _ = cancel_pending(From, To),
            ok;
        Mixed ->
            error(
                {eta_net,
                    {mixed_cut, Mixed, <<
                        "cut both ends as nodes or both as processes, not one of each"
                    >>}}
            )
    end.

-if(?DOCATTRS).
-doc "Restores a channel previously `cut/2`.".
-endif.
-spec heal(dest(), dest()) -> ok.
heal(From0, To0) ->
    ok = require_running(heal),
    case {resolve(From0), resolve(To0)} of
        {{node, A}, {node, B}} -> true = ets:delete(?CHAN, {node_cut, A, B});
        {{pid, From}, {pid, To}} -> true = ets:delete(?CHAN, {cut, From, To});
        Mixed -> error({eta_net, {mixed_cut, Mixed}})
    end,
    ok.

-if(?DOCATTRS).
-doc "Cuts both directions — which is what a lost link actually is. See `partition/3`.".
-endif.
-spec partition(dest(), dest()) -> ok.
partition(A, B) ->
    partition(A, B, #{}).

-if(?DOCATTRS).
-doc """
Cuts both directions, delivers the `noconnection` DOWNs the failure produces, and
optionally a link-down signal.

```erlang
eta_net:partition(na, nb, #{signal => nodedown}).
```

## The signal

**Prefer the signalling form.** A real link failure is never only lost messages:
it delivers `nodedown`/`nodeup` to both ends and systems hang recovery off those.
Dropping without signalling injects something no network produces — messages
vanishing while both ends still believe the link is up — and the unrecovered state
that follows is an artefact rather than a defect.

The opposite trap is real too: recovery driven by the signal may repair the very
divergence under test, so a run asserting a property *during* a fault restricts
loss by `scope` instead. Neither is right in every situation, which is why there
is no default.

Three forms, and the first is the one to reach for:

| `signal` | side A receives | side B receives |
|---|---|---|
| `nodedown` (any atom) | `{nodedown, B}` | `{nodedown, A}` |
| `{literal, Term}` | `Term` | `Term` |
| any other term | itself | itself |

An atom names a *kind* and is derived per side, because that is what a partition
actually says: processes on A learn that B is gone, and processes on B learn that
A is gone. One undifferentiated term tells both sides the same thing, which is
never what happened. `A` and `B` here are the arguments as resolved — a node name
if the argument named a node, the pid otherwise.

The literal forms are what a term that is already complete uses, and
`#{signal => {nodedown, node()}}` keeps meaning exactly what it always did.
`{literal, _}` exists for the one case the bare form cannot express: a signal
that is a single atom.

## Which side learns

`learns` says which side of the partition *observes* the failure: `both` (the
default), `a`, or `b`. It governs the signal and the DOWNs together, since they
are the same event seen twice.

```erlang
eta_net:partition(na, nb, #{signal => nodedown, learns => a}).
```

is "A finds out that B is gone, and B does not notice" — an asymmetry real
distribution produces constantly, since the two ends time out independently, and
one that had to be hand-rolled from two `cut/2` calls and a fan-out before. The
cut itself stays symmetric: a lost link loses both directions whether or not
anyone has realised.

## Monitors

Every simulated monitor held across the partition fires exactly one
`{'DOWN', Ref, process, Object, noconnection}` and is retired. See `monitor/2`
for which monitors those are and why the DOWN is terminal.

## Delivery

DOWNs first, then signals; side A's fan-out before side B's; each in placement
order. Everything is delivered directly rather than routed, so a cut channel
cannot swallow it, and everything is enqueued before this call returns — no
process the scheduler owns runs in between, so no one can observe half of it.
""".
-endif.
-spec partition(dest(), dest(), event_opts()) -> ok.
partition(A, B, Opts) ->
    ok = cut(A, B),
    ok = cut(B, A),
    link_event(A, B, Opts).

-if(?DOCATTRS).
-doc "Heals both directions. See `heal_partition/3`.".
-endif.
-spec heal_partition(dest(), dest()) -> ok.
heal_partition(A, B) ->
    heal_partition(A, B, #{}).

-if(?DOCATTRS).
-doc """
Heals both directions, optionally delivering a link-up signal — `#{signal =>
nodeup}`, or any of the forms `partition/3` takes. See `partition/3` for why that
matters, and `learns` for delivering it to one side only.

**Resurrects nothing.** A monitor that fired `noconnection` is gone, exactly as
it would be in real Erlang, and a system that wants to keep watching its peer has
to monitor it again. Healing a link is not undoing the failure; it is a second
event.
""".
-endif.
-spec heal_partition(dest(), dest(), event_opts()) -> ok.
heal_partition(A, B, Opts) ->
    ok = heal(A, B),
    ok = heal(B, A),
    signals(A, B, Opts).

%% A link between A and B has failed: retire the monitors that crossed it, then
%% announce it. Shared by `partition/3` and, in a one-sided form, `kill_node/2`.
link_event(A, B, Opts) ->
    ok = sever(endpoints(A), endpoints(B), learns(Opts)),
    signals(A, B, Opts).

%% Fires `noconnection` at every simulated monitor with one end on each side.
%%
%% `learns` selects by *watcher*, since it says who noticed: with `learns => a`,
%% only monitors held from side A fire, and side B carries on believing its peer
%% is reachable.
sever(SideA, SideB, Learns) ->
    A = sets:from_list(SideA),
    B = sets:from_list(SideB),
    Straddling = [
        M
     || M = {_Ref, W, T, _O, _Tag, _Seq} <- ets:tab2list(?MONS),
        straddles(W, T, A, B, Learns)
    ],
    _ = [fire(M, noconnection) || M <- by_seq(Straddling)],
    ok.

straddles(W, T, A, B, Learns) ->
    (Learns =/= b andalso sets:is_element(W, A) andalso sets:is_element(T, B)) orelse
        (Learns =/= a andalso sets:is_element(W, B) andalso sets:is_element(T, A)).

learns(Opts) ->
    case maps:get(learns, Opts, both) of
        L when L =:= both; L =:= a; L =:= b ->
            L;
        Other ->
            error(
                {eta_net,
                    {bad_learns, Other, <<"expected both, a or b — see eta_net:partition/3">>}}
            )
    end.

%% Delivers the signal, derived per side. Side A hears about B and side B hears
%% about A; `learns` can silence either.
signals(A, B, Opts) ->
    case maps:find(signal, Opts) of
        error ->
            ok;
        {ok, Sig} ->
            Learns = learns(Opts),
            ok = emit(sides(Learns, a, endpoints(A)), derive(Sig, name_of(B))),
            ok = emit(sides(Learns, b, endpoints(B)), derive(Sig, name_of(A)))
    end.

sides(both, _Side, Pids) -> Pids;
sides(Side, Side, Pids) -> Pids;
sides(_Other, _Side, _Pids) -> [].

%% An atom is a kind and is derived; anything else is already the message. Kept
%% in that order so `#{signal => {nodedown, node()}}` — the form that has callers
%% — is untouched, and `{literal, Atom}` is the escape for a bare atom.
derive({literal, Term}, _Other) -> Term;
derive(Kind, Other) when is_atom(Kind) -> {Kind, Other};
derive(Term, _Other) -> Term.

%% What the other side is *called*, for a derived signal: the node name if the
%% argument named one, and the pid otherwise. A pid-level partition has no node
%% names to offer and saying so is better than inventing one.
name_of(Ref) ->
    case resolve(Ref) of
        {node, Node} -> Node;
        {pid, Pid} -> Pid
    end.

emit(Pids, Sig) ->
    lists:foreach(
        fun(P) ->
            case is_pid(P) of
                true ->
                    _ = (catch erlang:send(P, Sig)),
                    bump(signalled),
                    _ = eta_log:log({net, signal, P, tag(Sig)});
                false ->
                    ok
            end
        end,
        Pids
    ),
    ok.

%% Who a link signal reaches: everything on the node, not a representative — a
%% process that never saw the event is one the harness excluded from recovering.
%% In placement order; see `on_node/1` for why that matters.
endpoints(Ref) ->
    case resolve(Ref) of
        {node, Node} -> on_node(Node);
        {pid, Pid} -> [Pid]
    end.

-if(?DOCATTRS).
-doc "Kills a simulated node with no signal. See `kill_node/2`.".
-endif.
-spec kill_node(term()) -> ok.
kill_node(Node) ->
    kill_node(Node, #{}).

-if(?DOCATTRS).
-doc """
"Node N is gone": every process located on it dies, and every survivor gets the
events that death produces.

```erlang
eta_net:kill_node(nb, #{signal => nodedown}).
```

Five things happen, in this order, and the order is the point — a harness that
hand-rolls this from `place/2`, `exit/2` and `partition/3` gets it subtly wrong,
usually by killing first:

1. Every simulated monitor held **from another node** on a process that is about
   to die fires `{'DOWN', Ref, process, Object, noconnection}`.
2. Every monitor with either end on the node is retired.
3. Messages in flight to or from the node are cancelled, as a failing node loses
   what was on the wire.
4. Every located process on the node — placed *and* attached — is killed.
5. Surviving located processes receive `{Signal, Node}`.

## The asymmetry in step 1

Real distribution reports a lost node as `noconnection` to a remote monitor while
a monitor on the same machine sees the true exit reason. One VM cannot do that on
its own: a killed process yields `killed` to every monitor it has. So the remote
monitors are retired *before* the process dies, and the DOWN they receive is the
one distribution would have sent. Monitors held by anything not located — a
client, a harness, an observer — are untouched real monitors and see `killed`,
which is the same asymmetry seen from the other side.

Step 1 must precede step 4 for a second reason: once the exit signal is out, the
scheduler may report it at any moment, and a monitor still on the books then
would fire with the real reason.

## Atomicity

The whole sequence runs in the driver, between steps, while every process the
scheduler owns is suspended. No survivor can observe a half-dead node, because no
survivor runs until it is a fully dead one.

## Restarting

A killed node's *name* survives; its processes do not. `place/2` or `attach/2` on
the same name afterwards is how a restart is expressed, and the new processes are
ordinary members of that node — they get a fresh position in the link-event
fan-out, since the ones they replace are gone.

Two things deliberately do not reset. Any cut involving the node is still in
force, so a node that was partitioned and then died comes back partitioned until
it is healed; and nothing re-monitors on a survivor's behalf. Both follow from
the same rule as everywhere else here: an event says what just happened, it does
not undo what happened before.
""".
-endif.
-spec kill_node(term(), event_opts()) -> ok.
kill_node(Node, Opts) ->
    ok = require_running(kill_node),
    ets:member(?PLACE, {node, Node}) orelse
        error(
            {eta_net,
                {no_such_node, Node, <<
                    "nothing has been placed on this node, so killing it would be a "
                    "no-op that looks like a fault. Name a node from place/2 or attach/2"
                >>}}
        ),
    %% `learns` is a `partition/3` option and means nothing here — a node kill
    %% has one surviving side, and the side that died is not going to notice
    %% anything. Refused rather than ignored, since an option that silently does
    %% nothing reads at the call site as though it did.
    not is_map_key(learns, Opts) orelse
        error(
            {eta_net,
                {learns_on_kill, Node, <<
                    "kill_node/2 has only one side that can learn anything; "
                    "`learns` is a partition/3 option"
                >>}}
        ),
    Dying = on_node(Node),
    Survivors = located() -- Dying,
    ok = sever_from(Dying),
    ok = forget_monitors(Dying),
    ok = cancel_touching(Dying),
    ok = kill_all(Dying),
    case maps:find(signal, Opts) of
        error -> ok;
        {ok, Sig} -> emit(Survivors, derive(Sig, Node))
    end.

%% Step 1: the monitors a remote watcher holds on something about to die. A
%% watcher that is dying too is not told anything, and one that is not located is
%% not on the network at all — its monitor is a real one and reports the truth.
sever_from(Dying) ->
    D = sets:from_list(Dying),
    Straddling = [
        M
     || M = {_Ref, W, T, _O, _Tag, _Seq} <- ets:tab2list(?MONS),
        sets:is_element(T, D),
        not sets:is_element(W, D)
    ],
    _ = [fire(M, noconnection) || M <- by_seq(Straddling)],
    ok.

forget_monitors(Dying) ->
    lists:foreach(
        fun(P) ->
            true = ets:match_delete(?MONS, {'_', P, '_', '_', '_', '_'}),
            true = ets:match_delete(?MONS, {'_', '_', P, '_', '_', '_'})
        end,
        Dying
    ),
    ok.

%% Kills every process on the node and **waits for each to be gone** before
%% returning.
%%
%% The wait is what makes the kill atomic with respect to the schedule rather
%% than merely quick. `erlang:exit/2` posts a signal; the process dies some
%% moment later, and the moment is chosen by the BEAM. `eta_sched:runnable/1`
%% asks `is_process_alive/1`, so a driver that returned before the answer settled
%% would leave the scheduler's next choice depending on how fast a corpse was
%% reaped — a step chosen by wall clock, and a seed that stops reproducing its own
%% schedule. Two runs of one seed then differ by one step, which is the entire
%% failure mode this framework exists to prevent.
%%
%% `kill` is untrappable and reaches a suspended process, so this terminates. The
%% bound is there to fail loudly rather than hang a suite if it ever does not.
kill_all(Dying) ->
    Mons = [{P, erlang:monitor(process, P)} || P <- Dying],
    lists:foreach(
        fun(P) ->
            true = ets:delete(?PLACE, {pid, P}),
            %% As `notify_exit/2` would, but the scheduler has not seen these
            %% deaths yet and a survivor may monitor one before it does.
            true = ets:insert(?DEAD, {P}),
            true = erlang:exit(P, kill)
        end,
        Dying
    ),
    lists:foreach(fun await_death/1, Mons),
    ok.

await_death({Pid, Mon}) ->
    receive
        {'DOWN', Mon, process, _, _} -> ok
    after 5000 ->
        true = erlang:demonitor(Mon, [flush]),
        error(
            {eta_net,
                {kill_timeout, Pid, <<
                    "a process on a killed node did not die within 5s; the node "
                    "cannot be reported gone while one of its processes is still there"
                >>}}
        )
    end.

%% Everything on the wire in either direction is lost with the node.
cancel_touching(Dying) ->
    D = sets:from_list(Dying),
    _ = [
        cancel_pending(From, To)
     || {{chan, From, To}, _Last, _Pending} <- ets:match_object(?CHAN, {{chan, '_', '_'}, '_', '_'}),
        sets:is_element(From, D) orelse sets:is_element(To, D)
    ],
    ok.

-if(?DOCATTRS).
-doc """
Drops exactly the next `K` messages from `From` to `To`, then resumes. Asks what a
probability cannot: "lose the reply to this one operation".
""".
-endif.
-spec drop_next(dest(), dest(), non_neg_integer()) -> ok.
drop_next(From0, To0, K) ->
    ok = require_running(drop_next),
    true = ets:insert(?CHAN, {{drop_next, normalize(From0), normalize(To0)}, K}),
    ok.

-if(?DOCATTRS).
-doc """
On the `From -> To` channel, lets the next `Skip` messages tagged `Tag` through,
then drops the following `K` of them. Other traffic is untouched.

Counts *within one kind of message*, which a raw message count cannot: a batch
interleaves on the wire with acks and replies, so "drop messages 2..4 of this
batch" is only expressible by tag. Losing a strict subset of one batch is a
materially different fault from losing a whole channel — it can leave no
discontinuity for the receiver to detect.

See `set_policy/1` for what a tag is.
""".
-endif.
-spec drop_matching(dest(), dest(), term(), non_neg_integer(), non_neg_integer()) -> ok.
drop_matching(From0, To0, Tag, Skip, K) ->
    ok = require_running(drop_matching),
    Key = {drop_matching, normalize(From0), normalize(To0), Tag},
    true = ets:insert(?CHAN, {Key, Skip, K}),
    ok.

-if(?DOCATTRS).
-doc """
Removes every cut and every pending `drop_next/3` or `drop_matching/5`.

Does not touch the random policy — `heal_all/0` plus
`set_policy(#{drop_p => 0.0, delay_p => 0.0})` is the perfect network to converge
into.

Does not touch monitors or topology either. A monitor retired by a `noconnection`
DOWN stays retired, exactly as `heal_partition/3` leaves it, and a node killed by
`kill_node/2` stays dead. This restores a *network*; it does not rewind a run.
""".
-endif.
-spec heal_all() -> ok.
heal_all() ->
    ok = require_running(heal_all),
    true = ets:match_delete(?CHAN, {{cut, '_', '_'}}),
    true = ets:match_delete(?CHAN, {{node_cut, '_', '_'}}),
    true = ets:match_delete(?CHAN, {{drop_next, '_', '_'}, '_'}),
    true = ets:match_delete(?CHAN, {{drop_matching, '_', '_', '_'}, '_', '_'}),
    ok.

%% ---------------------------------------------------------------------------
%% Observation
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc """
Message counts by disposition.

`dropped` is what a non-vacuity guard asserts on: a run whose network never lost
anything tested the same system a perfect one would, and reports the same `ok`.

`unmanaged` counts sends made by a process other than the one the scheduler was
stepping, *into* a process the scheduler owns — see `eta_sched:stepping/0`.
Non-zero means part of the run was ordered by wall clock, and nothing else
reports it. Traffic between two processes the run does not own is delivered
untouched and not counted.

`signalled` and `noconnection` count link *events* rather than traffic: node
signals delivered, and synthetic `noconnection` DOWNs fired. They are the
non-vacuity guard for a partition, the way `dropped` is for a lossy policy — a
run that partitioned a node nothing was on, or severed no monitor, exercised no
recovery and reports the same `ok`.
""".
-endif.
-spec stats() -> #{atom() => non_neg_integer()}.
stats() ->
    ok = require_running(stats),
    maps:from_list(
        [{C, ets:lookup_element(?STATE, C, 2)} || C <- ?COUNTERS] ++
            [{in_flight, in_flight()}]
    ).

-if(?DOCATTRS).
-doc """
How many messages are delayed but not yet delivered.

The network's analogue of a stray timer: non-zero at the end of a run means a
message was decided on and never arrived, usually because the run ended or its
destination died first.
""".
-endif.
-spec in_flight() -> non_neg_integer().
in_flight() ->
    ok = require_running(in_flight),
    Now = now_ms(),
    lists:sum([
        length([A || {A, _Ref} <- Pending, A > Now])
     || {{chan, _, _}, _Last, Pending} <- ets:match_object(?CHAN, {{chan, '_', '_'}, '_', '_'})
    ]).

%% ---------------------------------------------------------------------------
%% Routing
%% ---------------------------------------------------------------------------

route(From, Dest, Msg) ->
    route(From, Dest, Msg, tag(Msg)).

%% What this send is, as far as the schedule is concerned.
%%
%% - `scheduled` — routed, faulted, and allowed to draw from the fault schedule.
%% - `escaped` — the schedule did not order it, but it targets a process the
%%   scheduler owns. Delivered untouched, draws nothing, and **counted**: it is
%%   enough on its own to make a trace diverge, and nothing else reports it.
%% - `foreign` — neither end belongs to the run. Delivered untouched, draws
%%   nothing, not counted.
%%
%% During a step one process runs and every other one the scheduler owns is
%% suspended, so a send from anything else happened at a moment wall-clock timing
%% chose. That is exact, where asking "is the sender registered with the
%% scheduler" would be both weaker (a process can be owned and still running
%% loose) and stale.
%%
%% `foreign` exists because this module is global to the VM, so every transformed
%% module in it sends through here — including a leftover system from an earlier
%% test file. Such a send cannot change what is runnable, so counting it would
%% fire a diagnostic the reader cannot act on. It still draws nothing: a
%% stranger's message must not shift the fault schedule.
%%
%% No step in progress is not off-schedule: the driver runs between steps, and
%% outside a run there is no scheduler at all.
classify(From, Dest) ->
    case eta_sched:stepping() of
        undefined -> scheduled;
        From -> scheduled;
        Stepping -> escaped_or_foreign(Dest, Stepping)
    end.

%% "Does the scheduler own the destination", without asking it: mid-step every
%% process it owns except the stepped one is suspended.
escaped_or_foreign(Dest, Stepping) when Dest =:= Stepping ->
    escaped;
escaped_or_foreign(Dest, _Stepping) ->
    case erlang:process_info(Dest, status) of
        {status, suspended} -> escaped;
        _ -> foreign
    end.

%% `Tag` is what a policy scopes on and what the log records. Passed rather than
%% derived so `cast/2` and `reply/2` can name the payload, not OTP's envelope.
route(From, Dest, Msg, Tag) ->
    case classify(From, Dest) of
        scheduled ->
            route_managed(From, Dest, Msg, Tag);
        escaped ->
            bump(unmanaged),
            erlang:send(Dest, Msg);
        foreign ->
            erlang:send(Dest, Msg)
    end.

route_managed(From, Dest, Msg, Tag) ->
    case decide(From, Dest, Tag) of
        drop ->
            bump(dropped),
            _ = eta_log:log({net, dropped, Dest, Tag}),
            Msg;
        {send, Delay} ->
            schedule(From, Dest, Msg, Tag, Delay)
    end.

%% The order matters: a cut is absolute, a targeted drop is next, and only then
%% does the random policy get a say — and only for traffic in its scope.
decide(From, Dest, Tag) ->
    case is_cut(From, Dest) of
        true ->
            drop;
        false ->
            case take_drop_next(From, Dest) orelse take_drop_matching(From, Dest, Tag) of
                true -> drop;
                false -> roll(From, Dest, Tag)
            end
    end.

is_cut(From, Dest) ->
    ets:member(?CHAN, {cut, From, Dest}) orelse node_cut(From, Dest).

%% A node-level cut survives a process restart, which a pid-level one cannot: the
%% replacement is on the same node, so it is still on the wrong side of the
%% partition. That is the behaviour a partition actually has.
node_cut(From, Dest) ->
    case {node_of(From), node_of(Dest)} of
        {undefined, _} -> false;
        {_, undefined} -> false;
        {A, B} -> ets:member(?CHAN, {node_cut, A, B})
    end.

take_drop_matching(From, Dest, Tag) ->
    Key = {drop_matching, From, Dest, Tag},
    case ets:lookup(?CHAN, Key) of
        [{Key, Skip, K}] when Skip > 0 ->
            true = ets:insert(?CHAN, {Key, Skip - 1, K}),
            false;
        [{Key, 0, K}] when K > 0 ->
            true = ets:insert(?CHAN, {Key, 0, K - 1}),
            true;
        _ ->
            false
    end.

take_drop_next(From, Dest) ->
    Key = {drop_next, From, Dest},
    case ets:lookup(?CHAN, Key) of
        [{Key, K}] when K > 0 ->
            true = ets:insert(?CHAN, {Key, K - 1}),
            true;
        _ ->
            false
    end.

%% Draws from the RNG exactly once for an in-scope message, and a second time only
%% when the delay branch is taken.
%%
%% Two things draw nothing at all, and both are load-bearing rather than
%% optimisations.
%%
%% **Out-of-scope traffic**, so narrowing the scope does not shift the schedule
%% for the traffic still inside it.
%%
%% **A perfect policy.** With both probabilities at zero there is no decision to
%% make, and making it anyway costs a draw — which is how the fault schedule stops
%% being a function of the seed. A cluster that must sync before it can survive
%% faults starts perfect and turns loss on at the end of `init/2`, and everything
%% it sends while starting up runs on the *real* scheduler, varying in order and
%% count from run to run. Charging those messages for a decision that could only
%% ever come out one way left the generator at a different offset on every run:
%% same seed, different drops, and a trace that diverged the moment the first
%% fault landed. Measured on a 3-member registry, this was 5 distinct schedules in
%% 5 runs of one seed.
%%
%% The rule this leaves is worth stating whole: **the generator advances only when
%% the answer could have been otherwise.**
roll(From, Dest, Tag) ->
    Policy = policy(),
    DropP = maps:get(drop_p, Policy),
    DelayP = maps:get(delay_p, Policy),
    case faultable(From, Dest, Tag) of
        false ->
            {send, 0};
        true ->
            case draw() of
                P when P < DropP -> drop;
                P when P < DropP + DelayP ->
                    {send, max(1, trunc(draw() * maps:get(max_delay, Policy)))};
                _ ->
                    {send, 0}
            end
    end.

%% Whether this message is one the policy in force could actually do something to.
%% Three conditions, each load-bearing somewhere else in this module: the policy
%% has to be able to fault at all, the send has to cross a simulated link, and it
%% has to be in scope.
%%
%% Shared with `call/3`, which is why it is a function rather than a guard inside
%% `roll/3`: "would the network interfere with this?" is exactly the question that
%% decides whether a synchronous call is safe to let through.
faultable(From, Dest, Tag) ->
    Policy = policy(),
    maps:get(drop_p, Policy) + maps:get(delay_p, Policy) > 0.0 andalso
        crosses_link(From, Dest) andalso
        in_scope(maps:get(scope, Policy), From, Dest, Tag).

in_scope(all, _From, _Dest, _Tag) -> true;
in_scope({tags, Tags}, _From, _Dest, Tag) -> lists:member(Tag, Tags);
in_scope(Fun, From, Dest, Tag) when is_function(Fun, 3) -> Fun(From, Dest, Tag).

%% Read-modify-write on the RNG, which is safe because only one process runs at a
%% time under `eta_sched` — and outside a run, only whoever is driving by hand.
draw() ->
    {P, Next} = rand:uniform_s(ets:lookup_element(?STATE, rand, 2)),
    true = ets:insert(?STATE, {rand, Next}),
    P.

%% Where the ordering guarantee lives. A delivery lands at
%% `max(Now + Delay, Last + 1)`, so it can never overtake one already scheduled on
%% the same channel — and a message that needs no delay, on a channel with nothing
%% outstanding, is sent directly, so a perfect network behaves exactly as no
%% network does.
schedule(From, Dest, Msg, Tag, Delay) ->
    Now = now_ms(),
    {Last, Pending} = channel(From, Dest, Now),
    case max(Now + Delay, Last + 1) of
        At when At =< Now ->
            deliver(Dest, Msg);
        At ->
            Ref = eta_time:send_after(At - Now, Dest, Msg),
            true = ets:insert(?CHAN, {{chan, From, Dest}, At, [{At, Ref} | Pending]}),
            bump(delayed),
            _ = eta_log:log({net, {delayed, At - Now}, Dest, Tag}),
            Msg
    end.

%% Milliseconds on whichever clock is running.
%%
%% `eta_time` already delegates `send_after/3` and `cancel_timer/1` to `erlang`
%% when no virtual clock exists, so a delay outside a run is a real one — which is
%% what a network simulator driven by hand should do, and what lets a harness that
%% predates `eta_run` use this module unchanged. Inside a run there is always a
%% virtual clock and the delay costs nothing.
%%
%% The one thing that is exact only on the virtual clock is `channel/3`'s
%% deduction that a delivery at or before `Now` has already happened: a real timer
%% fires *around* its deadline rather than at it. The consequence is a
%% millisecond of slack in the ordering clamp, which no protocol can observe,
%% rather than a reordering.
now_ms() ->
    case eta_time:running() of
        true -> eta_time:now_ms();
        false -> erlang:monotonic_time(millisecond)
    end.

deliver(Dest, Msg) ->
    bump(delivered),
    erlang:send(Dest, Msg).

%% A channel's last scheduled delivery and whatever is still outstanding.
%%
%% "Outstanding" needs no bookkeeping beyond the clock: deliveries are timers, the
%% clock only moves by firing everything due, and an armed delivery is always at a
%% strictly future time — so anything at or before `Now` has already been
%% delivered. That is why the clamp is `Last + 1` rather than `Last`.
channel(From, Dest, Now) ->
    case ets:lookup(?CHAN, {chan, From, Dest}) of
        [{_, Last, Pending}] -> {Last, [P || P = {At, _} <- Pending, At > Now]};
        [] -> {Now - 1, []}
    end.

cancel_pending(From, Dest) ->
    case ets:lookup(?CHAN, {chan, From, Dest}) of
        [{_, _Last, Pending}] ->
            Now = now_ms(),
            Cancelled = [R || {At, R} <- Pending, At > Now, eta_time:cancel_timer(R) =/= false],
            true = ets:delete(?CHAN, {chan, From, Dest}),
            lists:foreach(
                fun(_) ->
                    bump(dropped),
                    bump(cancelled)
                end,
                Cancelled
            ),
            length(Cancelled);
        _ ->
            0
    end.

bump(Counter) ->
    _ = ets:update_counter(?STATE, Counter, 1),
    ok.

%% The message's leading element, which is what a policy scopes on and what the
%% log records. Enough to read a protocol trace without holding every payload.
tag(Msg) when is_tuple(Msg), tuple_size(Msg) > 0 -> element(1, Msg);
tag(Msg) when is_atom(Msg) -> Msg;
tag(_) -> unknown.
