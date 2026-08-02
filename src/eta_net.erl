-module(eta_net).

-define(DOCATTRS, ?OTP_RELEASE >= 27).

-if(?DOCATTRS).
-moduledoc """
A simulated network: seeded message loss, delay and partitions between processes
in one VM.

`eta_sched` decides *who runs* and `eta_time` decides *when*. Neither decides
whether a message arrives — so a client that sends to three peers reaches all
three inside one scheduler step, and a partially delivered broadcast is not a
state the schedule can produce. This module is the missing third: it owns
delivery, and can drop, delay or cut it.

## The seam is a function

Everything here hangs off `send/2`, which is an ordinary exported function that
**delegates straight to `erlang:send/2` unless a network is running**. A system
reaches it three ways, and they are the same seam:

```erlang
Peer ! {replicate, Batch}.                     %% eta_transform rewrites this
eta_net:send(Peer, {replicate, Batch}).        %% or write it yourself
myapp_link:send(Peer, {replicate, Batch}).     %% or from your own transport module
```

Use the transform unless your system already has a transport module, in which
case call this from it and skip the header entirely.

## The fault model is deliberately narrow

Only faults **real Erlang can produce**. Distribution guarantees that messages
delivered between one ordered pair of processes arrive in send order, and does
not guarantee delivery at all. So, per ordered `{From, To}` pair, this module may

- **deliver** the message (the default),
- **drop** it — a signal lost to a failing link,
- **delay** it — arrived later, still in order,
- **cut** the channel, dropping everything until healed.

It never reorders two messages on the same ordered pair, because distribution
never does. Injecting that would manufacture counterexamples the real system
cannot produce, and every "bug" found would be a false positive. Messages on
*different* pairs are already unordered with respect to each other.

**Loss comes with a signal.** A real link failure is never just lost messages —
it delivers `nodedown`/`nodeup` to both ends, and systems hang their recovery off
those. `partition/3` and `heal_partition/3` take a `signal` for exactly this;
dropping messages without it injects something no network produces, and the
unrecovered state you then report is not a defect. See `partition/3`.

## Delay is virtual, and there is no network process

The decision is made **at send time**, inside the
sending process's own step, and a delayed message is handed to `eta_time` — so
the delivery is an ordinary deadline in the timer wheel the driver already
drains, at a virtual time, costing nothing to wait for. `eta_time`'s
schedulability filter looks at a timer's *destination*, so a delivery belongs to
its receiver: a sender that exits mid-flight does not turn its message into a
stray timer.

Two consequences worth stating:

- **In-flight messages are pending deadlines**, so "nothing runnable" is not
  quiescence while one is outstanding, and `eta_run` needs no changes to know
  that — `idle/1` advances to the delivery and the existing `{clock, Ms}` trace
  entry records it. Replay works unmodified.

- **The fault schedule cannot drift.** The RNG is consumed once per in-scope
  send, and every send happens inside a scheduler step whose order is already a
  function of the seed.

## Message Ordering

Each channel records the virtual time of its last scheduled delivery, and a new
one lands at `max(Now + Delay, Last + 1)`. Per-pair order is preserved by
construction across delivered and delayed messages alike, with nothing to
release and nothing to drain.

This is also why a message is delivered *directly* when it draws no delay and the
channel has nothing outstanding: a perfect network is exactly the behaviour of no
network at all, which keeps `drop_p => 0.0` usable as a control run.

## Where the network is

By default every link is faultable, which is fine for a two-process test and
wrong for anything with structure: a system's real fault model is not "any
message may be lost" but "any message *that crosses a link* may be lost".
`place/2` says which processes are on which simulated node, and faults then apply
only to sends whose ends are on different ones.

Prefer it to `scope` for that job. `scope` narrows by message *kind*, which is
about what a run is entitled to assert (see `set_policy/1`); topology narrows by
*where the wire is*, which is a fact about the system. Describing the second with
a predicate is how a harness ends up faulting a reply to a client, or a
component that coordinates through a database — both of which produce
plausible-looking violations of real safety properties.

## Routing has to be uniform per channel

If some sends on `{A, B}` come through here and others go direct, a later direct
message can overtake an earlier delayed one — reordering within an ordered pair,
reintroduced by the harness, and attributed to the system under test.

A transform is *module* granular, so a peer-facing module is either in or out. A
hand-placed seam is *call-site* granular, which is where a half-routed channel
comes from: one `Pid ! ack` left unwrapped in an error branch is enough. A system
that writes its own calls owns this rule.
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
    reply/2,
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
    node_of/1,
    inherit/2
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
    drop_next/3,
    drop_matching/5
]).

%% Observation.
-export([
    stats/0,
    in_flight/0
]).

-export_type([policy/0, scope/0]).

-define(STATE, eta_net_state).
-define(CHAN, eta_net_chan).
-define(PLACE, eta_net_place).
-define(CALLS, eta_net_calls).

-type dest() :: pid() | atom().

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

-define(COUNTERS, [delivered, dropped, delayed, cancelled, unmanaged]).

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
  control run wants and what a cluster needs while it is still starting up.

One network per VM, the same rule (and for the same reason) as `eta_time` and
`eta_log`: the state is in named tables, so runs must be serial. Starting over a
running network resets it; starting over one another live process owns raises.
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
    Seed = maps:get(seed, Opts, 0),
    ets:insert(?STATE, [
        {rand, rand:seed_s(exsss, {Seed, Seed + 11, Seed + 23})},
        {policy, merge_policy(maps:get(policy, Opts, #{}))}
        | [{C, 0} || C <- ?COUNTERS]
    ]),
    ok.

%% See `eta_time`'s equivalent: the existence of a named table is the lock,
%% because the VM destroys it when its owner exits. `stop/0` is deliberately not
%% guarded, so an `on_exit` running in another process still cleans up.
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

Messages already in flight stay in `eta_time`'s wheel and are delivered by
whatever advances the clock next — they are ordinary timers by that point.
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
        [?STATE, ?CHAN, ?PLACE, ?CALLS]
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

Returns `Msg`, so this is a drop-in for both `Dest ! Msg` and `erlang:send/2` —
which is what lets `eta_transform` rewrite the operator without changing the
value of the expression.

A destination that is neither a pid nor a registered name — `{Name, Node}`,
`{global, _}`, `{via, _, _}` — is passed through untouched. `eta` does not
simulate distribution, and a name resolved by another registry is not a channel
this module can identify.
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
`erlang:send/3`. The options are for distribution (`nosuspend`, `noconnect`) and
have no meaning between two processes in one VM, so under a running network they
are ignored and the send is routed; otherwise it is passed through.
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
`gen_server:cast/2`, routed. Casts are the natural faultable channel — they are
asynchronous already, so a dropped one is a fault the sender's own code has to
tolerate rather than a hang.

Like `gen_server:cast/2` this never raises, including for a name nobody has
registered.
""".
-endif.
-spec cast(dest() | term(), term()) -> ok.
cast(Dest, Msg) ->
    case running() andalso normalize(Dest) of
        To when is_pid(To) ->
            %% Tagged by the payload, not by the envelope. `{'$gen_cast', _}` is
            %% OTP's wrapper and every cast in the system carries it, so scoping a
            %% policy on it would select all of them or none — which makes the
            %% `{tags, _}` form useless for exactly the traffic it is most wanted
            %% for. What a run means by "the replication stream" is the payload.
            catch route(self(), To, {'$gen_cast', Msg}, tag(Msg)),
            ok;
        _ ->
            gen_server:cast(Dest, Msg)
    end.

-if(?DOCATTRS).
-doc """
`gen_server:call/2,3`, with **both legs on the network**.

A call is the one exchange OTP does not let a transform reach all of. The request
is an ordinary send, but the reply is produced by `gen:reply` from inside
`gen_server`, and all three of its clauses end in a raw send from the server
process — so no `From` this module constructs can make it routable.

Routing only the request would be worse than routing nothing. A request that goes
through here is subject to the ordering clamp; a reply that goes around is not, so
it can be delivered ahead of a message sent before it. That is reordering within
an ordered pair — the one fault distribution never produces, and the one thing
this module must never manufacture.

So the reply is brought onto the network from the other end. `eta_transform`
rewrites a `gen_server` module's `handle_call/3` returns: `{reply, R, S}` becomes
`eta_net:reply(From, R)` followed by `{noreply, S}`. Both legs are then ordinary
routed messages, both can be dropped or delayed independently, and the asymmetric
fault that actually bites — the work happened, the caller never learned it did —
becomes reachable.

## What this implementation has to reproduce

`gen_server:call/3` is not just a send and a receive. It monitors the callee, so
a dead server is an exit rather than a hang; it takes a timeout; and it cleans up
after itself. All of that is here, with two differences that matter:

- **The timeout is virtual.** It is armed through `eta_time:arm_after/2`, the
  same mechanism `eta_transform` gives a rewritten `receive ... after`. A call
  that waits out a five-second timeout on a dropped request costs no real time,
  and — more importantly — fires at a point the schedule chose.

- **The monitor is not routed, and cannot be.** Exits and `DOWN`s are signals
  rather than messages, so a partition does not suppress them. In one VM the
  peer really is alive, so this is mostly right; a system whose failure detection
  is monitor-based will not see the failure the network is injecting. Model peer
  failure as a crash and network failure as a cut, and know they are different.

## Calling something that was not transformed

Then its reply is still unrouted, and no static check can see that from here. It
is caught after the fact instead: a call registers its tag, `reply/2` clears it,
and a reply that arrives with the tag still outstanding came around the network.
That raises, naming the callee, because a channel the run believes it is faulting
and is not is exactly the silent gap this module exists to close.
""".
-endif.
-spec call(dest() | term(), term()) -> term().
call(Dest, Req) ->
    call(Dest, Req, 5000).

-spec call(dest() | term(), term(), timeout()) -> term().
call(Dest, Req, Timeout) ->
    case running() andalso normalize(Dest) of
        To when is_pid(To) -> do_call(To, Dest, Req, Timeout);
        _ -> gen_server:call(Dest, Req, Timeout)
    end.

do_call(To, Dest, Req, Timeout) ->
    Tag = make_ref(),
    Mon = erlang:monitor(process, To),
    %% Registered before the request goes out, so that a reply cannot beat the
    %% registration and be mistaken for an unrouted one.
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

%% Did the reply come through the network, or around it?
%%
%% `reply/2` takes the tag out on its way past, so a tag still here when the
%% reply lands means the callee answered by a path this module never saw — it was
%% not built with the transform. The call worked; what did not work is the fault
%% model, which is quietly missing one direction of this channel.
check_routed(Tag, To, Req) ->
    case ets:take(?CALLS, Tag) of
        [] ->
            ok;
        [_] ->
            error(
                {eta_net,
                    {unrouted_reply, To, tag(Req), <<
                        "this call was answered by a process eta_transform never "
                        "reached, so the reply came around the network: it cannot be "
                        "dropped or delayed, and it is not subject to the ordering "
                        "clamp the request went through. Build the callee with the "
                        "transform, or keep the call off the simulated network by "
                        "placing both ends on one node"
                    >>}}
            )
    end.

-if(?DOCATTRS).
-doc """
`gen_server:reply/2`, routed — so a *reply* can be lost independently of the
request that caused it, which is the asymmetric fault that actually bites: the
work happened, the caller never learned it did, and it retries.

The reply is addressed to the caller's pid, `element(1, From)`, rather than to
the alias in the tag. Both land in the same mailbox and the caller's selective
receive matches on the tag either way, so this is the same delivery by a route
this module can identify. Note that OTP deactivates the alias once a reply
arrives through it: a reply sent here is *more* likely to be received than one
sent through a stale alias, never less.

The **request** leg of a `gen_server:call/3` is not routed — `gen:call` sends
from inside OTP, where no transform reaches. See the plan's "What cannot be
intercepted".
""".
-endif.
-spec reply(term(), term()) -> ok.
reply(From = {To, Tag}, Reply) when is_pid(To) ->
    case running() of
        true ->
            %% A reply's leading element is the caller's tag — a reference, unique
            %% per call — so it names nothing a policy or a log could use. Every
            %% reply is tagged `'$gen_reply'` instead, which is a channel a run can
            %% actually talk about: "lose the answer, keep the work".
            %% Marks the call as answered through the network. See `call/3`.
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
What `eta_transform` points the messaging functions this module does not
implement at. Raises while a network is running; calls the original otherwise.

The list lives in `eta_transform` (`?NET_UNSUPPORTED`) and covers the parts of
`gen_server`, `gen_statem` and `gen_event` whose traffic cannot yet be put on the
network: broadcasts and multi-node calls, the asynchronous request/response
interface, and everything belonging to a behaviour whose replies come from inside
OTP.

**Raising is the feature.** The alternative is letting the call through, which
means a run states a fault model — "this link drops one message in five" — and
the model silently does not cover a channel. That gap is invisible: the suite
stays green and the seeds mean less than they claim. An error names the exact
function and stops the run, so what is supported and what is not is discoverable
by using it rather than by reading a table.

Inert without a network, so a module holding one of these on a path no simulation
reaches still builds and still runs normally outside a run.
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
                        "Use gen_server:call/2,3, cast/2 or reply/2, which are routed, "
                        "or run without a network"
                    >>}}
            )
    end.

%% A channel is a pair of **pids**, and every way of addressing a local process
%% normalizes to one.
%%
%% This is a correctness requirement, not ergonomics. A system that sometimes
%% sends to `Peer` and sometimes to `{peer, node()}` — which is what
%% `gen_server:cast/2` to a named member looks like — would otherwise open two
%% channels to one process, each with its own ordering clamp, and a message on one
%% could overtake a message on the other. Same target, same channel, however it
%% was addressed.
%%
%% A name is resolved **at send time**, so a delayed message is committed to the
%% process that held the name when it was sent. That is a real difference from
%% `erlang:send/2`, which resolves on delivery, and it only shows up if the name
%% is re-registered to a different process while a message to it is in flight.
%%
%% `undefined` — an unregistered name, a remote node, a `{global, _}` or
%% `{via, _, _}` — means "not a channel this module can identify", and the caller
%% falls back to sending directly. `eta` does not simulate distribution.
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
Tagging casts by the envelope would make `{tags, _}` select every cast in the
system or none of them, which is useless for the traffic it is most wanted for.

**Scope is not a tuning knob.** It is how a run states which recovery paths it is
entitled to exercise. Loss on a request channel is recovered in production by the
node event a reconnect delivers, so a run that drops those without also
delivering the event is injecting a fault real distribution cannot produce. A run
that asserts a property *during* a fault — with no heal to pay that debt at — has
to restrict the loss to a channel whose recovery is self-contained, such as a
replication stream a follower repairs from a version discontinuity.

An out-of-scope message is delivered **without drawing from the RNG**. A message
the policy cannot affect must not shift the fault schedule for the ones it can.
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
Puts processes on a simulated node.

This is how a system says **where its network is**, and it is worth preferring
over `scope` for that job. Faults apply to a send only when both ends are placed
and their nodes differ — so the fault model becomes a fact about the topology
rather than a predicate a harness has to write correctly.

```erlang
eta_net:place(node_a, [MemberA]),
eta_net:place(node_b, [MemberB]),
eta_net:place(node_c, [MemberC]).
```

That distinction is not cosmetic. The harness this module replaced faulted
exactly the right set *by accident*, because its hook sat on the one function
that inter-member traffic went through — the seam's location was the fault model.
Re-describing that as a predicate got it wrong twice on a codebase whose fault
model was already written down, both times producing a plausible-looking
violation of a real safety property. A topology cannot make that mistake, because
"crosses a link" is the same statement in the simulation as in production.

## Unplaced means "not on the network"

A process nobody placed is never faulted, in either direction. That is the safe
default, and it is also a useful thing to say deliberately: a component that
coordinates through a database rather than by message passing is *modelled* by
messages in a single-VM simulation, and those messages should not be dropped
because the real ones do not exist. `dgen` places its registry members and leaves
their electors unplaced for exactly this reason — the elector's peer traffic
stands in for durable queue operations.

It also means a network is inert for anything it does not know about, including
processes left over from an earlier test in the same VM.

**If nothing at all is placed, every link is faultable.** A network with no
topology behaves as it did before there was one, so `place/2` is opt-in.

## Children inherit

A process spawned by a placed process is placed on the same node, at the moment
`eta_sched` adopts it. Without that, a topology would be as stale as whatever
last declared it — a transaction worker or a collector spawned mid-run would send
across a link nobody had told the network about, and the fault would silently not
happen. Inheritance is what makes this a description of the system rather than a
snapshot of it.
""".
-endif.
-spec place(term(), [dest()]) -> ok.
place(Node, Pids) ->
    ok = require_running(place),
    true = ets:insert(?PLACE, {{node, Node}, true}),
    true = ets:insert(?PLACE, [
        {{pid, P}, Node}
     || D <- Pids, P <- [normalize(D)], is_pid(P)
    ]),
    ok.

-if(?DOCATTRS).
-doc "The simulated node a process is on, or `undefined`.".
-endif.
-spec node_of(dest()) -> term() | undefined.
node_of(Dest) ->
    case ets:lookup(?PLACE, {pid, normalize(Dest)}) of
        [{_, Node}] -> Node;
        [] -> undefined
    end.

-if(?DOCATTRS).
-doc """
Places `Child` where `Parent` is. Called by `eta_sched` as it adopts a process;
a harness does not call this. See `place/2`.
""".
-endif.
-spec inherit(pid(), pid()) -> ok.
inherit(Parent, Child) ->
    case running() andalso ets:lookup(?PLACE, {pid, Parent}) of
        [{_, Node}] ->
            true = ets:insert(?PLACE, {{pid, Child}, Node}),
            ok;
        _ ->
            ok
    end.

%% Whether a send crosses a simulated link, and so is something the policy may
%% fault. No topology at all means every link is faultable, which is what a
%% network that predates `place/2` expects.
crosses_link(From, Dest) ->
    case ets:info(?PLACE, size) of
        0 ->
            true;
        _ ->
            case {node_of(From), node_of(Dest)} of
                {undefined, _} -> false;
                {_, undefined} -> false;
                {Same, Same} -> false;
                {_, _} -> true
            end
    end.

%% A reference to cut or partition: a node if it names one, otherwise a process.
%%
%% Only an atom is ambiguous, and only if a node has been given the same name as a
%% registered process. Nodes win, because `partition(node_a, node_b)` is the
%% overwhelmingly common thing to mean; a pid is never ambiguous, so a caller who
%% hits the collision can pass one.
resolve(Ref) when is_atom(Ref) ->
    case ets:member(?PLACE, {node, Ref}) of
        true -> {node, Ref};
        false -> {pid, normalize(Ref)}
    end;
resolve(Ref) ->
    {pid, normalize(Ref)}.

%% Every process on a node, for delivering a link signal.
on_node(Node) ->
    [P || [P] <- ets:match(?PLACE, {{pid, '$1'}, Node})].

merge_policy(Policy) ->
    maps:merge(?DEFAULT_POLICY, maps:with(maps:keys(?DEFAULT_POLICY), Policy)).

%% Everything that configures or inspects a network needs one to exist, and the
%% failure without this reads as an ETS `badarg` from somewhere in the middle of
%% this module — which says nothing about the actual mistake, and the actual
%% mistake is a specific and recurring one: a run whose options do not ask for a
%% network, driving a harness that assumes one. A replayed fixture saved before
%% the `net` option existed is exactly that.
%%
%% `send/2` and friends are deliberately *not* guarded. Their contract is to be
%% inert without a network, which is what lets a transformed module behave
%% normally outside a simulation.
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
%% Targeted faults
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc """
Drops everything sent from `From` to `To` until `heal/2`. One direction.

Messages already in flight on that channel are cancelled, because a link that
goes down loses what was on it. They are counted as both `dropped` and
`cancelled`.
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
Cuts both directions, optionally delivering a link-down signal to both ends.

```erlang
eta_net:partition(A, B, #{signal => {nodedown, node()}})
```

**Prefer the signalling form.** Dropping messages models a failing link, and a
real link failure is never only lost messages: it delivers `nodedown`/`nodeup` to
both ends, and systems deliberately hang recovery off those. A harness that drops
without signalling has injected something no network produces — messages
vanishing while both ends still believe the link is up — and the unrecovered
state it then reports is an artefact, not a defect. The Elixir original learned
this from a follower that had optimistically deleted a row whose request was
dropped and never recovered; it looked exactly like a product bug and was not
one.

The trap on the other side is real too: recovery driven by the signal may repair
the very divergence under test. A run asserting a property *during* a fault
therefore restricts loss by `scope` instead. Both options are wrong in some
situation, which is why neither is the default and why this argument is
mandatory-looking.

The signal is delivered directly, not routed — it is the link event itself, so a
cut channel must not swallow it.
""".
-endif.
-spec partition(dest(), dest(), #{signal => term()}) -> ok.
partition(A, B, Opts) ->
    ok = cut(A, B),
    ok = cut(B, A),
    signal(Opts, endpoints(A) ++ endpoints(B)).

-if(?DOCATTRS).
-doc "Heals both directions. See `heal_partition/3`.".
-endif.
-spec heal_partition(dest(), dest()) -> ok.
heal_partition(A, B) ->
    heal_partition(A, B, #{}).

-if(?DOCATTRS).
-doc """
Heals both directions, optionally delivering a link-up signal to both ends —
`#{signal => {nodeup, node()}}`. See `partition/3` for why that matters.
""".
-endif.
-spec heal_partition(dest(), dest(), #{signal => term()}) -> ok.
heal_partition(A, B, Opts) ->
    ok = heal(A, B),
    ok = heal(B, A),
    signal(Opts, endpoints(A) ++ endpoints(B)).

%% Who a link signal reaches. Partitioning two nodes has to tell **everything on
%% them**, not two representative processes: a system hangs its recovery off that
%% event, and a member that never saw it is a member the harness has quietly
%% excluded from recovering.
endpoints(Ref) ->
    case resolve(Ref) of
        {node, Node} -> on_node(Node);
        {pid, Pid} -> [Pid]
    end.

signal(#{signal := Sig}, Pids) ->
    lists:foreach(fun(P) -> is_pid(P) andalso (catch erlang:send(P, Sig)) end, Pids),
    ok;
signal(_, _) ->
    ok.

-if(?DOCATTRS).
-doc """
Drops exactly the next `K` messages from `From` to `To`, then resumes.

Precise where a probability is not: "lose the reply to this one operation" is a
question a `drop_p` cannot ask, and the answer is usually a shorter
counterexample.
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

Precise in a way neither `cut/2` nor `drop_next/3` can be, because it counts
*within one kind of message*. Severing a channel part-way through a single group
commit is the example that motivated it: a batch interleaves on the wire with
acks and replies, so "drop messages 2..4 of this batch" cannot be said as a raw
message count. Dropping a strict subset of one batch is a materially different
fault from dropping a whole channel — it can leave no version discontinuity for
gap detection to notice, which is exactly the shape of defect worth hunting.

See `set_policy/1` for what a tag is; for a cast it is the payload's leading
element, not `'$gen_cast'`.
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
%% - `escaped` — a send the schedule did not order, into a process it controls.
%%   Delivered untouched, drawn for nothing, and **counted**: nothing else in a
%%   run reports it, and it is enough on its own to make a trace diverge.
%% - `foreign` — neither end belongs to the run. Delivered untouched and not
%%   counted.
%%
%% The first test is exact rather than approximate: during a step one process runs
%% and every other one the scheduler owns is suspended, so a send from anything
%% else happened at a moment wall-clock timing chose. Asking "is the sender
%% registered with the scheduler" instead is both weaker and wrong — weaker
%% because a process can be owned and still be running loose, wrong because the
%% answer lags whatever the scheduler last adopted.
%%
%% The `foreign` case is not a softening of that, it is the same rule applied to
%% the destination. This module is global to the VM, so **every** transformed
%% module in the VM sends through it — including a leftover system from an earlier
%% test file, still firing its own heartbeat. Such a send cannot touch the run:
%% its target is not a process the scheduler owns, so nothing about it can change
%% what is runnable. Counting it anyway made a 250-seed sweep fail with
%% `unmanaged: 1` when run after the rest of a suite and pass when run alone,
%% which is the worst kind of diagnostic — one that fires on something the reader
%% cannot act on.
%%
%% Crucially, `foreign` still draws nothing. A stranger's message must not be able
%% to shift the fault schedule, which is the real hazard a shared VM presents and
%% the reason this is a three-way answer rather than a boolean.
%%
%% `undefined` — no step in progress — is not off-schedule at all. The driver runs
%% between steps, and a harness injecting an operation there is doing something
%% the trace records. Outside a run there is no scheduler and everything is
%% scheduled by default, which keeps a hand-driven network behaving as it did.
classify(From, Dest) ->
    case eta_sched:stepping() of
        undefined -> scheduled;
        From -> scheduled;
        Stepping -> escaped_or_foreign(Dest, Stepping)
    end.

%% "Does the scheduler own the destination" answered without asking the scheduler:
%% while a step is in progress every process it owns *except* the stepped one is
%% suspended, and nothing else in a normal system is.
escaped_or_foreign(Dest, Stepping) when Dest =:= Stepping ->
    escaped;
escaped_or_foreign(Dest, _Stepping) ->
    case erlang:process_info(Dest, status) of
        {status, suspended} -> escaped;
        _ -> foreign
    end.

%% `Tag` is what a policy scopes on and what the log records, and it is passed
%% rather than derived so that `cast/2` and `reply/2` can name the payload instead
%% of OTP's envelope. See their clauses.
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
