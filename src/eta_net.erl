-module(eta_net).

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

## Where the network is

`place/2` assigns processes to simulated nodes, and faults then apply only to
sends whose ends are on different ones. Prefer it to `scope`: topology says where
the wire is, `scope` says which traffic on it a run is entitled to break. With no
topology declared, every link is faultable.

Delay is virtual — a delayed message becomes a deadline in `eta_time`'s wheel — so
waiting one out costs no real time.
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
    Seed = maps:get(seed, Opts, 0),
    ets:insert(?STATE, [
        {rand, rand:seed_s(exsss, {Seed, Seed + 11, Seed + 23})},
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
`gen_server:call/2,3`, with **both legs on the network**.

The request is an ordinary send this module routes. The reply cannot be reached
from here — `gen:reply` sends it from inside `gen_server` — so `eta_transform`
brings it on from the other end, rewriting a `gen_server` module's
`handle_call/3` returns so `{reply, R, S}` becomes `eta_net:reply(From, R)` plus
`{noreply, S}`.

Both legs can therefore be dropped or delayed independently, which is what makes
the asymmetric fault reachable: the work happened, the caller never learned it
did.

Monitors the callee and takes a timeout, as `gen_server:call/3` does, with two
differences:

- **The timeout is virtual**, so waiting one out on a dropped request costs no
  real time and fires where the schedule chose.
- **The monitor is not routed**, and cannot be: `DOWN` is a signal, not a
  message, so a partition does not suppress it. Model peer failure as a crash and
  network failure as a cut; they are different faults.

**Raises `unrouted_reply` if the callee was not built with the transform**, since
its reply then comes around the network and one direction of the channel is
silently unfaultable.
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
`gen_server:reply/2`, routed — so a reply can be lost independently of the request
that caused it.

Addressed to the caller's pid rather than to the alias in the tag. Both land in
the same mailbox and the caller's selective receive matches on the tag either
way.

`eta_transform` also rewrites `handle_call/3` returns to call this, which is how
the reply leg of `call/3` gets onto the network.
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
What `eta_transform` points the messaging functions this module does not
implement at: broadcasts and multi-node calls, the asynchronous
request/response interface, and the `gen_statem` and `gen_event` client APIs.
The list is `?NET_UNSUPPORTED` in `eta_transform`.

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
                        "Use gen_server:call/2,3, cast/2 or reply/2, which are routed, "
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
Puts processes on a simulated node — how a system says where its network is.

Faults apply to a send only when both ends are placed and their nodes differ, so
the fault model follows the topology rather than a predicate the harness has to
keep correct.

```erlang
eta_net:place(node_a, [MemberA]),
eta_net:place(node_b, [MemberB]).
```

**Unplaced means not on the network**: never faulted, in either direction. That
is the safe default and also a useful thing to say deliberately — a component
that really coordinates through a database is only *modelled* by messages here,
and dropping them injects a failure the real system cannot have. It also keeps a
network inert for processes it does not know about, such as leftovers from an
earlier test in the same VM.

If nothing at all is placed, every link is faultable, so `place/2` is opt-in.

**Children inherit** their parent's node as `eta_sched` adopts them, so a worker
spawned mid-run does not send across a link the network was never told about.
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
%% fault. No topology at all means every link is faultable.
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
%% Only an atom is ambiguous — a node named the same as a registered process — and
%% nodes win; pass a pid to disambiguate.
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
%% Targeted faults
%% ---------------------------------------------------------------------------

-if(?DOCATTRS).
-doc """
Drops everything sent from `From` to `To` until `heal/2`. One direction.

Messages already in flight on that channel are cancelled, as a failing link loses
what was on it, and counted as both `dropped` and `cancelled`.
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

**Prefer the signalling form.** A real link failure is never only lost messages:
it delivers `nodedown`/`nodeup` to both ends and systems hang recovery off those.
Dropping without signalling injects something no network produces — messages
vanishing while both ends still believe the link is up — and the unrecovered state
that follows is an artefact rather than a defect.

The opposite trap is real too: recovery driven by the signal may repair the very
divergence under test, so a run asserting a property *during* a fault restricts
loss by `scope` instead. Neither is right in every situation, which is why there
is no default.

The signal is delivered directly rather than routed, so a cut channel cannot
swallow it, and reaches every process on both nodes.
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

%% Who a link signal reaches: everything on the node, not a representative — a
%% process that never saw the event is one the harness excluded from recovering.
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
