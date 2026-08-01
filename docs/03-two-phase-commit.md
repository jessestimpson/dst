# A worked example: two-phase commit

`eta` ships with a small two-phase commit implementation as an example system
under test. It lives in `test/support/eta_2pc*.erl`: about 170 lines of protocol
plus 150 of harness, with a bug planted in it that only an unlucky interleaving
reveals.

This page reads that example. If you'd rather build something from scratch,
[A Journey Through DST](06-a-journey-through-dst.md) does that instead.

## The protocol

A coordinator wants a group of participants to agree on a transaction. It asks
each one to *prepare*, and each votes `yes` or `no`. If every vote is `yes` the
coordinator decides `commit`, otherwise `abort`, and it tells everyone. The
property that has to hold is **atomicity**: no 2 participants may reach opposite
conclusions about the same transaction.

3 processes vote, 1 decides, and a client waits for the answer.

## The participants

A participant is an ordinary `gen_server` with no timers, so it needs no parse
transform at all:

```erlang
handle_cast({prepare, TxId, Coordinator}, St = #{tab := Tab, index := Index}) ->
    case plan(Tab, TxId, Index) of
        stall ->
            {noreply, St};
        Vote ->
            record(Tab, TxId, Index, {prepared, Vote}),
            gen_server:cast(Coordinator, {vote, TxId, Index, Vote}),
            {noreply, St}
    end;
```

A participant's *plan*, meaning what it will vote for a given transaction, isn't
decided by the participant. The driver writes it into a shared ETS table before
the transaction starts and the participant looks it up, which makes the workload
part of the seeded, replayable state rather than something each process invents
for itself.

`stall` means the participant never votes. That case exists to put the virtual
clock on the critical path: with one participant silent, the only thing that can
resolve the transaction is the coordinator's prepare timeout.

Everything the participant decides also goes into that table rather than into
its `gen_server` state, because the atomicity invariant has to read it while
every one of these processes is suspended. `eta_observe` would also make that
readable, and choosing between the 2 comes up on every system. Our rule of
thumb: **if the system already keeps the state somewhere readable, read it
there. If the state exists only inside a process, publish it.** The table wins
here because the driver has to write plans into it anyway, and because an
accumulating history is the wrong shape for `eta_observe`, which publishes a
snapshot of current fields.

## The coordinator, and the timer

The coordinator is where the header is needed, for exactly one line:

```erlang
-include_lib("eta/include/eta.hrl").

-define(VOTE_TIMEOUT, 30000).

handle_call({run_tx, TxId}, From, St) ->
    #{participants := Participants, pending := Pending} = St,
    Self = self(),
    [gen_server:cast(P, {prepare, TxId, Self}) || {_Index, P} <- Participants],
    Timer = erlang:send_after(?VOTE_TIMEOUT, self(), {vote_timeout, TxId}),
    ...
```

Under a simulation build the header brings the parse transform, so that
`erlang:send_after/3` becomes `eta_time:send_after/3`. Under a release build it
brings nothing and the line ships as written.

30 seconds is long enough that the timeout never fires by accident and short
enough to be interesting, and it costs nothing because it's virtual. When every
participant has stalled and no process is runnable, the driver advances the
clock straight to that deadline and the run continues.

## The planted bug

The coordinator has a second mode, selectable by config, in which it decides on
the first vote to *arrive* rather than waiting for all of them:

```erlang
on_vote(TxId, _Index, Vote, _Tx, St = #{mode := first_vote_wins}) ->
    Decision =
        case Vote of
            yes -> commit;
            no  -> abort
        end,
    {noreply, decide(TxId, Decision, St)};
```

Most of the time this behaves correctly. If a `no` arrives first the transaction
aborts. If every vote is `yes` it commits. Either way everybody agrees. The
defect is visible only when a `yes` wins the race and a `no` arrives afterwards,
and even then only because a participant that voted `no` refuses to commit
regardless of what it's told:

```erlang
Final =
    case {Decision, plan(Tab, TxId, Index)} of
        {commit, no} -> aborted;
        {commit, _}  -> committed;
        {abort, _}   -> aborted
    end,
```

That refusal converts a wrong decision into a *visible* atomicity violation.
Without it the coordinator would be quietly wrong and every participant would
agree with it, which is a nastier class of bug and one this example doesn't try
to model.

So reaching the defect needs at least one `no` vote in a transaction, plus a
schedule in which a `yes`-voting participant is stepped before the `no`-voting
one. The generator draws `no` about a fifth of the time per participant. The
rest is the scheduler's job.

## The harness

6 required callbacks, `init/2` through `terminate/1`, and one optional.

### init/2

```erlang
init(Seed, Config) ->
    Tab = ets:new(eta_2pc, [public, set]),
    N = maps:get(participants, Config, 3),
    Mode = maps:get(mode, Config, correct),

    Participants = [...],
    {ok, Coordinator} = eta_2pc_coordinator:start_link(Tab, Participants, Mode),

    {ok, #{tab => Tab, coordinator => Coordinator, participants => Participants,
           clients => [], next_tx => 1, plan => maps:get(plan, Config, random)}}.
```

The table is `public` because 3 different kinds of process touch it: the driver
writes plans, the participants write their states, and the invariant reads all
of it.

**`init/2` has to return with the system quiescent.** Not merely started, and
not merely ready, but idle. Anything still in flight when the driver takes over
ran on the real scheduler, outside the schedule. For two-phase commit that's
free, since `start_link` returns once `init/1` has run. For a real cluster it
isn't, and [page 4](04-writing-a-system-under-test.md) covers what it takes.

### processes/1

```erlang
processes(#{coordinator := C, participants := Ps, clients := Clients}) ->
    [C | [P || {_Index, P} <- Ps]] ++ Clients.
```

The driver consults this after every operation, so processes an operation
creates get picked up. Pids it already knows are ignored, so returning the whole
set every time is both correct and expected.

**The order is part of the contract.** `eta_sched` assigns ids in registration
order and the trace records ids, so a list that comes out differently between
runs makes the same seed pick different processes. The resulting trace
difference looks exactly like a scheduling bug.

### generate/2

```erlang
generate(#{participants := Participants, next_tx := TxId}, Rand0) ->
    {Plan, Rand} = lists:foldl(
        fun({Index, _Pid}, {Acc, R0}) ->
            {Roll, R1} = rand:uniform_s(R0),
            {[{Index, vote_for(Roll)} | Acc], R1}
        end,
        {[], Rand0},
        Participants
    ),
    {{run_tx, TxId, lists:reverse(Plan)}, Rand}.

vote_for(Roll) when Roll < 0.70 -> yes;
vote_for(Roll) when Roll < 0.90 -> no;
vote_for(_Roll) -> stall.
```

Draw from the `rand` state you're handed and return the advanced state. Drawing
entropy from anywhere else, like `rand:uniform/1`, `erlang:unique_integer/0` or
the clock, breaks replay silently.

Votes are mostly `yes` so that transactions usually commit and the interesting
cases stay rare enough that the scheduler has to go looking for them.

A `plan` config option pins the votes to a fixed list, which is how a test
isolates the schedule as the only variable. Same votes, different seed, and any
change in outcome is the interleaving.

### execute/2

```erlang
execute({run_tx, TxId, Plan}, Sut = #{tab := Tab, coordinator := Coordinator}) ->
    [ets:insert(Tab, {{plan, TxId, Index}, Vote}) || {Index, Vote} <- Plan],

    Client = eta_run:spawn_op(fun() ->
        Result = gen_server:call(Coordinator, {run_tx, TxId}, infinity),
        ets:insert(Tab, {{client, TxId}, Result})
    end),

    Sut#{clients := [Client | maps:get(clients, Sut)], next_tx := TxId + 1}.
```

**`execute/2` must not block.** Every process the scheduler owns is suspended,
so a synchronous call from the driver into one of them is never answered.
Operations are issued by spawning a process to perform them, which is the right
model anyway: the client is part of the concurrent system, and the next
`processes/1` call picks it up and interleaves it like everything else.

The spawn goes through `eta_run:spawn_op/1` rather than `spawn/1`. The driver
isn't traced, so a process it creates isn't adopted until the driver registers
it, and in that window the new process runs free. 2 clients created by
operations injected close together race each other to the coordinator's mailbox,
and the order they arrive in is decided by wall clock. `spawn_op/1` parks the
process on a handshake until registration is done.

Note `infinity` on the call. A client under simulation should never be waiting
on the real clock.

### check/1

```erlang
check(#{tab := Tab, next_tx := Next}) ->
    first_violation(lists:seq(1, Next - 1), Tab).

first_violation([TxId | Rest], Tab) ->
    case lists:usort(eta_2pc_participant:decisions(Tab, TxId)) of
        Mixed when length(Mixed) > 1 ->
            {violation, #{property => atomicity,
                          detail => <<"participants disagreed about a transaction">>,
                          tx => TxId,
                          decision => coordinator_decision(Tab, TxId),
                          participants => [...]}};
        _ ->
            first_violation(Rest, Tab)
    end.
```

Nothing in there sends a message or waits for anything. It's an `ets:tab2list`
and some list operations. An invariant that calls into a suspended process
either hangs or, worse, gets a plausible answer from an API that swallowed its
own timeout, and then passes while checking nothing.

**Give the violation a `property` key.** `eta_shrink` uses it to tell "the same
failure" from "a different failure that also happens to be a failure". Without
it, a shrinker with more than one invariant to choose from will reduce one bug
into another and report the result as progress.

### labels/1, the optional one

```erlang
labels(#{coordinator := C, participants := Ps, clients := Clients}) ->
    maps:from_list(
        [{C, coordinator}] ++
            [{Pid, {participant, Index}} || {Index, Pid} <- Ps] ++
            [{Pid, {client, N}} || {N, Pid} <- lists:enumerate(lists:reverse(Clients))]
    ).
```

Without a name a step reads `p3`; with one, `participant-2`. Names are ordinary
terms; `eta_log` renders `{participant, 2}` as `participant-2`.

Note what's doing the work here and what isn't. The coordinator and the
participants call `?ETA_LABEL` in their own `init/1`, and a self-reported label
wins, so this callback isn't what names them.

**The clients are the reason it exists.** They're anonymous funs handed to
`eta_run:spawn_op/1`, with no module to put a `?ETA_LABEL` in, so the harness is
the only thing that knows one of them is transaction 3. That's the general
shape: reach for `labels/1` when the process can't name itself, whether it's a
fun like this, something from a library you don't own, or a module you
deliberately kept `eta.hrl` out of.

It's called once, after the run, which is why it takes the whole state rather
than one pid at a time. Ids are handed out as processes register, so the mapping
can't exist before the run is over.

### terminate/1

```erlang
terminate(Sut = #{coordinator := C, participants := Ps, tab := Tab}) ->
    [exit(Client, kill) || Client <- maps:get(clients, Sut)],
    [stop(P) || {_Index, P} <- Ps],
    stop(C),
    catch ets:delete(Tab),
    ok.
```

Clients first, and by killing rather than asking. By the time `terminate/1` runs
the driver has released the scheduler, so anything still in flight is running
again, and a client that reaches its `ets:insert` after the table has been
deleted crashes with a `badarg` that has nothing to do with the run.

## Running it

In correct mode, over 40 seeds:

```erlang
[eta_run:run(eta_2pc, #{seed => S, max_ops => 25, max_steps => 20000})
 || S <- lists:seq(1, 40)].
```

Every one comes back `#{outcome := ok}`.

Turning the defect on:

```erlang
eta_run:run(eta_2pc, #{seed => 1, max_ops => 25, max_steps => 20000,
                       config => #{mode => first_vote_wins}}).
```

Seed 1 fails after 19 steps:

```erlang
#{outcome := {violation, #{property := atomicity,
                           tx := 5,
                           decision := commit,
                           detail := <<"participants disagreed about a transaction">>,
                           participants := [{1, aborted}, {2, {prepared, yes}}, {3, committed}]}},
  steps := 19,
  ops := 7}
```

Participant 1 voted `no` and refused to commit. Participant 3 committed. The
coordinator had already decided `commit` on participant 2's `yes`, which is why
participant 2 is still at `{prepared, yes}`; the decision hasn't reached it yet.
The run stopped after 7 of its 25 operations, because `eta_run` ends on the
first violation.

## Making the failure readable

The trace is everything the driver did, in order: `{step, Id}`, `{op, Op}` and
`{clock, Ms}` entries. For this failure it's 26 of them, which is small only
because the failure happened early. On a larger system a first trace runs to
1000s.

```erlang
#{outcome := {violation, _}, trace := Trace} =
    eta_run:run(eta_2pc, Opts),

#{trace := Minimal, original := 26, shrunk := 8, verified := true} =
    eta_shrink:shrink(eta_2pc, Trace, Opts).
```

26 entries down to 8, in 142 candidate replays and 24 msec:

```erlang
[{op,   {run_tx, 2, [{1, yes}, {2, yes}, {3, yes}]}},
 {op,   {run_tx, 3, [{1, yes}, {2, yes}, {3, no}]}},
 {step, 5},
 {step, 0},
 {step, 2},
 {step, 0},
 {step, 3},
 {step, 1}]
```

If that means little to you, that's the right reaction. **A trace is a scheduler
artifact, not an explanation.** `{step, 5}` records which process the scheduler
chose and says nothing about what that process did, and the ids are anonymous
positions assigned in registration order. Turning one into a story means
reconstructing every mailbox state by hand.

What shrinking gives you is a trace short enough to be worth narrating.
`eta_log` is what narrates it.

### Reading it

`eta_run` writes its own decisions into the same log your system writes to, so
replaying the shrunk trace and printing gives you one timeline:

```erlang
eta_run:replay(eta_2pc, Minimal, Opts),
eta_log:analyze().
```

```
   30  $eta           {op,{run_tx,5,[{1,no},{2,yes},{3,stall}]}}
   39  participant-2  {voted,5,yes}
   42  coordinator    {decided,5,commit}
   53  participant-1  {voted,5,no}
   55  participant-1  {decided,5,commit,aborted}
```

Line 42 is the bug: the coordinator decided on one vote. Line 53 is the `no`
arriving too late, and line 55 is the participant refusing, which is what makes
the disagreement visible.

The events come from `?ETA_LOG` calls in the participant and coordinator, about
one per protocol decision. `eta_log:analyze(#{until => N})` stops the output
where the story does, which matters because everything after the run proper is
the scheduler releasing the system during teardown.

### What the shrinker couldn't remove

Transaction 2 contributes nothing, and its survival shows the shrinker's one
real limitation. Step ids are positional, so removing that leading operation
renumbers every process created after it, the surviving `{step, Id}` entries
stop naming what they named, the candidate runs clean, and the removal is
reverted as unsafe. The trace is still a genuine reproduction. It's the
minimality that's approximate.

2 things about how the shrinker works matter when you read a result.

**Its oracle has 3 answers rather than 2.** Replaying a candidate gives the same
violation, a clean run, or a *divergence*, meaning the candidate isn't a valid
schedule and is evidence of nothing. Treating divergence as "did not fail"
produces a shrinker that appears to work while quietly reverting safe removals.

**The result is re-recorded before it's verified.** Candidates replay leniently,
skipping entries, so a surviving candidate is a recipe rather than an artifact.
`shrink/3` finishes by replaying what the run actually executed, under strict
rules. If that doesn't reproduce you get `verified => false` and the original
trace back.

## What this example leaves out

No faults are injected. No message loss, no crashes, no partitions. Two-phase
commit here is purely a scheduling exercise. Fault injection is a separate axis
with a reproducibility trap of its own, described on [page 5](05-gotchas.md).

## Next

[Writing a system under test](04-writing-a-system-under-test.md) generalizes
from this example to the callbacks and practices a real system needs.
