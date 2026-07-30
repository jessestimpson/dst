# A worked example: two-phase commit

`dst` ships with a small two-phase commit implementation as an example system
under test. It lives in `test/support/dst_2pc*.erl`, it's about 170 lines of
protocol plus 150 lines of harness, and it has a bug planted in it that only an
unlucky interleaving reveals. This page walks through the whole thing.

## The protocol

A coordinator wants a group of participants to agree on a transaction. It asks
each one to *prepare*, and each votes `yes` or `no`. If every vote is `yes` the
coordinator decides `commit`, otherwise `abort`, and it tells everyone the
decision. The property that has to hold is **atomicity**: no 2 participants may
reach opposite conclusions about the same transaction.

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

A participant's *plan*, meaning what it will vote for a given transaction,
isn't decided by the participant. The driver writes it into a shared ETS table
before the transaction starts and the participant looks it up. That makes the
workload part of the seeded, replayable state rather than something each
process invents for itself.

`stall` means the participant never votes. That case exists to put the virtual
clock on the critical path of a run. With one participant silent, the only
thing that can resolve the transaction is the coordinator's prepare timeout.

Everything the participant decides also goes into that shared table rather than
into its `gen_server` state, because the atomicity invariant has to read it
while every one of these processes is suspended.

`dst_observe` would also make that state readable, and choosing between the two
comes up on every system. The table wins here because the driver has to write
plans into it before each transaction anyway. Once it exists, recording
outcomes in the same place costs nothing and gives the invariant one thing to
read. `dst_observe` is the better tool when what you need is a process's
*current* state, like who it believes the leader is or what epoch it's on, and
there's no natural place that already lives. It republishes named fields on
every callback return and `read/1` copies whatever was published, so an
accumulating history of every transaction is the wrong shape for it. Our rule
of thumb: if the system already keeps the state somewhere readable, read it
there. If the state exists only inside a process, publish it.

## The coordinator, and the timer

The coordinator is where the transform is needed, for exactly one line, the
timer:

```erlang
-compile({parse_transform, dst_transform}).

-define(VOTE_TIMEOUT, 30000).

handle_call({run_tx, TxId}, From, St) ->
    #{participants := Participants, pending := Pending} = St,
    Self = self(),
    [gen_server:cast(P, {prepare, TxId, Self}) || {_Index, P} <- Participants],
    Timer = erlang:send_after(?VOTE_TIMEOUT, self(), {vote_timeout, TxId}),
    ...
```

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

Most of the time this behaves correctly. If a `no` happens to arrive first the
transaction aborts, everybody agrees, and nothing looks wrong. If every vote is
`yes` the transaction commits, and again nothing looks wrong. The defect is
visible only when a `yes` wins the race and a `no` arrives afterwards, and even
then only because a participant that voted `no` refuses to commit regardless of
what it's told:

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

So reaching the defect requires at least one `no` vote in a transaction, plus a
schedule in which a `yes`-voting participant is stepped before the `no`-voting
one. The workload generator draws `no` about a fifth of the time per
participant. The rest is the scheduler's job.

## The system under test

6 callbacks, `init/2` through `terminate/1`.

### init/2

```erlang
init(Seed, Config) ->
    Tab = ets:new(dst_2pc, [public, set]),
    N = maps:get(participants, Config, 3),
    Mode = maps:get(mode, Config, correct),

    Participants = [...],
    {ok, Coordinator} = dst_2pc_coordinator:start_link(Tab, Participants, Mode),

    {ok, #{tab => Tab, coordinator => Coordinator, participants => Participants,
           clients => [], next_tx => 1, plan => maps:get(plan, Config, random)}}.
```

The table is `public` because 3 different kinds of process touch it. The driver
writes plans into it, the participants write their states, and the invariant
reads all of it.

`init/2` has to return with the system quiescent. Not merely started, and not
merely ready, but idle. Anything still in flight when the driver takes over ran
on the real scheduler, outside the schedule. For two-phase commit that's free,
since `start_link` returns once `init/1` has run. For a real cluster it isn't,
and [page 4](04-writing-a-system-under-test.md) covers what it takes.

### processes/1

```erlang
processes(#{coordinator := C, participants := Ps, clients := Clients}) ->
    [C | [P || {_Index, P} <- Ps]] ++ Clients.
```

The driver consults this after every operation, so processes an operation
creates get picked up. Pids it already knows are ignored, which means returning
the whole set every time is both correct and expected.

The order this list comes back in is part of the contract. `dst_sched` assigns
ids in registration order and the trace records ids, so a list that comes out
differently between runs makes the same seed pick different processes. The
resulting trace difference looks exactly like a scheduling bug.

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

    Client = dst_run:spawn_op(fun() ->
        Result = gen_server:call(Coordinator, {run_tx, TxId}, infinity),
        ets:insert(Tab, {{client, TxId}, Result})
    end),

    Sut#{clients := [Client | maps:get(clients, Sut)], next_tx := TxId + 1}.
```

`execute/2` must not block. Every process the scheduler owns is suspended, so a
synchronous call from the driver into one of them is never answered and the
driver sits there until something times out. Operations are issued by spawning
a process to perform them. That process is picked up by the next `processes/1`
call and interleaved like everything else, which is the right model anyway,
since the client is part of the concurrent system.

The spawn goes through `dst_run:spawn_op/1` rather than `spawn/1`. The driver
isn't traced, so a process it creates isn't adopted by the scheduler until the
driver registers it, and in that window the new process runs free. 2 clients
created by operations injected close together will race each other to the
coordinator's mailbox, and the order they arrive in is decided by wall clock.
`spawn_op/1` parks the process on a handshake until registration is done.

Note `infinity` on the call. A client under simulation should never be waiting
on the real clock.

### check/1

```erlang
check(#{tab := Tab, next_tx := Next}) ->
    first_violation(lists:seq(1, Next - 1), Tab).

first_violation([TxId | Rest], Tab) ->
    case lists:usort(dst_2pc_participant:decisions(Tab, TxId)) of
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
either hangs or, worse, receives a plausible answer from an API that swallowed
its own timeout and then passes while checking nothing.

The violation carries a `property` key. `dst_shrink` uses it to tell "the same
failure" from "a different failure that also happens to be a failure". Without
it, a shrinker with more than one invariant to choose from will reduce one bug
into another and report the result as progress.

### terminate/1

```erlang
terminate(Sut = #{coordinator := C, participants := Ps, tab := Tab}) ->
    [exit(Client, kill) || Client <- maps:get(clients, Sut)],
    [stop(P) || {_Index, P} <- Ps],
    stop(C),
    catch ets:delete(Tab),
    ok.
```

Clients first, and by killing rather than asking. By the time `terminate/1`
runs the driver has released the scheduler, so anything still in flight is
running again, and a client that reaches its `ets:insert` after the table has
been deleted crashes with a `badarg` that has nothing to do with the run.

## Running it

In correct mode, over 40 seeds:

```erlang
[dst_run:run(dst_2pc, #{seed => S, max_ops => 25, max_steps => 20000})
 || S <- lists:seq(1, 40)].
```

Every one comes back `#{outcome := ok}`.

Turning the defect on:

```erlang
dst_run:run(dst_2pc, #{seed => 1, max_ops => 25, max_steps => 20000,
                       config => #{mode => first_vote_wins}}).
```

and seed 1 fails after 19 steps:

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
participant 2 is still at `{prepared, yes}`; the decision hasn't reached it
yet. The run stopped after 7 of its 25 operations, because `dst_run` ends on
the first violation.

## Making the failure readable

The trace is the list of everything the driver did, in order: `{step, Id}`,
`{op, Op}` and `{clock, Ms}` entries. For this failure it's 26 of them, which
is small only because the failure happened early. On a larger system a first
trace runs to 1000s.

```erlang
#{outcome := {violation, _}, trace := Trace} =
    dst_run:run(dst_2pc, Opts),

#{trace := Minimal, original := 26, shrunk := 8, verified := true} =
    dst_shrink:shrink(dst_2pc, Trace, Opts).
```

26 entries down to 8, in 142 candidate replays and 24 msec. Here's the whole
result:

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

Transaction 3 is the one that matters, since participant 3 votes `no`, and the
6 steps after it are the interleaving that lets a `yes` reach the coordinator
first. Process 0 is the coordinator, 1 to 3 are the participants, and 5 is the
client for that transaction.

Transaction 2 contributes nothing, and its survival illustrates the shrinker's
one real limitation. Step ids are positional, so removing that leading
operation renumbers every process created after it, the surviving `{step, Id}`
entries stop naming what they named, the candidate runs clean, and the removal
is reverted as unsafe. The trace is still a genuine reproduction. It's the
minimality that's approximate.

2 aspects of how the shrinker works matter when you read a result.

Its oracle has 3 answers rather than 2. A candidate trace is tested by
replaying it, and the outcome is the same violation, a clean run, or a
*divergence*, meaning the candidate isn't a valid schedule and is evidence of
nothing. Treating divergence as "did not fail" produces a shrinker that appears
to work while quietly reverting safe removals. Here divergence is avoided
rather than classified. Candidates replay in a lenient mode that skips a step
naming a process which isn't currently runnable, which is necessary because
removing an operation strands the steps belonging to the process that operation
created.

And the result is re-recorded before it's verified. A lenient replay silently
dropped entries, so the surviving candidate is a recipe rather than an
artifact. What the run actually executed is the artifact, and `shrink/3`
finishes by replaying that under strict rules. If it doesn't reproduce you get
`verified => false` and the original trace back.

## What this example leaves out

No faults are injected. No message loss, no crashes, no partitions. Two-phase
commit here is purely a scheduling exercise. Fault injection is a separate axis
with a reproducibility trap of its own, described on [page 5](05-gotchas.md).

## Next

[Writing a system under test](04-writing-a-system-under-test.md) generalizes
from this example to the callbacks and practices a real system needs.
