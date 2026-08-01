# A Journey Through DST

This page guides you through the `dst` features. We start from scratch and build
an [ABD Quorum Register](https://scholar.google.com/citations?view_op=view_citation&hl=en&user=eoMK92MAAAAJ&citation_for_view=eoMK92MAAAAJ:d1gkVwhDpl0C) in Erlang. `dst` requires you to build
your distributed system-under-test in a specific manner, to avoid leaking
nondeterminism. This walkthrough explains those requirements through an actual
implementation.

An **ABD Quorum Register** is a simple distributed system that shares a single value,
replicated across 3 nodes. The value is meant to stay correct while clients read
and write concurrently. It's designed for message-passing systems, like the BEAM,
and so its implementation is fairly simple. It also features a counter-intuitive
step that leads to an inconsistency bug whem omitted, making it a good example
for demonstrating DST's bug-finding capabilities.

We recommend you have read [What DST Is](01-what-dst-is.md) to establish a baseline
for the technique.

## What ABD is

We'll start 3 processes, each one is labeled as a "replica", and they all particpate
in message passing to each other. Each process holds a `{Timestamp, Value}` tuple in
its state. For a quorum to be achieved, 2 of the 3 must agree on the tuple contents.

The system provides the following API:

- **Write(V)**: ask a quorum (a set of processes) for their timestamps, take the
  highest, then write `{Highest + 1, V}` to a quorum (not necessarily the same
  process set).
- **Read()**: ask a quorum, take the pair with the highest timestamp, **write
  that pair back to a quorum**, then return the value.

The write-back in the read path is the counter-intuitive part. It may look
redundant: you're writing back a value you just read a couple nanonseconds ago.
But it turns out to be critical to the consistency of the system, and we'll
demonstrate exactly that over the course of this document.

## Step 1: create the project

We'll put the project beside your `dst` checkout so the dependency can be a
plain relative path. We're using `mix` because the tooling makes for a simpler
walkthough, but `rebar` could just as well be used, with some modification.
We'll write the ABD register itself in Erlang - we're going to use `parse_transform`.

```bash
mix new abd
```

## Step 2: build configuration

Clear the Elixir scaffolding and make the directories we need:

```bash
cd abd && rm -rf lib test/abd_test.exs && mkdir -p src test/support
```

Then edit `mix.exs`. In `project/0`, add 3 keys:

```elixir
      erlc_paths: erlc_paths(Mix.env()),
      erlc_options: erlc_options(Mix.env()),
```

Replace the commented-out examples in `deps/0` with:

```elixir
      {:dst, path: "../dst", runtime: false}
      # --- or ---
      {:dst, git: "https://github.com/jessestimpson/dst.git", runtime: false}
```

and add 2 private function pairs at the bottom:

```elixir
  # The ABD register itself is ordinary Erlang in src/. The dst harness lives in
  # test/support and only exists for the test env, so a release never sees it.
  defp erlc_paths(:test), do: ["src", "test/support"]
  defp erlc_paths(_), do: ["src"]

  # The DST define is required for `dst` to engage
  defp erlc_options(:test), do: [:debug_info, {:d, :DST}]
  defp erlc_options(_), do: [:debug_info]
```

Then:

```bash
mix deps.get && MIX_ENV=test mix compile
```

If the compile fails, double-check your configuration.

### Project configuration details

**`runtime: false` rather than `only: :test`.** `dst` requires the inclusion
of an hrl file in the ABD code. For the preprocessor to find the hrl, the `dst`
project must be findable in all compilations. The `DST` define is what disables
all `dst` features in your production code.

**`{:d, :DST}` for the whole test env, not a dedicated simulation profile.**
`dst_time` falls back to the real `erlang` functions whenever no virtual clock
is running, so a transformed module behaves normally outside a simulation.

**No `mod:` in `application/0`.** ABD is small enough not to need one, and this
keeps the walkthrough short.

## Step 3: the replica `gen_server`

`src/abd_replica.erl`:

```erlang
-module(abd_replica).
-behaviour(gen_server).

-include_lib("dst/include/dst.hrl").

-export([start_link/1, stop/1, read/3, write/5, get/1]).
-export([init/1, handle_call/3, handle_cast/2]).

-record(st, {
    index :: pos_integer(),
    ts = {0, 0} :: {non_neg_integer(), non_neg_integer()},
    val = undefined :: term()
}).

start_link(Index) ->
    gen_server:start_link(?MODULE, Index, []).

stop(Pid) ->
    gen_server:stop(Pid).

%% Asynchronous on purpose. A client sends to all three replicas and takes the
%% first two answers, so which two those are has to be a scheduling decision.
read(Pid, Ref, From) ->
    gen_server:cast(Pid, {read, Ref, From}).

write(Pid, Ref, From, Ts, Val) ->
    gen_server:cast(Pid, {write, Ref, From, Ts, Val}).

%% For poking at a replica by hand. Never call this from check/1: under the
%% scheduler the replica is suspended and the call can never be answered.
get(Pid) ->
    gen_server:call(Pid, get).

init(Index) ->
    {ok, #st{index = Index}}.

handle_call(get, _From, St = #st{ts = Ts, val = Val}) ->
    {reply, {Ts, Val}, St}.

handle_cast({read, Ref, From}, St = #st{index = I, ts = Ts, val = Val}) ->
    From ! {read_ack, Ref, I, Ts, Val},
    {noreply, St};
handle_cast({write, Ref, From, Ts, Val}, St = #st{index = I}) ->
    St1 = store(Ts, Val, St),
    From ! {write_ack, Ref, I},
    {noreply, St1}.

%% Standard ABD: keep whichever pair carries the higher timestamp. Timestamps
%% are {Seq, WriterId} so two writers that pick the same Seq still order.
store(Ts, Val, St) when Ts > St#st.ts -> St#st{ts = Ts, val = Val};
store(_Ts, _Val, St) -> St.
```

Let's try it out in the `iex` shell:

```bash
MIX_ENV=test iex -S mix
```

```elixir
{:ok, p} = :abd_replica.start_link(1)
:abd_replica.get(p)
:abd_replica.write(p, make_ref(), self(), {1, 7}, :hello)
flush()
:abd_replica.get(p)
```

RESULT: `{{0, 0}, :undefined}`, a `write_ack`, then `{{1, 7}, :hello}`.

### 3 things worth calling out

**Casts, not `gen_server:call`.** ABD is based on fully non-blocking message
passing. Blocking calls would collect answers in a fixed order. Casts let
a client take the first set that arrive, which makes the quorum set a scheduling
choice, which would typically be a source of nondeterminism.

**Timestamps are `{Seq, WriterId}`, not integers.** 2 concurrent writers can
read the same maximum and both pick `Seq + 1`. Including a another identifier
gets us a total order. This is an ABD implementation detail that you are free
to ignore for the purposes of the demo.

**`get/1` is a trap, and we'll see why later.** `get/1` is useful for poking
at a replica in `iex`. When we write `check/1`, you'll notice we don't use it.
This is because the process will be suspended during the check and unable to
respond to any incoming requests. `check/1` must read shared memory rather than
do message passing.

### The `dst.hrl` include

Technically you can leave this out at this step, but if you do, remember to add it
back in later, where it will be needed.

## Step 4: the client

`src/abd_client.erl`:

```erlang
-module(abd_client).

-include_lib("dst/include/dst.hrl").

-export([read/1, write/3]).

-define(QUORUM, 2).

%% Read phase to find the highest timestamp, then write one past it.
write(Replicas, WriterId, Value) ->
    {{Seq, _}, _} = query_phase(Replicas),
    Ts = {Seq + 1, WriterId},
    ok = write_phase(Replicas, Ts, Value),
    Ts.

%% Read phase, then write back what we found before returning it.
read(Replicas) ->
    {Ts, Val} = query_phase(Replicas),
    ok = write_phase(Replicas, Ts, Val),
    {Ts, Val}.

%% Ask everyone, keep the best answer from the first quorum to reply. Which
%% replicas those are is decided by the schedule, which is the point.
query_phase(Replicas) ->
    Ref = make_ref(),
    [abd_replica:read(Pid, Ref, self()) || Pid <- Replicas],
    collect_reads(Ref, ?QUORUM, {{0, 0}, undefined}).

collect_reads(_Ref, 0, Best) ->
    Best;
collect_reads(Ref, N, Best) ->
    receive
        {read_ack, Ref, _Index, Ts, Val} ->
            collect_reads(Ref, N - 1, best(Best, {Ts, Val}))
    end.

best({BestTs, _}, {Ts, Val}) when Ts > BestTs -> {Ts, Val};
best(Best, _Candidate) -> Best.

write_phase(Replicas, Ts, Val) ->
    Ref = make_ref(),
    [abd_replica:write(Pid, Ref, self(), Ts, Val) || Pid <- Replicas],
    collect_writes(Ref, ?QUORUM).

collect_writes(_Ref, 0) ->
    ok;
collect_writes(Ref, N) ->
    receive
        {write_ack, Ref, _Index} -> collect_writes(Ref, N - 1)
    end.
```

Drive the whole protocol:

```elixir
reps = Enum.map(1..3, fn i -> {:ok, p} = :abd_replica.start_link(i); p end)
:abd_client.read(reps)
:abd_client.write(reps, 1, :v1)
:abd_client.read(reps)
:abd_client.write(reps, 2, :v2)
:abd_client.read(reps)
```

RESULT: `{{0, 0}, :undefined}`, `{1, 1}`, `{{1, 1}, :v1}`, `{2, 2}`,
`{{2, 2}, :v2}`.

### What to notice

**The `dst` project is still not really involved.** We've defined the distributed
system with normal Erlang code, and we'll only need to tweak it to make it compatible
with `dst`. The code that you ship to production is the same code that is under test.

**`abd_client` does not start a process.** It runs inside whatever
process calls it, which right now is your `iex` shell. In a moment the harness
will spawn a process per operation and run these same functions there. Keeping
the 2 things separate is what lets a client be a schedulable participant in the
concurrency without the protocol knowing anything about it.

**`make_ref()` is nondeterministic, but not harmful to the test.** Refs differ between
runs, and unseeded identity is a genuine source of nondeterminism. It's
harmless in this case because the value of the ref never decides anything. It only matches a
client's own replies. Refs and pids break replay when their values
influence *control flow*, which would be very atypical for conventional Erlang.

**Some messages can get stuck in the mailbox.** A quorum is 2, so after `collect_reads`
returns there's still a `read_ack` sitting in the mailbox, and the next phase's
`receive` scans straight past it on a different ref. That's what real ABD code
looks like, and it exercises the trickiest part of `dst_sched`: a process
blocked in a selective receive *with a non-empty mailbox*. `dst` can handle this
just fine. Your actual code will probably want to flush these out, but we
skip that here.

**And the read does a write-back.** Right now it looks redundant, but it's not.
We'll delete it later and observe the result.

## Step 5: the harness

Everything so far has been the actual ABD register. Next is the harness: a
behaviour implementation that `dst_run` will exercise. This module could
be in Elixir, if you want. As written, it doesn't use Erlang compile-time
features.

`test/support/abd_harness.erl`:

```erlang
-module(abd_harness).
-behaviour(dst_harness).

-export([init/2, processes/1, generate/2, execute/2, check/1, terminate/1]).

%% ---------------------------------------------------------------------------
%% Setup and teardown
%% ---------------------------------------------------------------------------

init(_Seed, Config) ->
    Tab = ets:new(abd, [public, ordered_set]),
    true = ets:insert(Tab, {clock, 0}),
    N = maps:get(replicas, Config, 3),
    Replicas = [begin {ok, P} = abd_replica:start_link(I), P end || I <- lists:seq(1, N)],
    {ok, #{tab => Tab, replicas => Replicas, clients => [], next_op => 1}}.

terminate(#{tab := Tab, replicas := Replicas, clients := Clients}) ->
    [exit(C, kill) || C <- Clients],
    [abd_replica:stop(P) || P <- Replicas],
    catch ets:delete(Tab),
    ok.

%% ---------------------------------------------------------------------------
%% What to schedule
%% ---------------------------------------------------------------------------

processes(#{replicas := Replicas, clients := Clients}) ->
    Replicas ++ [C || C <- lists:reverse(Clients), is_process_alive(C)].

%% ---------------------------------------------------------------------------
%% The workload
%% ---------------------------------------------------------------------------

generate(#{next_op := N}, Rand0) ->
    {Roll, Rand} = rand:uniform_s(Rand0),
    case Roll < 0.4 of
        true -> {{write, N, {v, N}}, Rand};
        false -> {{read, N}, Rand}
    end.

execute(Op, Sut = #{tab := Tab, replicas := Replicas, clients := Clients}) ->
    N = op_number(Op),
    Client = dst_run:spawn_op(fun() -> run_op(Op, Tab, Replicas) end),
    Sut#{clients := [Client | Clients], next_op := N + 1}.

op_number({write, N, _Value}) -> N;
op_number({read, N}) -> N.

%% Both phases are timed on a logical clock kept in the same table. Only one
%% process runs at a time, so the counter is a faithful total order of events.
run_op({write, N, Value}, Tab, Replicas) ->
    Start = tick(Tab),
    Ts = abd_client:write(Replicas, N, Value),
    ets:insert(Tab, {{write, N}, Start, tick(Tab), Ts, Value});
run_op({read, N}, Tab, Replicas) ->
    Start = tick(Tab),
    {Ts, Val} = abd_client:read(Replicas),
    ets:insert(Tab, {{read, N}, Start, tick(Tab), Ts, Val}).

tick(Tab) ->
    ets:update_counter(Tab, clock, 1).

%% ---------------------------------------------------------------------------
%% The invariant
%% ---------------------------------------------------------------------------

%% Regularity: if one read finished before another started, the later read may
%% not report an older timestamp than the earlier one.
check(#{tab := Tab}) ->
    Reads = [{S, F, Ts} || {{read, _}, S, F, Ts, _} <- ets:tab2list(Tab)],
    inversion(lists:sort(Reads)).

inversion(Reads) ->
    case [{A, B} || A <- Reads, B <- Reads, sequential(A, B), backwards(A, B)] of
        [] ->
            ok;
        [{A, B} | _] ->
            {violation, #{
                property => no_new_old_inversion,
                detail => <<"a later read reported an older value">>,
                earlier => A,
                later => B
            }}
    end.

sequential({_StartA, FinishA, _TsA}, {StartB, _FinishB, _TsB}) -> FinishA < StartB.
backwards({_StartA, _FinishA, TsA}, {_StartB, _FinishB, TsB}) -> TsB < TsA.
```

Compile:

```bash
MIX_ENV=test mix compile
```

RESULT: compiles clean.

### `init/2` hands over an idle system

`start_link` returns once `init/1` has run, and a fresh replica has an empty
mailbox, so this system is quiescent - the replicas are at a steady-state. It's
important to maintain this property, because `dst_run` will suspend all processes.
If it suspends a busy process, then the actual moment in time of that suspend action
is nondeterministic. Make sure the system's processes are idle.

The ets table is `ordered_set`, which is a determinism decision, because `set`
does not specify a term order in the API contract.

### `processes/1` and `lists:reverse`

`dst_sched` assigns ids in registration order, and the trace records ids, so
the order this callback returns is part of the contract. We prepend new clients
to the list because that's cheap, which means the list itself is in reverse
chronological order, so we reverse it here to hand them over oldest first.

### `generate/2` draws from the state it's handed

40% writes, 60% reads, drawn from the `rand` state `dst_run` passes in and
returning the advanced state. Drawing from anywhere else, `rand:uniform/1` or
the clock or `erlang:unique_integer/0`, breaks replay silently. Again, we're
always on the lookout for nondeterminism leaks.

#### Why generating and executing are separate

`dst` will help us both run new seeds and replay old ones. The replay will avoid
calling generate, but must still call execute.

### `execute/2` spawns using a special `dst` function

Every process is suspended, and `dst_sched` is the only entity allowed to
progress the system. A special `dst_run:spawn_op/1` function must be used. It
prevents the newly spawned process from executing any real code. The pid is
handed over to `dst_run` and `dst_sched` to progress.

### `check/1` is for defining the rules

Our invariant is known as "regularity", sometimes called *no new-old inversion*:

> Once a read has returned a value, no read that starts later may return an older one.

Our `check/1` confirms that the system's timestamps make sense in the context of this
invariant. Every operation gets a start and finish stamp from a simple logical clock
(`ets:update_counter`). Since only one process is ever running at a time, the
counter reliably represents the total order of operations.

As mentioned earlier, we can't call `abd_replica:get/1`. We can't do any message
passing to the system under test. Currently, we're reading from a client-driven
ets table. The last step of this guide will explain how to probe the
hidden state of your system.

## Step 6: the first run

Everything is in place. We're ready to do our first deterministic run.

```bash
MIX_ENV=test iex -S mix
```

```elixir
result = :dst_run.run(:abd_harness, %{seed: 1, max_ops: 20, max_steps: 20_000})
:dst_run.summary(result)
:dst_run.audit(result)
```

RESULT:

```elixir
%{
  outcome: :ok,
  seed: 1,
  ops: 20,
  steps: 103,
  clock_ms: 0,
  skipped: 0,
  modules_loaded: [],
  sched: %{processes: 23, exited: 20, adopted_late: 0, timeouts: 0, steps: 103},
  trace_length: 123
}
```

and `:ok` from `audit/1`.

`summary/1` reduces the rendered size of `result` so that you can read it on
the `iex` shell. The `trace` entries can get quite long, so we simply
report the length of the trace in the summary. We'll make the trace easier to
read later.

**`steps` is the number of scheduler steps taken by `dst_sched`** If you see
20 steps for 20 operations, and nothing `exited`, check `modules_loaded`. A
module being lazily loaded during runtime can break determinism. We'll handle
this in step 7. It will always be something to look out for during all your runs.

Otherwise, let's inspect the other fields a little more closely.

**`outcome`** is `:ok`, `{:violation, Detail}`, or `{:error, Reason}`. We expect
`:ok`. If it's anything else, go back and check your code. At this point we
should have a fully functional and safe ABD register.

**`ops`** should be 20. If it's lower the run ended early, which indicates a bug.

**`clock_ms`** is 0, because our system has no timers at all. Nothing ever
armed one, so the virtual clock never had a reason to move. Later, we'll add
in virtual time via `dst_time`.

**`modules_loaded`** must be empty, and **`sched.adopted_late`** and
**`sched.timeouts`** must be 0. These are the properties that `audit/1` looks for.
`:ok` is what we want here.

`adopted_late` counts processes that ran before the
scheduler owned them, and every one of them is a piece of the interleaving
decided by wall clock rather than by the seed (nondeterminism). A nonzero count
here means the seed won't reproduce.

### Make it a permanent test

Create `test/abd_test.exs`:

```elixir
defmodule AbdTest do
  use ExUnit.Case, async: false

  @opts %{max_ops: 20, max_steps: 20_000, preload: [:abd]}

  test "the register satisfies regularity" do
    for seed <- 1..40 do
      %{outcome: outcome, sched: sched} =
        :dst_run.run(:abd_harness, Map.put(@opts, :seed, seed))

      assert outcome == :ok, "seed #{seed}: #{inspect(outcome)}"
      assert sched.adopted_late == 0, "seed #{seed} adopted #{sched.adopted_late} late"
    end
  end
end
```

```bash
mix test
```

RESULT: green.

`async: false` is required. `dst_time` keeps its state in named ETS tables,
so exactly one virtual clock exists per node, and 2 simulations running
concurrently will corrupt each other. Every test that drives `dst_run` has to
be serial.

Asserting on `adopted_late` in the same loop is a habit worth forming. It costs
one line and it turns a silent loss of determinism into a failing test.

## Step 7: Confirm that a seed is reliable

The entire goal of this endeavor is to define a single RNG seed that can drive
our system the same way every time. If we don't get that, then we've failed,
so it's worth spending some time to confirm.

There is one failure mode in particular that we should call out: the `code_server`.

Start a **fresh** `iex` shell and make this the first thing you run. The first run in a
new VM is a special case and we want it in the sample.

This experiment will group traces that match each other. Notice that we're giving the
same seed on each of 5 runs. We hope to end up with identical traces each time.

```elixir
traces = for _ <- 1..5, do: :dst_run.run(:abd_harness, %{seed: 1, max_ops: 20, max_steps: 20_000}).trace

length(Enum.uniq(traces))

traces
|> Enum.with_index(1)
|> Enum.group_by(fn {t, _i} -> t end, fn {_t, i} -> i end)
|> Map.values()
```

RESULT: `2` and `[[1], [2, 3, 4, 5]]`.

If this is the first thing you run in an `iex` shell, you will see that there are 2 unique traces,
instead of 1.

The grouping shows us that run #1 is the odd-man-out. Runs #2-#5 all match each other. This is
the clue - something is different about that first run on the VM. Hint: it's the `code_server`.

Here are some potential outputs and what they might mean.

| Grouping | Meaning |
|---|---|
| `[[1, 2, 3, 4, 5]]` | Clean. |
| `[[1], [2, 3, 4, 5]]` | The **first run** is special. Warm-up or first-touch state. Usually means something is lazily-loaded. |
| `[[1, 3], [2], [4, 5]]` | A coin flip on every run. A live leak in the system. Something to debug. |

### Diagnosing the `code_server` problem

Modules are loaded lazily, by a single `code_server` process on the node. `dst_sched`
has scheduled a process to run. That process reaches a module the system hasn't loaded yet,
and sends a message to the `code_server` process, which isn't tracked by `dst_sched`.
Our process waits on a reply, which `dst_sched` picks up as a yeild, and an opportunity
for something else to be scheduled. On future runs, this specific yield doesn't exist,
so we end up with a different trace.

**`modules_loaded` tell you when this happens.** Start a *fresh* `iex` again.

```bash
MIX_ENV=test iex -S mix
```

```elixir
:dst_run.run(:abd_harness, %{seed: 1, max_ops: 20, max_steps: 20_000}).modules_loaded
```

This should show you the modules that were loaded during the run. This is an
indicator of nondeterminism, not because the sytem is nondeterministic, but because
it changes `dst_sched`'s scheduling choices.

### The fix

Add the `preload` option (which is what we had already done in our test file):

```elixir
opts = %{seed: 1, max_ops: 20, max_steps: 20_000, preload: [:abd]}
```

`preload` loads every module of the named applications before the run starts. It
also always loads the modules of `kernel`, `stdlib` and `dst`. If your code has
other modules, you will have to add them here.

**Prewarming is not good enough** A prewarming run will only execute a subset of
codepaths. Some other seed may enter a path with a new module call. Only you know
the full set of modules that's relevant to preload.

Now go back to **"does a seed name an execution?"** at the top of this step and
run the 5-trace grouping again, on yet another fresh VM.

RESULT: `1` and `[[1, 2, 3, 4, 5]]`.

### Add this assertion to our tests

Add to `test/abd_test.exs`:

```elixir
test "a seed names an execution" do
  for seed <- [1, 7, 13] do
    traces =
      for _ <- 1..5 do
        :dst_run.run(:abd_harness, Map.put(@opts, :seed, seed)).trace
      end

    assert length(Enum.uniq(traces)) == 1, "seed #{seed} produced divergent traces"
  end
end
```

Provide enough input seeds to have a high likeihood of covering all codepaths. This
test will start failing if you add code that calls a new module that is not preloaded.

## Step 8: fault injection

Before we purposefully introduce a bug, the workload needs to be able to express the
failure that makes the bug visible.

For example, your network can experience a failure
that causes one of the ABD writers to crash. Right now our system can't
express this failure mode, because the network is always reliable. Forcing the matter
is called fault injection. Defining the system faults is your responsibility. `dst` gives
you the entrypoint for making those faults deterministic.

### What our reliable network looks like in code

A client sends to all 3 replicas inside a **single scheduler step**:

```erlang
[abd_replica:write(Pid, Ref, self(), Ts, Val) || Pid <- Replicas],
```

All replicas will always get every write. Perfectly reliable network, and not realistic!

### The crashed writer

Fault injection is itself a deep topic. For this walkthrough, we'll model it as a
new operation on the client.

Add to `src/abd_client.erl`, and put `partial_write/4` in the export list:

```erlang
%% A writer that died partway through its write phase, having reached only
%% some of the replicas. It collects no acks and never completes, which is
%% what a crashed writer leaves behind: a value durable at fewer replicas
%% than a quorum. Nothing in the protocol will clean this up.
partial_write(Replicas, WriterId, Value, Targets) ->
    {{Seq, _}, _} = query_phase(Replicas),
    Ts = {Seq + 1, WriterId},
    Ref = make_ref(),
    [abd_replica:write(lists:nth(I, Replicas), Ref, self(), Ts, Value) || I <- Targets],
    Ts.
```

In `test/support/abd_harness.erl`, `generate/2` becomes:

```erlang
generate(#{next_op := N, replicas := Replicas}, Rand0) ->
    {Roll, Rand1} = rand:uniform_s(Rand0),
    case Roll of
        R when R < 0.15 ->
            {Target, Rand2} = rand:uniform_s(length(Replicas), Rand1),
            {{partial_write, N, {v, N}, [Target]}, Rand2};
        R when R < 0.40 ->
            {{write, N, {v, N}}, Rand1};
        _ ->
            {{read, N}, Rand1}
    end.
```

`op_number/1` gains a clause:

```erlang
op_number({partial_write, N, _Value, _Targets}) -> N;
```

and `run_op/3` gains one:

```erlang
run_op({partial_write, N, Value, Targets}, Tab, Replicas) ->
    Start = tick(Tab),
    Ts = abd_client:partial_write(Replicas, N, Value, Targets),
    ets:insert(Tab, {{partial_write, N}, Start, tick(Tab), Ts, Value});
```

Put it alongside the `write` and `read` clauses you already have. The stamps
still come from `tick/1`. (Note: that will change in Step 11)

The target replica is selected in `generate/2`, using the seed, and carried
in the operation, so that it is part of the recorded trace.

### All tests are still green

RESULT: still green over 40 seeds.

Even with network faults, a properly implemented ABD register still works.
That's the power of correct distributed systems, and not a function of
`dst`. We've increased our confidence that we've properly implemented the
ABD Quorum Register. Next, we'll introduce a bug on purpose and discover
it via `dst`.

## Step 9: delete the write-back

Remember that odd-looking write on the read path earlier? Let's pretend someone came
along and deleted that, thinking it was superfluous. You could just delete the line
of code, but for demo purposes, we'll make the bug a configurable property of our
app.

Let's introduce `Mode` to define the buggy behavior. In `src/abd_client.erl`:

```erlang
read(Replicas) ->
    read(Replicas, correct).

read(Replicas, Mode) ->
    {Ts, Val} = query_phase(Replicas),
    case Mode of
        correct ->
            ok = write_phase(Replicas, Ts, Val);
        no_writeback ->
            ok
    end,
    {Ts, Val}.
```

and add `read/2` added to the export list.

The rest of the changes are to thread `mode` from `Config` down to the call.
Four edits in `test/support/abd_harness.erl`.

`init/2`'s returned map gains a `mode` key:

```erlang
    {ok, #{tab => Tab, replicas => Replicas, clients => [], next_op => 1,
           mode => maps:get(mode, Config, correct)}}.
```

`execute/2` pattern-matches it and plumbs it into `run_op`.

```erlang
execute(Op, Sut = #{tab := Tab, replicas := Replicas, clients := Clients, mode := Mode}) ->
    N = op_number(Op),
    Client = dst_run:spawn_op(fun() -> run_op(Op, Tab, Replicas, Mode) end),
    Sut#{clients := [Client | Clients], next_op := N + 1}.
```

All three `run_op` clauses gain a fourth argument; only the read clause uses it:

```erlang
run_op({write, N, Value}, Tab, Replicas, _Mode) ->
    Start = tick(Tab),
    Ts = abd_client:write(Replicas, N, Value),
    ets:insert(Tab, {{write, N}, Start, tick(Tab), Ts, Value});
run_op({partial_write, N, Value, Targets}, Tab, Replicas, _Mode) ->
    Start = tick(Tab),
    Ts = abd_client:partial_write(Replicas, N, Value, Targets),
    ets:insert(Tab, {{partial_write, N}, Start, tick(Tab), Ts, Value});
run_op({read, N}, Tab, Replicas, Mode) ->
    Start = tick(Tab),
    {Ts, Val} = abd_client:read(Replicas, Mode),
    ets:insert(Tab, {{read, N}, Start, tick(Tab), Ts, Val}).
```

Start a new `iex`, and let's conduct a search:

```elixir
opts = %{max_ops: 20, max_steps: 20_000, preload: [:abd], config: %{mode: :no_writeback}}

{micros, results} =
  :timer.tc(fn ->
    for seed <- 1..200 do
      {seed, :dst_run.run(:abd_harness, Map.put(opts, :seed, seed)).outcome}
    end
  end)
```

RESULT: **200 seeds in ~1 sec**, **21 of 200 fail.** The first failure
is **seed 6**, which means it only took 30 msec to find the bug.

A 10% hit rate: It's frequent enough that the search feels instant,
and rare enough that it would be annoying to pinpoint by hand. That's why
we think DST is useful.

The violation:

```elixir
{:violation, %{
  property: :no_new_old_inversion,
  detail: "a later read reported an older value",
  earlier: {8, 11, {1, 1}},
  later: {12, 13, {0, 0}}
}}
```

Those tuples are `{start, finish, timestamp}`. `start` and `finish` define the
scheduler steps - they point back to items of the trace to aide in debugging.
The timestamps show the ordering violation - 1 ia earlier than 0.

## Step 10: We have the trace of the bug, now let's print it out

The trace can potentially be very long. One advantage of having a replayable
system is that we can conduct a search to find the shortest possible trace
that reproduces the same violation. (Note: global minimization is not guaranteed)

```elixir
opts6 = Map.put(opts, :seed, 6)
result = :dst_run.run(:abd_harness, opts6)
shrunk = :dst_shrink.shrink(:abd_harness, result.trace, opts6)
:dst_run.summary(shrunk)
```

**Use the same options.** `opts6` has `config: %{mode: :no_writeback}`,
and the shrinker starts a fresh system for every candidate.

RESULT: `original: 35, shrunk: 19, tests: 393, verified: true`, in 457 ms.

`verified: true` tells you that the search was successful. During the search, a
candidate is only a recipe, replayed in lenient mode where some steps  get skipped.
`verified: true` means `dst_shrink` took what that run actually
executed and replayed it again strictly, and it still failed. `false` means the
search found something smaller that doesn't reproduce, and you get the original
back.

## Step 11: make the failure readable

Here is the shrunk trace:

```erlang
[{op, {partial_write, 1, {v, 1}, [2]}},
 {op, {partial_write, 2, {v, 2}, [3]}},
 {op, {read, 3}},
 ...
 {step, 3}, {step, 1}, {step, 2}, {step, 3}, {step, 8}, {step, 1}, ...]
```

If that means nothing to you, that's the correct reaction.

Remember that we're still working with replayable `dst_sched` actions. Even in
this minimized state, the shrunk trace is an artifact of the `dst` library,
and it tells you little about the specifics of the ABD Register. Let's bridge
that gap.

### Using `dst_log`

`dst_log` uses a global table. It's API is modeled after `fprof` and other built-in
Erlang analysis tools. Don't execute these yet; we need to instrument the ABD
code first.

```erlang
dst_run:run(abd_harness, Opts),   %% 1. collect  (automatic)
dst_log:profile(),                %% 2. correlate
dst_log:analyze().                %% 3. present
```

### What you add to your system

We'll use `?DST_LABEL` and `?DST_LOG` in our system code. Both of these macros
expand to no-ops when `DST` is undefined. In production code, this instrumentation
simply does not exist.

**`src/abd_replica.erl`.** Label the process in `init/1`:

```erlang
init(Index) ->
    ok = ?DST_LABEL({replica, Index}),
    {ok, #st{index = Index}}.
```

and log what it does (with `?DST_LOG`), in the `handle_cast` clauses:

```erlang
handle_cast({read, Ref, From}, St = #st{index = I, ts = Ts, val = Val}) ->
    _ = ?DST_LOG({answered_read, Ts}),
    From ! {read_ack, Ref, I, Ts, Val},
    {noreply, St};
handle_cast({write, Ref, From, Ts, Val}, St = #st{index = I}) ->
    St1 = store(Ts, Val, St),
    _ = ?DST_LOG({applied_write, Ts, St1#st.ts =:= Ts}),
    From ! {write_ack, Ref, I},
    {noreply, St1}.
```

The boolean on `applied_write` is whether the write actually took. A replica
that already holds a higher timestamp ignores it, and knowing which is which
will help us troubleshoot.

**`src/abd_client.erl`**, instrument where the protocol decides something, still
using `?DST_LOG`. In `query_phase/1`, surrounding the collection:

```erlang
query_phase(Replicas) ->
    Ref = make_ref(),
    _ = ?DST_LOG(query_sent),
    [abd_replica:read(Pid, Ref, self()) || Pid <- Replicas],
    Best = collect_reads(Ref, ?QUORUM, {{0, 0}, undefined}),
    _ = ?DST_LOG({quorum_max, element(1, Best)}),
    Best.
```

in `write_phase/3`, before the sends:

```erlang
    _ = ?DST_LOG({write_sent, Ts, length(Replicas)}),
```

in `partial_write/4`, after the sends:

```erlang
    _ = ?DST_LOG({crashed_after_writing, Ts, Targets}),
```

and in `read/2`'s `no_writeback` branch, which is the one that names the bug:

```erlang
        no_writeback ->
            _ = ?DST_LOG({returned_without_writeback, Ts})
```

This instrumentation requires knowledge of the system under test, ABD in this case,
to be useful. `dst` can't do that for you. Choosing the right instrumentation means
the trace will be readable. Luckily, since traces are replayable, the instrumentation
can be decided upon post-fact. You will be able to hone your instrumentation over time
using real bugs to define the narrative.

### What you add to your harness

`test/support/abd_harness.erl` never ships, so it can go either way: include
`dst.hrl` and use the macros, or call `dst_log` directly. We call directly.

The reason: **The macros are Erlang**. A harness that calls the functions instead
could be written in Elixir, or another BEAM language. The system under test cannot,
because `dst_transform` is an Erlang parse transform and never reaches an Elixir
module. Your harness does not have special compile-time requirements, so write it
in Elixir if you want.

First, one function that decides what every client is called. Each one calls it
on itself, which we wire up in a moment:

```erlang
label_for({write, N, _Value}) -> {writer, N};
label_for({partial_write, N, _Value, _Targets}) -> {crasher, N};
label_for({read, N}) -> {reader, N}.
```

There is an optional `labels/1` callback on `dst_harness`, but we don't need it here
because each process names itself.

### Use one clock, and only one

Your harness has been stamping operations from its own `tick/1` counter since
step 5. Throw it away and stamp from `dst_log:log/1`, which returns its sequence
number. Four edits in `test/support/abd_harness.erl`.

**In `init/2`**, delete the counter row:

```erlang
    true = ets:insert(Tab, {clock, 0}),     %% delete this line
```

**Delete `tick/1` entirely:**

```erlang
tick(Tab) ->                               %% delete this function
    ets:update_counter(Tab, clock, 1).
```

**In each of the 3 `run_op/4` clauses**, name the process and replace both
`tick(Tab)` calls. Provided here in full for easy copying.

```erlang
run_op(Op, Tab, Replicas, Mode) ->
    ok = dst_log:label(label_for(Op)),
    Start = dst_log:log(started),
    run_op(Op, Tab, Replicas, Mode, Start).

run_op({write, N, Value}, Tab, Replicas, _Mode, Start) ->
    Ts = abd_client:write(Replicas, N, Value),
    ets:insert(Tab, {{write, N}, Start, dst_log:log({finished, Ts}), Ts, Value});
run_op({partial_write, N, Value, Targets}, Tab, Replicas, _Mode, Start) ->
    Ts = abd_client:partial_write(Replicas, N, Value, Targets),
    ets:insert(Tab, {{partial_write, N}, Start, dst_log:log({finished, Ts}), Ts, Value});
run_op({read, N}, Tab, Replicas, Mode, Start) ->
    {Ts, Val} = abd_client:read(Replicas, Mode),
    ets:insert(Tab, {{read, N}, Start, dst_log:log({finished, Ts}), Ts, Val}).
```

It can be helpful to confirm that the log does not influence trace replayability:

```elixir
a = :dst_run.run(:abd_harness, Map.put(opts, :seed, 6)).trace
b = :dst_run.run(:abd_harness, Map.put(opts, :seed, 6) |> Map.put(:log, false)).trace
a == b
```

`log => false` suppresses the *events*, but still allows the sequence numbers.

**Observability that changes the trace is not worth it**, because every
failure you then investigate is a different failure from the one you set out to
investigate.

### Read the failure

Four files have changed since step 10, so start a **fresh shell** and rebuild
`shrunk` from scratch.

```bash
MIX_ENV=test iex -S mix
```

```elixir
opts = %{max_ops: 20, max_steps: 20_000, preload: [:abd], config: %{mode: :no_writeback}}
opts6 = Map.put(opts, :seed, 6)

result = :dst_run.run(:abd_harness, opts6)
shrunk = :dst_shrink.shrink(:abd_harness, result.trace, opts6)
:dst_run.summary(shrunk)
```

Use whichever seed your own step 9 sweep turned up; 6 was ours.

**Don't skip this: replay the shrunk trace** Collection resets at the
start of every run, and `shrink/3` just did several hundred of them. Make
sure the system can collect the log events you care about.

```elixir
:dst_run.replay(:abd_harness, shrunk.trace, opts6)

{:violation, %{later: {_start, finish, _ts}}} = shrunk.outcome
:dst_log.analyze(%{until: finish})
```

RESULT:

```
    1  $dst           {op,{partial_write,1,{v,1},[2]}}
    2  $dst           {op,{partial_write,2,{v,2},[3]}}
    3  $dst           {op,{read,3}}
    4  $dst           {op,{read,4}}
    5  $dst           {op,{read,5}}
    6  crasher-1      {step,3}
    7  crasher-1      started
    8  crasher-1      query_sent
    9  replica-2      {step,1}
   10  replica-2      {answered_read,{0,0}}
   11  $dst           {op,{read,6}}
   12  replica-3      {step,2}
   13  replica-3      {answered_read,{0,0}}
   14  crasher-1      {step,3}
   15  crasher-1      {quorum_max,{0,0}}
   16  crasher-1      {crashed_after_writing,{1,1},[2]}
   17  crasher-1      {finished,{1,1}}
   18  reader-6       {step,8}
   19  reader-6       started
   20  reader-6       query_sent
   21  replica-2      {step,1}
   22  replica-2      {applied_write,{1,1},true}
   23  replica-2      {answered_read,{1,1}}
   24  $dst           {op,{read,7}}
   25  replica-3      {step,2}
   26  replica-3      {answered_read,{0,0}}
   27  reader-6       {step,8}
   28  reader-6       {quorum_max,{1,1}}
   29  reader-6       {returned_without_writeback,{1,1}}
   30  reader-6       {finished,{1,1}}
   31  reader-7       {step,9}
   32  reader-7       started
   33  reader-7       query_sent
   34  replica-3      {step,2}
   35  replica-3      {answered_read,{0,0}}
   36  replica-1      {step,0}
   37  replica-1      {answered_read,{0,0}}
   38  replica-1      {answered_read,{0,0}}
   39  replica-1      {answered_read,{0,0}}
   40  reader-7       {step,9}
   41  reader-7       {quorum_max,{0,0}}
   42  reader-7       {returned_without_writeback,{0,0}}
   43  reader-7       {finished,{0,0}}
```

### How to read 40 lines

Don't! The violation tells you which ones matter.

```
earlier: {19, 30, {1, 1}}
later:   {32, 43, {0, 0}}
```

Those are **line numbers**. Read 19 to 30 for how the earlier read saw `{1,1}`,
32 to 43 for how the later one missed it, and the `crashed_after_writing` line
above them for why there was anything to miss. Everything else is context you
can skip until you need it.

Still, to make sense of this rendering, you will need to have knowledge and
understanding of the ABD system itself. Don't be alarmed if the trace remains
confusing even at this stage. When you write your system, you will be the expert,
and as the expert you will be able to parse this information.

`dst_log:analyze(%{until: finish, driver: false})` drops the `$dst` lines
entirely if you want only what the system did.

### What it says

Line 16 is a writer that reached exactly 1 replica and died. Line 29 is a reader
that saw that value and returned it without making it durable. Line 42 is the
next reader, which didn't see it at all. Line 30 precedes line 32, so those 2
reads are sequential, and the register went backwards.

The counterintuitive part, and the reason this bug survives code review:
reader-6's quorum was `{replica-2, replica-3}` and reader-7's was
`{replica-3, replica-1}`. **They do intersect, at replica-3.** Quorum intersection worked exactly as advertised. But it didn't help, because `{1,1}` only ever existed at 1 replica.

Reader-6 got lucky enough to observe a value that was never durable, and by
returning it, published something the system could no longer produce. **The
write-back is what turns "I observed this" into "this is durable at a quorum"
before the value escapes.** With it, reader-6 writes `{1,1}` to 2 replicas
before returning, and reader-7's quorum of 2 cannot miss it.

One other item to note. There are entries in this trace that are a result of
old messages in the process mailboxes that eventually get picked up. They
exist because we were lazy about maintaining a clean mailbox in the implementation.

## (Optional) Step 12: confirm the bug and the patch in formal assertions

Optional step. This illustrates how the same seed can experience different codepaths
using different configuration options. Because we made the buggy behavior a configurable
mode, we can test the same seed under two different codepaths. One asserts the violation,
and one asserts system health.

```elixir
  @buggy %{max_ops: 20, max_steps: 20_000, preload: [:abd], config: %{mode: :no_writeback}}

  test "seed 6 reproduces the inversion without the write-back" do
    assert {:violation, %{property: :no_new_old_inversion}} =
             :dst_run.run(:abd_harness, Map.put(@buggy, :seed, 6)).outcome
  end

  test "the write-back fixes seed 6" do
    correct = Map.put(@buggy, :config, %{mode: :correct})
    assert :ok = :dst_run.run(:abd_harness, Map.put(correct, :seed, 6)).outcome
  end
```


## Step 13: a virtual clock

Every run so far has reported `clock_ms: 0`. The system has no timers, so
`dst_time` has never done anything and the parse transform we put on `abd_replica`
back at step 3 has never rewritten a line. However, time is a very typical
component of distributed systems, and usually shows up as timeouts.

`dst_transform` rewrites `receive-after` language constructs and `erlang:send_after`.
Both become deterministic with `dst_time`'s virtual clock.

### Another fault injection - a replica that dies

Here's another fault injection site. This time, we'll allow the replica to become
busy, preventing timely responses. This fault simulates high network latency.

In `src/abd_replica.erl`, add a `paused` field, a `pause/2` cast, and 3 clauses
**above** the existing `handle_cast`s:

```erlang
handle_cast({pause, Ms}, St) ->
    _ = ?DST_LOG({paused, Ms}),
    _ = erlang:send_after(Ms, self(), resume),
    {noreply, St#st{paused = true}};
%% An unreachable replica. The request arrives and is simply never answered,
%% which is what a caller sees from a node that has gone away.
handle_cast({read, _Ref, _From}, St = #st{paused = true}) ->
    {noreply, St};
handle_cast({write, _Ref, _From, _Ts, _Val}, St = #st{paused = true}) ->
    {noreply, St};
```

and a `handle_info` clause to bring it back:

```erlang
handle_info(resume, St) ->
    _ = ?DST_LOG(resumed),
    {noreply, St#st{paused = false}}.
```

That `erlang:send_after/3` is the first line in the project the transform
actually rewrites. If it feels nondeterministic to you, you're right, it should!
What you don't see is that `dst_transform` will rewrite things at compile time
(when `DST` is defined). That rewrite replaces the wall clock with a virtual one,
and removes the nondeterminism.

TODO - JMS - continue here

### A client that gives up

Add near the top of `src/abd_client.erl`:

```erlang
-define(PHASE_TIMEOUT, 5000).
```

and give both collection loops an `after` clause. The read side:

```erlang
collect_reads(_Ref, 0, Best) ->
    Best;
collect_reads(Ref, N, Best) ->
    receive
        {read_ack, Ref, _Index, Ts, Val} ->
            collect_reads(Ref, N - 1, best(Best, {Ts, Val}))
    after ?PHASE_TIMEOUT ->
        _ = ?DST_LOG(gave_up_waiting_for_read_quorum),
        throw({abd_timeout, Ref})
    end.
```

and `collect_writes/2`:

```erlang
collect_writes(_Ref, 0) ->
    ok;
collect_writes(Ref, N) ->
    receive
        {write_ack, Ref, _Index} ->
            collect_writes(Ref, N - 1)
    after ?PHASE_TIMEOUT ->
        _ = ?DST_LOG(gave_up_waiting_for_write_quorum),
        throw({abd_timeout, Ref})
    end.
```

Nothing else in either phase changes: no ref to arm, no timer to cancel, no
stale message to flush.

**Giving up has to mean failing, not proceeding with what you have.** A client
that returns a value backed by 1 replica instead of 2 has broken the quorum
property, and you'd have introduced a second bug while demonstrating the first.
That's why this throws rather than returning the partial result.

### The harness issues pauses and records give-ups

3 changes in `test/support/abd_harness.erl`. `generate/2` gains a pause operation,
and the other thresholds shift up to make room:

```erlang
generate(#{next_op := N, replicas := Replicas}, Rand0) ->
    {Roll, Rand1} = rand:uniform_s(Rand0),
    case Roll of
        R when R < 0.04 ->
            {Target, Rand2} = rand:uniform_s(length(Replicas), Rand1),
            {{pause, N, Target, 10000}, Rand2};
        R when R < 0.25 ->
            {Target, Rand2} = rand:uniform_s(length(Replicas), Rand1),
            {{partial_write, N, {v, N}, [Target]}, Rand2};
        R when R < 0.45 ->
            {{write, N, {v, N}}, Rand1};
        _ ->
            {{read, N}, Rand1}
    end.
```

4% is deliberate and the reasoning is below, under "why the tuning is
bimodal". Our first attempt used 10% and broke the workload.

`op_number/1` and `label_for/1` each gain a clause:

```erlang
op_number({pause, N, _Index, _Ms}) -> N;
```

```erlang
label_for({pause, N, _Index, _Ms}) -> {pauser, N};
```

And `run_op/5` gains a clause for the new operation. Naming and the start stamp
already happened in the `run_op/4` head, so this one only does the work:

```erlang
run_op({pause, _N, Index, Ms}, _Tab, Replicas, _Mode, _Start) ->
    _ = dst_log:log({pausing, Index, Ms}),
    abd_replica:pause(lists:nth(Index, Replicas), Ms);
```

### Timed-out operations are not completed operations

The other 3 `run_op/4` clauses each wrap their client call and record a failure
under a different key:

```erlang
run_op({write, N, Value}, Tab, Replicas, _Mode, Start) ->
    try abd_client:write(Replicas, N, Value) of
        Ts ->
            ets:insert(Tab, {{write, N}, Start, dst_log:log({finished, Ts}), Ts, Value})
    catch
        throw:{abd_timeout, _} ->
            ets:insert(Tab, {{gave_up, write, N}, Start, dst_log:log(gave_up)})
    end;
run_op({partial_write, N, Value, Targets}, Tab, Replicas, _Mode, Start) ->
    try abd_client:partial_write(Replicas, N, Value, Targets) of
        Ts ->
            ets:insert(Tab, {{partial_write, N}, Start, dst_log:log({finished, Ts}), Ts, Value})
    catch
        throw:{abd_timeout, _} ->
            ets:insert(Tab, {{gave_up, partial_write, N}, Start, dst_log:log(gave_up)})
    end;
run_op({read, N}, Tab, Replicas, Mode, Start) ->
    try abd_client:read(Replicas, Mode) of
        {Ts, Val} ->
            ets:insert(Tab, {{read, N}, Start, dst_log:log({finished, Ts}), Ts, Val})
    catch
        throw:{abd_timeout, _} ->
            ets:insert(Tab, {{gave_up, read, N}, Start, dst_log:log(gave_up)})
    end.
```

Put the `pause` clause above these.

`check/1` needs no change at all, and that's the interesting part.

`{gave_up, read, N}` doesn't match `check/1`'s `{{read, _}, S, F, Ts, _}`
pattern, so timed-out reads are excluded from the invariant. **They have to
be.** A read that never returned a value to anybody can't have gone backwards,
and counting one would manufacture violations that aren't real.

### Run it

```elixir
opts = %{max_ops: 20, max_steps: 20_000, preload: [:abd], config: %{mode: :correct}}

for seed <- 1..40 do
  r = :dst_run.run(:abd_harness, Map.put(opts, :seed, seed))
  {seed, r.outcome, r.clock_ms}
end
```

RESULT: every outcome `:ok`. `clock_ms` reads **10000 on 23 of the 40 seeds and
0 on the other 17.**

Outcomes staying `ok` is the check that matters. Pausing a replica must not
break a correct register, since ABD tolerates 1 failure and 2 paused just means
nobody makes progress.

**The split is the interesting part, and worth predicting before you run it.** A
pause is the only timer this system arms, so the clock moves only on seeds that
drew one, and at 4% over 20 operations most runs draw none. Measured across
those 40 seeds: 17 runs with no pause, 14 with one, 6 with two, 2 with three,
1 with four.

Where it does move it lands on exactly 10000, because every pause is issued
while the clock still reads 0 and lasts 10 seconds, and a run ends once no
timers remain. The final reading is the last resume, every time.

10 virtual seconds of a stalled cluster, inside a few milliseconds of wall
clock, because when nothing is runnable the driver jumps straight to the next
deadline instead of waiting.

### Check that the timeout branch actually runs

`clock_ms: 10000` only proves the *replica's* resume timer fired. It says
nothing about whether a client ever gave up, and a timeout branch that never
executes is exactly the kind of thing that looks tested and isn't.

```elixir
:dst_log.profile()
|> Enum.reject(&(&1.label == :"$dst"))
|> Enum.map(fn %{what: what} -> if is_tuple(what), do: elem(what, 0), else: what end)
|> Enum.frequencies()
```

`profile/0` hands back maps rather than tuples, and the `reject` drops
`dst_run`'s own `op`, `step` and `clock` entries so that you count only what
your system did.



RESULT, on seed 1:

```
answered_read: 34                     quorum_max: 13
applied_write: 22                     resumed: 4
finished: 7                           started: 20
gave_up: 9                            write_sent: 13
gave_up_waiting_for_read_quorum: 3
gave_up_waiting_for_write_quorum: 6
paused: 4
pausing: 4
query_sent: 16
```

Both timeout paths fire, so the branch is live rather than merely present.

**Seed 1 is not a typical run, and that makes it more useful than one.** It drew
4 pauses where the average is under one, so 9 of its 20 operations gave up and
the cluster was down more than it was up. Across all 40 seeds only 8 see a
give-up at all.

Now look at what's **missing** from that list: `crashed_after_writing` doesn't
appear. Not one partial write completed. The fault we added at step 8, the one
that makes the bug reachable, was entirely crowded out by the fault we added
here — and this is at 4%. Our first attempt used 10% and every run looked like
this one.

**An injected fault that's too frequent starves the workload of the scenarios
you're searching for.** A system that never completes an operation can't
violate a property about completed operations, and your suite stays green
because nothing interesting ever happens. [Page 5](05-gotchas.md) warns about
injecting faults the system can't see. This is the opposite failure and it's
just as quiet.

### Why the tuning is bimodal

Dropping the pause rate from 10% to the 4% above helps, and it doesn't smooth
anything out. Over
10 seeds the give-up counts came back as `0, 0, 0, 0, 0, 0` and `9, 9, 11, 12`.
There's no middle.

The cause is worth understanding because it's a property of simulated time
generally: **every operation is injected while the virtual clock still reads
0.** The clock only advances when nothing is runnable, and the driver is busy
injecting and stepping throughout, so the whole 20-operation workload happens
at t=0. A 10-second pause therefore spans the entire run. Either you draw 2
pauses on distinct replicas and the cluster is down for all of it, or you don't
and it's never down at all.

Lowering the rate makes jams rarer. It can't make them milder. Shortening the
pause wouldn't help either, because a paused replica *drops* requests rather
than queuing them, so a client that queried during a pause is doomed whenever
the pause ends.

### What it cost

The no-write-back sweep found **21 failures per 200 seeds** before this step
and **12 per 200** after, with the first at seed 53 rather than seed 6.

Fault injection isn't free, and the currency is search efficiency on the faults
you already had. Operations that time out never complete, and only completed
reads can violate regularity.

## Step 14: pin the trace, not the seed

Adding the pause operation changed `generate/2`, and `mix test` went red:

```
1) test seed 6 reproduces the inversion without the write-back (AbdTest)
   left:  {:violation, %{property: :no_new_old_inversion}}
   right: :ok
```

Nothing about the bug changed. Seed 6 now draws a different workload, so the
schedule it names is a different schedule.

**A seed-pinned regression test is coupled to your workload generator.** Touch
the generator and every pinned seed silently stops testing what it was pinned
for. Ours failed loudly because it asserts on a violation. A test asserting
`ok` would have gone vacuous without a word.

The fix is to pin the trace. `dst_run:replay/3` never calls `generate/2`, it
walks the entries it's given, so a saved trace survives generator changes
completely.

Capture it once, with `save_fixture/4`:

```elixir
buggy = %{max_ops: 20, max_steps: 20_000, preload: [:abd], config: %{mode: :no_writeback}}
opts53 = Map.put(buggy, :seed, 53)

r = :dst_run.run(:abd_harness, opts53)
s = :dst_shrink.shrink(:abd_harness, r.trace, opts53)
{s.original, s.shrunk, s.verified}

File.mkdir_p!("test/fixtures")
:dst_run.save_fixture("test/fixtures/inversion.dst", :abd_harness, s.trace, opts53)
```

RESULT: `{39, 19, true}`, then `{:ok, {:violation, %{property: :no_new_old_inversion}}}`.

**`save_fixture/4` replays the trace strictly before writing anything**, so a
fixture on disk is one that has been demonstrated to reproduce rather than one
you believed would. If it comes back `{:error, {:did_not_replay, _}}`, nothing
is written and you have learned something useful.

It saves the harness and the options alongside the trace, and the options matter
as much as the trace does. A reproduction that needs
`config: %{mode: :no_writeback}` and gets replayed without it does not
reproduce, and you will spend an afternoon on it. Carrying them means a test
cannot get that wrong:

```elixir
  test "the recorded inversion still reproduces" do
    assert %{outcome: {:violation, %{property: :no_new_old_inversion}}} =
             :dst_run.replay_fixture("test/fixtures/inversion.dst")
  end
```

That test names a file and nothing else. There is no seed, no config and no
harness module in it to drift out of sync.

Don't try to write the mirror of this by replaying the same trace against the
correct implementation. The write-back sends extra messages, so the recorded
step ids stop being runnable and you get `{error, {diverged, _, _}}` rather
than `ok`. "Correct mode is fine" is a claim about the *whole system*, and the
40-seed sweep is the right shape for it.

A fixture also ignores the seed entirely, which is the clearest statement of
what it buys you. Replay it under any seed and the failure is still there,
because nothing in a replay is drawn from one.

It does still break if the *shape* of an operation changes, or if `processes/1`
starts registering processes in a different order. Those are changes to the
contract rather than to the workload, and they should break it.

### What this still doesn't exercise

`settle_steps`. Our pause timers are one-shot, so the system still reaches true
quiescence and the run ends on its own. A system with a *periodic* timer, a
heartbeat, never reaches "nothing runnable and nothing pending", and its runs
can only end by exhausting the step budget unless you give it a settle phase.
If yours has one, read that section of [page 2](02-setting-up.md) before your
first run.

## Step 15: read state the invariant can't ask for

`dst_log` records what your system *did*. This is the other half: what it
currently *holds*. Different question, and you want both.

The problem it solves is specific. `check/1` runs against a frozen system, so it
cannot call into a replica to ask what it stores — every one of them is
suspended and always will be. That is why the invariant reads ETS, and why the
replicas' `{Ts, Val}` has been invisible to it all along.

### Publish it

One attribute on `src/abd_replica.erl`:

```erlang
-include_lib("dst/include/dst.hrl").
-dst_observe({st, [ts, val]}).
```

**`{st, ...}` and not a bare list**, because our record is `#st{}`. A bare
`-dst_observe([ts, val])` assumes a record named `state` and fails at compile
time with `{no_such_record, state}`, which is at least a loud way to find out.

Nothing else changes. The transform republishes those 2 fields into the
process dictionary on every `gen_server` callback return, so there is no
assignment site anybody can forget to update and staleness is impossible.

### Read it where it matters

In `test/support/abd_harness.erl`, take the replicas in `check/1` and attach
their state to a violation:

```erlang
check(#{tab := Tab, replicas := Replicas}) ->
    Reads = [{S, F, Ts} || {{read, _}, S, F, Ts, _} <- ets:tab2list(Tab)],
    case inversion(lists:sort(Reads)) of
        ok -> ok;
        {violation, Detail} -> {violation, Detail#{replicas => replica_states(Replicas)}}
    end.

%% What each replica holds, read straight out of its heap while it is suspended.
replica_states(Replicas) ->
    [{I, dst_observe:read(Pid)} || {I, Pid} <- lists:enumerate(Replicas)].
```

**On violation only, not on every check.** `read/1` copies whatever was
published, and `check/1` runs after every single action. Paying for that
thousands of times to enrich a report you will see once is the wrong trade, and
it is the reason `-dst_observe` takes a field list rather than `all`.

### Run it

```elixir
opts = %{max_ops: 20, max_steps: 20_000, preload: [:abd], config: %{mode: :no_writeback}}
:dst_run.run(:abd_harness, Map.put(opts, :seed, 53)).outcome
```

RESULT:

```elixir
{:violation,
 %{
   property: :no_new_old_inversion,
   detail: "a later read reported an older value",
   earlier: {18, 45, {1, 1}},
   later: {48, 93, {0, 0}},
   replicas: [
     {1, %{ts: {1, 1}, val: {:v, 1}}},
     {2, %{ts: {0, 0}, val: :undefined}},
     {3, %{ts: {0, 0}, val: :undefined}}
   ]
 }}
```

**The bug is in those 3 lines.** `{1,1}` exists at exactly one replica. One is
less than a quorum, so a read whose quorum misses that replica cannot see it,
and the write-back is what would have fixed that before the value ever escaped.
You can see the shape of the defect without reading the log at all.

### The rule, since you now have 2 ways to observe

They answer different questions and the choice comes up on every system.

**If the state already lives somewhere readable, read it there.** Our reads and
writes go into ETS because the invariant needs a history, and a history is the
wrong shape for `dst_observe` — `read/1` returns what was published *last*, not
a record of everything.

**If the state exists only inside a process, publish it.** A replica's
`{Ts, Val}` is a current value with no natural home outside the process, so it
publishes. On a suspended process, in a couple of microseconds, whatever the
mailbox depth.

Alongside `dst_log`: the log is the narrative, this is the snapshot. A failing
run wants both, and they cost about 10 lines between them.

---

---

## TODO from here

All 15 steps are drafted and walked end to end. Every library feature is now
exercised. What is left is framing.

- **Front matter**: prerequisites, what you end up with, a time estimate.
- **Steps 9 and 10 still carry `RESULT` numbers recorded before the pause fault
  existed.** Everything from step 11 on has been re-recorded from real runs;
  those 2 have not, and the seed they name (6) no longer fails now that
  `generate/2` draws pauses. Seed 53 is the first failing one on the final
  workload. Re-record both.
