# A Journey Through DST

> Working draft of the final walkthrough, and **the specification for rebuilding
> ABD**. There is no living copy of that project to fall back on: if a delta is
> missing here, the step cannot be reproduced, so treat any gap as a defect
> rather than untidiness.
>
> It has been updated for the library as it now stands — the `dst.hrl` include,
> `dst_harness`, `dst_log`, `preload`, `audit/1`, `save_fixture/4`, and
> `receive ... after` being handled by default — but **it has not been walked end
> to end since those changes.** `RESULT` blocks are real output from the
> original pass; re-record them, and expect the `dst_log` output in step 11 to
> differ in shape because the library interleaves the driver's own decisions.
>
> Links to `01-what-dst-is.md`, `02-setting-up.md`, `04-writing-a-system-under-test.md`
> and `05-gotchas.md` are dangling **on purpose**. They're relative links to
> sibling walkthrough pages and resolve once this file lands in `docs/`. Don't
> "fix" them here.

---

# Your first system under test

The rest of this manual explains `dst`. This page is the part where you use it.
We start with an empty directory and finish with a minimized, replayable
counterexample to a real distributed systems bug.

The system we'll build is an **ABD quorum register**: a single shared value,
replicated across 3 nodes, that stays correct while clients read and write it
concurrently. It's about 100 lines of Erlang. ABD is a good subject for this
because the bug we're going to find is one that people really make, the
invariant that catches it is a single line, and the counterexample needs a
genuine interleaving rather than an unlucky input.

You should have read [page 1](01-what-dst-is.md). Everything else you need is
explained here.

## What ABD is

3 replicas, each holding a `{Timestamp, Value}` pair. A quorum is 2 of them,
so any 2 quorums overlap in at least one replica. That overlap is the whole
mechanism.

- **Write(V)**: ask a quorum for their timestamps, take the highest, then
  write `{Highest + 1, V}` to a quorum.
- **Read()**: ask a quorum, take the pair with the highest timestamp, **write
  that pair back to a quorum**, then return the value.

The write-back in the read path is the part everyone leaves out. It looks
redundant. You're writing back a value you just read, and nothing has changed.
We'll write it correctly first, delete it later, and let `dst` explain why it
was there.

## Step 1: create the project

We'll put the project beside your `dst` checkout so the dependency can be a
plain relative path.

```bash
cd ~/dev && mix new abd
```

Mix is a comfortable build system for an Erlang project, and using it means
`dst`'s own examples transfer without translation, since `dst` is arranged the
same way. Nothing here needs Elixir, and you can add some later if you want it.

## Step 2: build configuration

Clear the Elixir scaffolding and make the directories we need:

```bash
cd abd && rm -rf lib test/abd_test.exs && mkdir -p src test/support
```

Then edit `mix.exs`. In `project/0`, add 3 keys:

```elixir
      erlc_paths: erlc_paths(Mix.env()),
      erlc_options: erlc_options(Mix.env()),
      elixirc_paths: [],
```

Replace the commented-out examples in `deps/0` with:

```elixir
      {:dst, path: "../dst", runtime: false}
```

and add 2 private function pairs at the bottom:

```elixir
  # The register itself is ordinary Erlang in src/. The dst harness lives in
  # test/support and only exists for the test env, so a release never sees it.
  defp erlc_paths(:test), do: ["src", "test/support"]
  defp erlc_paths(_), do: ["src"]

  # DST is what the parse transform hides behind.
  defp erlc_options(:test), do: [:debug_info, {:d, :DST}]
  defp erlc_options(_), do: [:debug_info]
```

Leave `application/0` and the generated `elixir:` version alone. Then:

```bash
mix deps.get && MIX_ENV=test mix compile
```

`src/` is empty, so anything that fails here is the configuration rather than
your code. That's why we run it now.

### The 3 decisions in that file

**`elixirc_paths: []`.** There's no Elixir source to compile. Test files stay
`.exs` because ExUnit loads them at runtime rather than compiling them through
`elixirc_paths`, so you keep ExUnit without keeping an Elixir codebase.

**`runtime: false` rather than `only: :test`.** `-include_lib` is resolved by
the preprocessor before any `-ifdef` inside the header is considered, so the
header has to be findable in *every* environment — a `only: :test` dependency
fails a plain `mix compile` with `can't find include lib`. `runtime: false`
makes `dst` available to the compiler everywhere and keeps it out of your
release: it isn't added to your application's `applications` list, so
`mix release` doesn't ship it.

**`{:d, :DST}` for the whole test env, not a dedicated simulation profile.**
`dst_time` falls back to the real `erlang` functions whenever no virtual clock
is running, so a transformed module behaves normally outside a simulation.
Compiling your ordinary test suite with the transform on means every test you
write is continuously demonstrating that inertness. A separate profile only
demonstrates it for the tests you remember to run under it.

**No `mod:` in `application/0`.** ABD is small enough not to need one, and this
keeps the walkthrough short.

**This is not a rule that a simulated system can't be an OTP application.** It
can, and yours probably should be. The rule is narrower: *the simulation* has to
start the processes it schedules, because `init/2` must hand the driver a system
that is **idle**, and a supervision tree started by the application is already
running before your harness gets control. How you start the same processes in
production is entirely your business — a harness that builds its own cluster and
a release that boots one under a supervisor are both fine, and most real
adoptions have both.

For a project that doesn't have `dst` checked out beside it, the dep is:

```elixir
{:dst, git: "https://github.com/jessestimpson/dst.git", runtime: false}
```

## Step 3: the replica

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

Drive it by hand:

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

**Casts, not `gen_server:call`.** This is the load-bearing decision of the
whole example. 3 sequential calls would collect all 3 answers in a fixed order
every time. Casts let a client take the first 2 that arrive, which makes
*quorum composition* a scheduling choice. That's precisely what puts our
counterexample in reach of `dst_sched` without injecting a single fault.

**Timestamps are `{Seq, WriterId}`, not integers.** 2 concurrent writers can
read the same maximum and both pick `Seq + 1`. Without a tiebreak they'd leave
different values at the same timestamp, which is a second, unrelated bug that
would muddy the one we're hunting. 1 extra tuple element buys a total order.

**`get/1` is a trap, and we've labeled it.** It's genuinely useful for poking
at a replica in `iex`, and it's exactly the shape of call that silently
switches an invariant off (see [page 4](04-writing-a-system-under-test.md)).
Having it present with a warning attached teaches better than a rule in the
abstract. When we write `check/1`, notice that we don't use it.

### One include, and it does nothing yet

`-include_lib("dst/include/dst.hrl")` is the entire opt-in. Under a simulation
build it brings the parse transform and the `?DST_LOG` and `?DST_ROLE` macros;
under a release build it brings nothing at all, so this module has no
relationship to `dst` and could ship exactly as written.

Right now it does nothing either way, because the replica has no timers, no
spawns and no logging. That is the inertness claim from step 2, demonstrated
rather than asserted, and it costs one line to have it in place before you need
it.

Worth confirming while it is cheap: a parse transform arriving through `deps/`
is found by `erlc` with no extra configuration.

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

**There's nothing framework-shaped in this file.** No `dst_` call, no harness
hook, nothing conditional on being under simulation. That's the pitch. The
thing you test is your real client, not a simulation-only copy of it. The
transform attribute is there for later and currently rewrites nothing.

**`abd_client` is protocol code, not a process.** It runs inside whatever
process calls it, which right now is your `iex` shell. In a moment the harness
will spawn a process per operation and run these same functions there. Keeping
the 2 things separate is what lets a client be a schedulable participant in the
concurrency without the protocol knowing anything about it.

**`make_ref()` is fine here, and that isn't obvious.** Refs differ between
runs, and unseeded identity is a genuine source of nondeterminism. It's
harmless in this case because the ref never decides anything. It only matches a
client's own replies. The rule is that refs and pids break replay when they
influence *control flow*, and matching your own mailbox isn't that.

**Watch what happens to the 3rd ack.** A quorum is 2, so after `collect_reads`
returns there's still a `read_ack` sitting in the mailbox, and the next phase's
`receive` scans straight past it on a different ref. That's what real ABD code
looks like, and it exercises the trickiest part of `dst_sched`: a process
blocked in a selective receive *with a non-empty mailbox*, which is the case
the scheduler's blocked-at-queue-length tracking exists for.

**And the read does a write-back.** Right now it looks redundant, which is the
point. We'll delete it later and it'll take 3 lines.

## Step 5: the harness

Everything so far has been the register. This is the harness: the 6 callbacks
`dst_run` drives it through, and the only file that names `dst` at runtime. The
register modules reference it too, through the header and the macros, but
nothing in them survives a release build.

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
mailbox, so this system is quiescent the moment it's built. That's free here
and it isn't free in general. A real cluster is *ready* long before it's
*idle*, and the difference is unanswered messages sitting in mailboxes when the
scheduler takes over. [Page 4](04-writing-a-system-under-test.md) covers what
that costs and how to wait for it properly.

The table is `public` rather than `protected` because 3 different kinds of
process touch it: client processes write results, the invariant reads them, and
`check/1` runs in a fresh process of its own each time.

It's an `ordered_set`, which is a determinism decision rather than a
performance one. `ets:tab2list/1` on a `set` returns rows in whatever order the
hash table happens to hold them. That's stable enough in practice to pass a
replay check and fragile enough to break when the table grows or the OTP
version changes. Term order costs nothing here and can't surprise us. We sort
in `check/1` as well, which is belt and braces.

### `processes/1` and why the `lists:reverse` is load-bearing

`dst_sched` assigns ids in registration order, and the trace records ids, so
the order this callback returns is part of the contract. We prepend new clients
to the list because that's cheap, which means the list itself is in reverse
chronological order, so we reverse it here to hand them over oldest first.

With one new client per operation you could get away without this, since only
one pid is ever unknown at a time. It's still wrong to rely on that. The moment
an operation creates 2 processes, registration order starts depending on list
order, and the symptom is a trace difference at a very early index that reads
exactly like a scheduling bug.

### `generate/2` draws from the state it's handed

40% writes, 60% reads, drawn from the `rand` state `dst_run` passes in and
returning the advanced state. Drawing from anywhere else, `rand:uniform/1` or
the clock or `erlang:unique_integer/0`, breaks replay silently.

Both operations carry their own number, and the value written is derived from
it. That's what makes an operation mean the same thing on a replay against a
freshly started system. An operation carrying a pid, a ref or a generated name
would not.

#### Why generating and executing are 2 callbacks

Because replay never calls `generate/2`. It runs only when `dst_run` is
generating a run. On a replay the operation comes out of the trace instead, and
goes straight to `execute/2`.

That split is what makes an operation a **value** rather than a position in an
RNG stream, and 3 things depend on it.

Shrinking would be impossible otherwise. `dst_shrink` works by deleting entries
from the trace, and deleting `{op, {read, 7}}` is meaningful only because the
surviving operations still mean what they meant. If a trace recorded "the
generator ran here", removing one entry would shift every later draw and
produce a different workload rather than a smaller version of the same one.

The trace would also be unreadable, which you'll care about the moment you have
a counterexample in front of you. Operations that are terms can be read
straight off the trace. A recorded RNG position can't.

And because operations are ordinary terms, you can write a trace by hand and
feed it to `replay/3` to test a scenario you already have in mind.

The split also localizes the entropy requirement to exactly one function.
`generate/2` is the only place randomness may enter a workload, which is why
"draw from the `rand` state you're handed" is a rule about `generate/2` and
about nothing else. `execute/2` is effectful, but deterministic given its
operation.

It's the same generator/runner split property-based testing libraries use, for
the same reason. Shrinking needs values, not seeds.

### `execute/2` spawns, and it has to

Every process the scheduler owns is suspended between steps, so a synchronous
call from the driver into the system can never be answered. Operations are
issued by spawning a process to carry them out, and that process is picked up
by the next `processes/1` call and interleaved like everything else.

That isn't a workaround. A client *is* part of the concurrent system, and this
is what makes it schedulable. It's also why `abd_client` was written as plain
functions rather than as a process: the harness decides where the protocol
runs.

The spawn goes through `dst_run:spawn_op/1` rather than `spawn/1`. The driver
isn't traced, so a process it creates isn't owned by the scheduler until the
driver registers it, and in that window the process runs on the real scheduler.
2 clients created by operations injected close together would race each other
to the replicas' mailboxes, and the winner would be decided by wall clock.
`spawn_op/1` parks the process until registration is done.

### `check/1` is where the real work is

Our invariant is regularity, sometimes called *no new-old inversion*: once a
read has returned a value, no read that starts later may return an older one.

Making that precise needs a notion of "later", and the logical clock in the
table provides it. Every operation stamps a start and a finish from the same
`ets:update_counter`. Under the scheduler exactly one process runs at a time,
so that counter is a faithful total order of the events, and it's completely
deterministic. 2 reads are *sequential* when one's finish precedes the other's
start, and that's the only case where the property has to hold. Reads that
overlap in time are allowed to disagree, because they're concurrent, and
nothing about a quorum register promises otherwise.

The check itself is a pairwise scan, which is O(n²) in completed reads and gets
evaluated after every action. At the scale we're running that's nothing. If a
workload ever makes it hurt, `check_every` is the lever.

Notice what `check/1` does *not* do. It never calls `abd_replica:get/1`, even
though that function exists and would be the obvious way to look at the system.
Under the scheduler every replica is suspended, so that call would hang, and
`dst_run` would report `check_blocked` after bounding it. The quieter and worse
version of the same mistake is an API that catches its own timeout and returns
something plausible, which leaves the invariant computing over nothing and
passing. Invariants read state out of band, and here that means ETS.

### The harness does not include the header

`abd_harness` has no `-include_lib`, and that distinction is worth holding on
to: **a harness is not the system under test.** Your replica and your client are
that. The harness starts them, declares which processes to schedule, drives them
with a workload and judges them.

Only the system ships, so only the system needs its instrumentation to
disappear in a release build. The harness never leaves your test directory, so
it calls `dst_log` directly and skips the macros. It also has no business being
transformed: it runs in the driver process, which the scheduler doesn't own, so
rewriting its spawns and timers would point them at machinery that isn't
managing it.

## Step 6: the first run

Everything is in place. A run is one call.

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

`summary/1` is the result with the trace swapped for its length. Use it rather
than printing a result whole: a run of 20 operations produces a few hundred
trace entries, so your shell renders the schedule and elides the fields you
wanted underneath it. The trace is for later, once you have a failure worth
shrinking.

**If `steps` comes back suspiciously close to `ops`** — say 20 steps for 20
operations, with nothing exited — don't puzzle over it. Check `modules_loaded`
and skip ahead to step 7. You have hit the code-server problem early, and it can
end a run at what looks exactly like quiescence.

Look at 6 fields before anything else.

**`outcome`** is `:ok`, `{:violation, Detail}`, or `{:error, Reason}`. We expect
`:ok`, and we want to see it before we go looking for bugs. A run that passes
proves the harness is wired up. If you turn a defect on first, you can't tell a
real violation from a broken `check/1`.

**`ops`** should be 20. If it's lower the run ended early, which for a passing
run means it hit quiescence with operations left over.

**`clock_ms`** is 0, because our system has no timers at all. Nothing ever
armed one, so the virtual clock never had a reason to move. That's worth
noticing now so that it's obviously different later.

**`modules_loaded`** must be empty, and **`sched.adopted_late`** and
**`sched.timeouts`** must be 0. Rather than remembering that list, ask:

```elixir
:dst_run.audit(result)
```

`:ok` means the seed means something. `{:suspect, reasons}` names what got in.
It's the assertion to put next to your invariants, and it keeps checking as more
counters get added.

`adopted_late` counts processes that ran before the
scheduler owned them, and every one of them is a piece of the interleaving
decided by wall clock rather than by the seed. A nonzero count here means the
seed won't reproduce, and it's the first thing to check when one doesn't.

**`skipped`** is 0. It only ever becomes nonzero during a lenient replay, which
is a shrinking thing. On a generated run it's a sanity check.

And `trace` is the whole run: every `{step, Id}`, `{op, Op}` and `{clock, Ms}`
in order.

2 things about reading one from Elixir. The entries are 2-tuples with atom
heads, so Elixir prints them in keyword-list style: `{:step, 3}` shows up as
`step: 3`. And the ids are positional. 0 through 2 are the replicas, registered
by `init/2` in the order `processes/1` returned them, and everything from 3 up
is a client, one per operation, numbered in injection order. So this:

```
op: {:read, 1},
step: 3,
```

is read 1 being injected, then its client taking its first step.

You can also watch the driver's 3-way choice in the trace. 2 `op:` entries back
to back are operations injected with no step in between, and a long run of
`step:` entries is the system being left alone to make progress.

### Now make it a test

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

`async: false` is not optional. `dst_time` keeps its state in named ETS tables,
so exactly one virtual clock exists per node, and 2 simulations running
concurrently will corrupt each other. Every test that drives `dst_run` has to
be serial.

Asserting on `adopted_late` in the same loop is a habit worth forming. It costs
one line and it turns a silent loss of determinism into a failing test.

### The run ended on its own

Notice we never touched `settle_steps`. This system has no periodic timers, so
after the last client exits there's nothing runnable and nothing pending, and
`dst_run` recognizes that as a finished run.

Not every system does that. Anything with a heartbeat never reaches "nothing
runnable and nothing pending", so a run can only end by exhausting its step
budget, which gets reported as an error and makes every run look like a
failure. That's what `settle_steps` is for, and if you add a timer to this
system later you'll need it.

## Step 7: does a seed name an execution?

This is the gate. Nothing downstream means anything until it passes, because a
seed that doesn't reproduce makes the shrinker useless, a counterexample
unshareable, and a regression test a coin flip.

Start a **fresh** VM and make this the first thing you run. The first run in a
new VM is a special case and we want it in the sample.

```elixir
traces = for _ <- 1..5, do: :dst_run.run(:abd_harness, %{seed: 1, max_ops: 20, max_steps: 20_000}).trace

length(Enum.uniq(traces))

traces
|> Enum.with_index(1)
|> Enum.group_by(fn {t, _i} -> t end, fn {_t, i} -> i end)
|> Map.values()
```

The count is the assertion. **1** means the seed names an execution.

The grouping is the diagnostic, and it's why we group rather than count.
*Which* runs differ tells you far more than how many distinct traces came out:

| Grouping | Meaning |
|---|---|
| `[[1, 2, 3, 4, 5]]` | Clean. |
| `[[1], [2, 3, 4, 5]]` | The **first run** is special. Warm-up or first-touch state, nearly always on-demand module loading. |
| `[[1, 3], [2], [4, 5]]` | A coin flip on every run. A live leak in the system. |

RESULT: `2` and `[[1], [2, 3, 4, 5]]`.

### Diagnosing it

We got the middle row, on a 100-line system, inside the first hour.

Modules are loaded lazily, by a single `code_server` process on the node. A
scheduled process that reaches a module it hasn't touched yet therefore sends a
message to a process the scheduler doesn't own, and waits for the reply. That
reply arrives when wall clock says so, not when the schedule says so, and every
choice after it shifts. On the second run everything is loaded and it never
happens again.

**`modules_loaded` names the culprit.** Start a *fresh* VM again — the one from
the gate above has already loaded everything, so it will tell you nothing:

```bash
MIX_ENV=test iex -S mix
```

```elixir
:dst_run.run(:abd_harness, %{seed: 1, max_ops: 20, max_steps: 20_000}).modules_loaded
```

The rule that usually identifies the culprit without any of this: **the module
that bites is the one only reachable from a scheduled process.** `abd_harness`
and `abd_replica` are both reached from `init/2`, which runs in the driver
process before the scheduler exists, so they load early. `abd_client` is first
called from *inside* a spawned client, mid-run.

The rest of what this failure looks like in the wild is worth knowing, because
none of it points anywhere: `adopted_late` was 0, no scheduler warning fired,
nothing was logged, and the correct-mode suite was green throughout. Not every
determinism leak has a counter aimed at it, which is why the grouping
diagnostic above still earns its place. [Page 5](05-gotchas.md) has the longer
version, including a worse failure this can cause.

### The fix

One option:

```elixir
opts = %{seed: 1, max_ops: 20, max_steps: 20_000, preload: [:abd]}
```

`preload` loads every module of the named applications before the run starts,
while there is still no scheduler to be outside of. `kernel`, `stdlib` and `dst`
are always included, so naming your own application is all there is to it.

Put it in the options map you pass everywhere from here on.

**Running a seed twice is not a substitute**, and this is the part worth
remembering. Warming is per *code path*, not per VM: a seed that first reaches a
timeout handler runs a branch no earlier seed touched and loads code no earlier
seed needed. Warming up fixes the seed you warmed and leaves the next one
exposed.

Now go back to **"does a seed name an execution?"** at the top of this step and
run the 5-trace grouping again, on yet another fresh VM.

RESULT: `1` and `[[1, 2, 3, 4, 5]]`.

`init/2` is called by the driver rather than by you beforehand for the same
family of reasons. Its contract isn't "start the system", it's "hand over a
system that won't do anything the schedule didn't ask for".

### Make it a test

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

Run it early, run it on several seeds, and run it again whenever you touch
anything.

## Step 8: teach the model to crash a writer

Before we introduce the bug, the workload needs to be able to express the
failure that makes the bug visible. This step is here because getting it wrong
costs you a fruitless search, and the reasoning generalizes past ABD.

### Why scheduling alone isn't enough

The natural expectation is that a partially-applied write is just a transient
the scheduler can freeze. It isn't, and the reason is worth following.

A client sends to all 3 replicas inside a **single scheduler step**:

```erlang
[abd_replica:write(Pid, Ref, self(), Ts, Val) || Pid <- Replicas],
```

A step runs until the process blocks, so all 3 messages are queued before
anything else runs. A replica that hasn't applied the write also hasn't been
stepped, so it hasn't answered any read either. Mailboxes are FIFO, so when the
scheduler does step it, the `gen_server` drains the write and *then* answers
the read.

Follow that through and the register is effectively linearizable in this model.
Any read sent after a write's send-step sees that write at every replica that
answers it, so a later read's quorum has seen a superset of an earlier read's.
No inversion is reachable, at any seed.

Sending to every replica in one step models a network that delivers everywhere
simultaneously, which is stronger than any real network. **A simulation is only
as faithful as the concurrency it models, and a fault your model can't express
is a bug your search can't find.** [Page 5](05-gotchas.md) warns against
injecting faults the system could never see. This is the mirror image, and it
fails silently rather than loudly: you burn seeds and conclude the code is
fine.

### The crashed writer

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
still come from `tick/1`; that changes at step 11.

The target replica is drawn in `generate/2` and carried in the operation, so
it's part of the recorded trace and replays identically. A crashed writer that
picked its victim at execution time would be a fault schedule that drifts
between runs, which is its own section on [page 5](05-gotchas.md).

### The sweep should still be green

RESULT: still green over 40 seeds.

That's the point of doing this step separately. **A crashed writer is harmless
under correct ABD.** Reader A queries a quorum, finds the orphaned value at the
1 replica that received it, and writes it back to a quorum before returning.
Reader B's quorum of 2 intersects A's write-back quorum of 2, so B can't miss
it.

That's the write-back's whole job, stated as a test. It still looks like belt
and braces. Next step we delete it, change nothing else, and the same workload
starts failing.

## Step 9: delete the write-back

One variable changes. In `src/abd_client.erl`:

```erlang
read(Replicas) ->
    read(Replicas, correct).

read(Replicas, Mode) ->
    {Ts, Val} = query_phase(Replicas),
    case Mode of
        correct -> ok = write_phase(Replicas, Ts, Val);
        no_writeback -> ok
    end,
    {Ts, Val}.
```

with `read/2` added to the export list. **That's the whole bug**: one `case` in
one function, and the deleted branch is the one that looked redundant.

The rest is threading `mode` from `Config` down to the call. Four small edits in
`test/support/abd_harness.erl`.

`init/2`'s returned map gains a key:

```erlang
    {ok, #{tab => Tab, replicas => Replicas, clients => [], next_op => 1,
           mode => maps:get(mode, Config, correct)}}.
```

`execute/2` picks it up and passes it on:

```erlang
execute(Op, Sut = #{tab := Tab, replicas := Replicas, clients := Clients, mode := Mode}) ->
    N = op_number(Op),
    Client = dst_run:spawn_op(fun() -> run_op(Op, Tab, Replicas, Mode) end),
    Sut#{clients := [Client | Clients], next_op := N + 1}.
```

and all three `run_op` clauses gain a fourth argument, which only the read
clause uses:

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

Nothing in `check/1` changes. The invariant never knew which mode it was
checking, which is the point of it being a property of the system rather than
of the implementation.

Then search:

```elixir
opts = %{max_ops: 20, max_steps: 20_000, preload: [:abd], config: %{mode: :no_writeback}}

{micros, results} =
  :timer.tc(fn ->
    for seed <- 1..200 do
      {seed, :dst_run.run(:abd_harness, Map.put(opts, :seed, seed)).outcome}
    end
  end)
```

RESULT: **200 seeds in 949 ms**, ~4.7 ms per run. **21 of 200 fail.** The first
is **seed 6**, so about 30 milliseconds of searching to a counterexample.

A 10% hit rate is worth pausing on. It's frequent enough that the search feels
instant, and rare enough that it would be annoying to pinpoint by hand. That band is
where this technique earns its keep.

The violation:

```elixir
{:violation, %{
  property: :no_new_old_inversion,
  detail: "a later read reported an older value",
  earlier: {8, 11, {1, 1}},
  later: {12, 13, {0, 0}}
}}
```

Read those as `{start, finish, timestamp}`. The earlier read finished at 11,
the later one started at 12, so they're genuinely sequential with no overlap.
And `{0,0}` is the *initial* timestamp: the second read didn't just go stale,
it reported the register as never written, having watched the previous read
return a value.

## Step 10: shrink it

```elixir
opts6 = Map.put(opts, :seed, 6)
result = :dst_run.run(:abd_harness, opts6)
shrunk = :dst_shrink.shrink(:abd_harness, result.trace, opts6)
:dst_run.summary(shrunk)
```

**Pass the same options.** `opts6` carries `config: %{mode: :no_writeback}`,
and the shrinker starts a fresh system for every candidate. Hand it the
defaults and it builds a *correct* register, gets a clean run on the first
candidate, and reports that there's nothing to shrink for a failure that just
happened.

RESULT: `original: 35, shrunk: 19, tests: 393, verified: true`, in 457 ms.

`verified: true` is the field that decides whether the other 3 mean anything. A
shrunk candidate is only a recipe, replayed in lenient mode where steps that no
longer apply get skipped. `true` means `dst_shrink` took what that run actually
executed and replayed it again strictly, and it still failed. `false` means the
search found something smaller that doesn't reproduce, and you get the original
back.

### What the shrinker couldn't remove

The 19 surviving entries include 7 operations, and **4 of them never run at
all.** No `{step, Id}` entry ever names their clients.

That's the positional-id bound. `dst_sched` assigns ids in registration order,
so deleting an operation renumbers every process created after it, the
surviving `{step, Id}` entries stop naming what they named, the candidate stops
failing, and ddmin reverts the removal as unsafe.

So read the result honestly: it's a genuine, verified reproduction, and its
*minimality* is approximate. The real story is 3 operations, and the next step
is what makes that visible.

## Step 11: make the failure readable

Here is the shrunk trace:

```erlang
[{op, {partial_write, 1, {v, 1}, [2]}},
 {op, {partial_write, 2, {v, 2}, [3]}},
 {op, {read, 3}},
 ...
 {step, 3}, {step, 1}, {step, 2}, {step, 3}, {step, 8}, {step, 1}, ...]
```

If that means nothing to you, that's the correct reaction, and it's the most
important thing on this page.

**A trace is a scheduler artifact, not an explanation.** `{step, 8}` records
which process the scheduler chose. It says nothing about what that process did,
what it read, what it sent, or what state anything was in, and the ids are
anonymous positions assigned by registration order. Turning it into an
explanation means reconstructing every mailbox state by hand.

The schedule tells you what the *scheduler* did. Nothing in `dst` tells you
what your *system* did, and you need both on one timeline.

### `dst_log`, which the library ships for exactly this

> **Note for the rebuild.** The `RESULT` blocks in this step came from a
> hand-built version of `dst_log` that predates the library one, so the shape is
> right but the exact lines will differ — the library interleaves `$dst` step
> entries that the hand-built version never had. Re-record them.

3 phases, deliberately shaped like `fprof`. Don't try to run them yet: our ABD
system doesn't record any events worth reading until we add the calls below.

```erlang
dst_run:run(abd_harness, Opts),   %% 1. collect  (automatic)
dst_log:profile(),                %% 2. correlate
dst_log:analyze().                %% 3. present
```

Collection is on by default. The middle phase is not ceremony: process names
**cannot** be resolved during a run, because ids are handed out as processes
register and only your harness can name them. So the raw log stays dumb, no
callbacks on the hot path, and correlation happens afterwards.

### What you add to your system

Two files, seven lines between them. Both already include `dst.hrl` from step 3,
so the macros are available.

**`src/abd_replica.erl`.** Name the process in `init/1`:

```erlang
init(Index) ->
    ok = ?DST_ROLE({replica, Index}),
    {ok, #st{index = Index}}.
```

and log what it answers, in both `handle_cast` clauses:

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
that already holds a higher timestamp ignores it, and knowing which is which is
the difference between reading a failure and guessing at it.

**`src/abd_client.erl`**, at the 4 points where the protocol decides something.
In `query_phase/1`, around the collection:

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

in `partial_write/4`, after them:

```erlang
    _ = ?DST_LOG({crashed_after_writing, Ts, Targets}),
```

and in `read/2`'s `no_writeback` branch, which is the one that names the bug:

```erlang
        no_writeback -> ?DST_LOG({returned_without_writeback, Ts})
```

Seven lines in total. They are the difference between a failure you can read and
one you have to reconstruct, and you choose where they go — nothing here writes
your narrative for you. Shortly we'll see how that narrative renders alongside
a trace.

### Macros, not `dst_log` directly

Under a release build `?DST_LOG` expands to nothing at all: no call, no ETS
lookup, nothing to pay for and nothing to carry. `dst_log:log/1` is inert at
runtime when no collection is running, but inert still costs a function call and
a table lookup on every event, and it means your release has to ship `dst` to
satisfy the call.

The macros are what let instrumentation stay in the source of something that
ships. Taking it out again is how a system ends up unreadable when it fails.

The argument is **not evaluated** in a release build, so a logged term must be
free of side effects, and a variable used only inside a `?DST_LOG` will be
reported unused.

Your harness is the exception, and the next section is about it.

### What you add to your harness

`test/support/abd_harness.erl` never ships, so it can go either way: include
`dst.hrl` and use the macros, or call `dst_log` directly. We call directly.

The reason is worth knowing even if you don't want it here. **The macros are
Erlang**, so a harness that calls the functions can be written in Elixir
instead — which the rest of a simulated system cannot, because `dst_transform`
is an Erlang parse transform and never reaches an Elixir module. Your harness is
the one part with that freedom. (You'd want a non-empty `elixirc_paths` for it,
which step 2 deliberately emptied.)

First, one function that decides what every process is called, so that the
harness and the processes themselves cannot disagree:

```erlang
role_for({write, N, _Value}) -> {writer, N};
role_for({partial_write, N, _Value, _Targets}) -> {crasher, N};
role_for({read, N}) -> {reader, N}.
```

`execute/2` remembers it, in a new `roles` map beside `clients`:

```erlang
execute(Op, Sut = #{tab := Tab, replicas := Replicas, clients := Clients,
                    roles := Roles, mode := Mode}) ->
    N = op_number(Op),
    Client = dst_run:spawn_op(fun() -> run_op(Op, Tab, Replicas, Mode) end),
    Sut#{
        clients := [Client | Clients],
        roles := Roles#{Client => role_for(Op)},
        next_op := N + 1
    }.
```

with `roles => #{}` added to `init/2`'s returned map. Then the optional 7th
callback is a merge:

```erlang
labels(#{replicas := Replicas, roles := Roles}) ->
    maps:merge(
        maps:from_list([{Pid, {replica, I}} || {I, Pid} <- lists:enumerate(Replicas)]),
        Roles
    ).
```

Add `labels/1` to the export list.

**One source, and that is the point.** A process names itself with `role/1` and
your harness names it with `labels/1`, and those are 2 different mechanisms
reaching the same output: `labels/1` names the `{step, Id}` lines, `role/1` names
everything the process logs. Let them disagree and the same process appears
under 2 names in the same report, which is worse than no names at all. Deriving
both from `role_for/1` makes that impossible.

It's called **once**, after the run, which is why it takes the whole state
rather than one pid at a time. Your harness already holds its processes in
lists, so building the map forwards is a comprehension; being asked "what is
this pid called" one at a time would mean writing reverse lookups you otherwise
never need. And ids are handed out as processes register, so this mapping cannot
exist until the run is over.

Name what you care about and leave the rest out. Names are ordinary terms:
`dst_log` renders any `{Kind, N}` as `kind-n`, so `{replica, 2}` prints as
`replica-2` and `{reader, 6}` as `reader-6`. There is no special case for short
names, which is why `{r, 2}` would print as `r-2` and sit awkwardly next to
`reader-6` in a long column.

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
`tick(Tab)` calls. Every clause follows the same shape, so here are all three:

```erlang
run_op(Op, Tab, Replicas, Mode) ->
    ok = dst_log:role(role_for(Op)),
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

Naming and stamping happen once, in the `run_op/4` head, which is the same
`role_for/1` your `labels/1` uses. The `role/1` call belongs here rather than in
`execute/2` because `execute/2` runs in the *driver* process, and the role
belongs to the client that `spawn_op/1` created.

**Why bother.** 2 counters would give you a violation reporting
`earlier: {8, 11, ...}` and no way to look up event 8. With 1, **the violation
indexes into the narrative**: read the numbers, jump to those lines. That is the
difference between a log you search and a failure report that points at itself.

### 2 things you no longer have to get right

**The teardown boundary.** `dst_run` releases every suspended process before
tearing the system down, so anything they had left to do runs on the *real*
scheduler. In our hand-built version that produced 26 trailing events with no
marker, and a reader would have tried to make sense of them. `dst_log` records
a release marker and `analyze/0` hides everything after it by default.

**The perturbation check.** ETS operations don't block, and a step ends when a
process blocks in a receive, so logging can't move a step boundary. That's the
argument; `dst_log`'s own suite asserts it. Confirm it for your own logging:

```elixir
a = :dst_run.run(:abd_harness, Map.put(opts, :seed, 6)).trace
b = :dst_run.run(:abd_harness, Map.put(opts, :seed, 6) |> Map.put(:log, false)).trace
a == b
```

RESULT: `true`, at 35 entries each.

`log => false` suppresses the *events* and not the sequence numbers `log/1`
hands out. That distinction is load-bearing here: your harness stamps every
operation from `log/1`, so stamps that all read 0 would make `check/1`'s
"finished before started" test compare `0 < 0`, hold for no pair, and quietly
check nothing. The run would sail past the violation and the traces would differ
— `a` a strict prefix of `b`, because only one of them stopped at the bug.

**Observability that changes the schedule is worse than none**, because every
failure you then investigate is a different failure from the one you set out to
investigate.

### Read the failure

Four files have changed since step 10, so start a **fresh shell** and rebuild
`shrunk` from scratch. The old binding is stale and so are the beams behind it.

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

**Then replay the shrunk trace, and don't skip this.** Collection resets at the
start of every run, and `shrink/3` just did several hundred of them. Right now
the log holds whatever its *last* candidate did, which is not your failure.
Replaying puts the trace you care about in the log and nothing else:

```elixir
:dst_run.replay(:abd_harness, shrunk.trace, opts6)

{:violation, %{later: {_start, finish, _ts}}} = shrunk.outcome
:dst_log.analyze(%{until: finish})
```

The stamps in that violation are `dst_log` sequence numbers now, not the
`tick/1` counter they came from at step 10, so expect different values from the
ones you saw there. That is the point of the change: they are line numbers.

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

A `{step, _}` line and the events under it always carry the same name, which is
what `role_for/1` bought. If you ever see one process appear under 2 names, your
`labels/1` and your `role/1` calls have drifted apart.

### How to read 40 lines

You don't. The violation tells you which ones matter, and that is what the
single clock bought:

```
earlier: {19, 30, {1, 1}}
later:   {32, 43, {0, 0}}
```

Those are **line numbers**. Read 19 to 30 for how the earlier read saw `{1,1}`,
32 to 43 for how the later one missed it, and the `crashed_after_writing` line
above them for why there was anything to miss. Everything else is context you
can skip until you need it.

`dst_log:analyze(%{until: finish, driver: false})` drops the `$dst` lines
entirely if you want only what the system did.

### What it says

Line 16 is a writer that reached exactly 1 replica and died. Line 29 is a reader
that saw that value and returned it without making it durable. Line 42 is the
next reader, which didn't see it at all. Line 30 precedes line 32, so those 2
reads are sequential, and the register went backwards.

The counterintuitive part, and the reason this bug survives code review:
reader-6's quorum was `{replica-2, replica-3}` and reader-7's was
`{replica-3, replica-1}`. **They do intersect, at replica-3.** Quorum intersection worked exactly as advertised. It didn't
help, because `{1,1}` only ever existed at 1 replica, which is less than a
quorum, and intersection only guarantees you see what was written to a quorum.

Reader-6 got lucky enough to observe a value that was never durable, and by
returning it, published something the system could no longer produce. **The
write-back is what turns "I observed this" into "this is durable at a quorum"
before the value escapes.** With it, reader-6 writes `{1,1}` to 2 replicas
before returning, and reader-7's quorum of 2 cannot miss it. Not "probably
won't". Cannot, by counting.

### One thing that looks wrong and isn't

Lines 37 to 39 are replica-1 answering 3 reads in a row off a single step. It had never
been stepped before, so it drained a mailbox holding queries from crasher-1,
reader-6 and reader-7 in one go, and 2 of those replies went to processes that
had already exited. That is a `gen_server` running until it blocks, which is
exactly what a step is.

## Step 12: lock it in

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

That pair is the standard [page 4](04-writing-a-system-under-test.md) asks for
and that almost nobody manages: **a test that fails deterministically before
the fix and passes after it.** Not "we ran it a few times and it stopped
happening". The same seed, the same schedule, 1 line of protocol different.

## Step 13: put the clock on the critical path

Every run so far has reported `clock_ms: 0`. The system has no timers, so
`dst_time` has never done anything and the transform we put on `abd_replica`
back at step 3 has never rewritten a line. This step fixes both, by letting a
replica become unreachable and letting a client give up.

### Two ways to write a timeout, and the simple one works

The obvious version:

```erlang
receive
    {read_ack, Ref, _Index, Ts, Val} -> ...
after ?PHASE_TIMEOUT ->
    give_up()
end
```

`after` is a language construct rather than a call, so it looks as though a
transform that rewrites calls can't reach it. It can. `receive Cs after T -> B
end` is an abstract form like any other, and `dst_transform` rewrites it into a
receive whose timeout arrives as an ordinary message on the virtual clock.

**It's on by default**, because `after` is the one real-time dependence a system
can hold without naming a function. `-dst_after(false).` opts a module out.
`after 0` is never rewritten, since it's a mailbox poll rather than a wait.

So write the obvious version. It costs a timer armed and cancelled per receive,
which is worth knowing about before putting it on a hot loop and not worth
thinking about here.

The one thing to be deliberate about is what the deadline *means*. `after T`
inside a collection loop restarts on every message, so this is "give up if no
replica answers for T", not "give up if the whole phase takes longer than T".
For a quorum read that's a reasonable liveness policy. If you wanted a
whole-phase deadline you'd arm one `erlang:send_after/3` before the loop and
match its message as a clause, which the transform handles the same way.

### A replica that goes away

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
actually rewrites.

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

`op_number/1` and `role_for/1` each gain a clause:

```erlang
op_number({pause, N, _Index, _Ms}) -> N;
```

```erlang
role_for({pause, N, _Index, _Ms}) -> {pauser, N};
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
|> Enum.reject(&(&1.role == :"$dst"))
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
