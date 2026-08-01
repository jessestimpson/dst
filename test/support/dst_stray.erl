-module(dst_stray).

%% A system whose only interesting property is that it can leave a timer in the
%% virtual wheel belonging to a process `dst_sched` does not own.
%%
%% It exists to reproduce, in about eighty lines, a divergence found in a real
%% system (`dgen_registry`) where it took a day to corner. There, a leadership
%% handoff fans out an RPC per peer from a bare `spawn` and collects the answers
%% under a single `receive ... after ?GATHER_TIMEOUT + 500`. `dst_transform` puts
%% that receive on the virtual clock. Two runs of one seed then produced traces
%% that were identical for hundreds of entries and then differed by a single
%% `{clock, 2500}` entry **with no step behind it** — a deadline became due and
%% made nothing runnable. `dst_run:audit/1` reported `ok` on both: nothing was
%% adopted late, no module was loaded, no scheduler timeout. The leak was not in
%% the schedule at all, it was in the wheel.
%%
%% The general shape, which is what this module isolates:
%%
%%   **A timer is owned by a process. The scheduler is not obliged to own that
%%   process. When it does not, the timer is still in the wheel and the driver
%%   still advances to it.**
%%
%% Two ways to get there, both modelled below and both reachable from ordinary
%% code:
%%
%% `dead` — the owner is gone. `dst_transform` puts `dst_time:disarm_after/2` at
%% the head of every ordinary receive clause, so a process that never reaches one
%% never disarms. Kill it while it blocks and the deadline outlives it forever.
%%
%% `unowned` — the owner is alive but was spawned where the scheduler could not
%% adopt it. `dst_sched:spawn/1` falls back to a plain `erlang:spawn/1` when no
%% scheduler is active, and `dst_harness:init/2` runs before there is one. So
%% anything a system spawns while starting up is outside the schedule by
%% construction, and it wakes on the real scheduler when its deadline fires.
%%
%% The workers are here only so a run has something legitimate to schedule.

-include_lib("dst/include/dst.hrl").

-behaviour(gen_server).

-export([start_link/1, stop/1, tick/1, await_release/1, spawn_waiter/1, deadline/0, heartbeat/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

%% `?GATHER_TIMEOUT + 500` in the system this was extracted from.
-define(DEADLINE, 2500).

%% Every worker heartbeats, for the same reason the original system did, and it
%% is what makes the reproduction faithful rather than merely true.
%%
%% Without it the stray is the *only* timer anywhere, so it is the first deadline
%% the driver sees and it advances to it as its opening move. That diverges the
%% two runs from entry zero — the advance draws from the seeded RNG, so every
%% choice after it shifts — which reproduces "the traces differ" but not the thing
%% that made the original hard: the traces were **identical for hundreds of
%% entries** and then differed by one inserted advance.
%%
%% With a heartbeat the wheel is never empty. Deadlines land at 1000, 2000, 3000
%% … in both runs, the stray's 2500 sits between two of them doing nothing, and
%% the runs stay step-for-step identical until the clock actually reaches it.
%% Which is the shape to recognise: **a stray deadline is invisible until it
%% becomes the earliest one.**
-define(HEARTBEAT, 1000).

-record(st, {index :: pos_integer(), ticks = 0 :: non_neg_integer()}).

deadline() -> ?DEADLINE.

heartbeat() -> ?HEARTBEAT.

start_link(Index) ->
    gen_server:start_link(?MODULE, Index, []).

stop(Pid) ->
    gen_server:stop(Pid).

%% Asynchronous, so a client and a worker are two schedulable participants rather
%% than one blocked on the other.
tick(Pid) ->
    gen_server:cast(Pid, tick).

init(Index) ->
    ok = ?DST_LABEL({worker, Index}),
    _ = erlang:send_after(?HEARTBEAT, self(), heartbeat),
    {ok, #st{index = Index}}.

handle_call(get, _From, St = #st{ticks = N}) ->
    {reply, N, St}.

handle_cast(tick, St = #st{ticks = N}) ->
    _ = ?DST_LOG({ticked, N + 1}),
    {noreply, St#st{ticks = N + 1}}.

%% Periodic and self-perpetuating, so the wheel is never empty and the driver's
%% clock advances are routine rather than remarkable. See the define.
handle_info(heartbeat, St) ->
    _ = ?DST_LOG(heartbeat),
    _ = erlang:send_after(?HEARTBEAT, self(), heartbeat),
    {noreply, St}.

%% The receive the whole reproduction rests on. Nothing ever sends `release`, so
%% the deadline is the only thing that can end it — which is exactly the position
%% a collector is in when the peer it is waiting for never answers.
%%
%% Rewritten by `dst_transform` into a receive whose timeout is an ordinary
%% message clause, armed through `dst_time:arm_after/2`. The arming is what puts a
%% row in the wheel; reaching a clause head is what takes it out again.
await_release(Ms) ->
    receive
        release -> released
    after Ms -> timed_out
    end.

%% Deliberately a bare `spawn/1` and deliberately not linked: the point is a
%% process nobody supervises and nobody is waiting for. Under the transform this
%% is `dst_sched:spawn/1`, which is a plain spawn whenever no scheduler is
%% running — so calling it from `init/2` produces an unowned process every time.
spawn_waiter(Ms) ->
    spawn(fun() -> await_release(Ms) end).
