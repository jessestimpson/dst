defmodule DstSchedTest do
  @moduledoc """
  Phase 0 exit criteria for `:eta_sched` (see docs/design/eta_framework_design.md).

  The bar: a `gen_server` ping-pong under supervision replays identically across
  many runs of one seed, selective-receive cases included. "Identical" is judged
  on an event trace the system under test writes itself — not on pids or refs,
  which differ between runs by construction and would make the assertion
  impossible to satisfy for the wrong reason.
  """
  use ExUnit.Case, async: false

  # The scheduler under test is Erlang: src/eta_sched.erl
  @sched :eta_sched

  # ---------------------------------------------------------------------------
  # Systems under test
  # ---------------------------------------------------------------------------

  defmodule Trace do
    @moduledoc false
    # A shared, ordered event log. Ordering is by insertion, which under a
    # serializing scheduler is exactly the schedule.
    def new, do: :ets.new(:eta_trace, [:public, :ordered_set])

    def record(tab, event) do
      :ets.insert(tab, {:erlang.unique_integer([:monotonic]), event})
    end

    def events(tab), do: :ets.tab2list(tab) |> Enum.map(&elem(&1, 1))
  end

  defmodule Pinger do
    @moduledoc false
    # Plain casts: no call timeouts, so nothing depends on the real clock.
    use GenServer

    def start_link({name, trace}) do
      GenServer.start_link(__MODULE__, {name, trace})
    end

    def init({name, trace}), do: {:ok, %{name: name, trace: trace, peers: []}}

    def handle_cast({:peers, peers}, st), do: {:noreply, %{st | peers: peers}}

    def handle_cast({:ping, 0}, st), do: {:noreply, st}

    def handle_cast({:ping, n}, st) do
      Trace.record(st.trace, {st.name, n})
      for p <- st.peers, do: GenServer.cast(p, {:ping, n - 1})
      {:noreply, st}
    end

    # A gen_server:call served here proves the scheduler copes with the caller's
    # selective receive on a monitor ref (see the SelectiveReceiver test too).
    def handle_call(:whoami, _from, st) do
      Trace.record(st.trace, {st.name, :whoami})
      {:reply, st.name, st}
    end
  end

  defmodule SelectiveReceiver do
    @moduledoc false
    # The case the naive quiescence check gets wrong: blocked on a specific tag,
    # with non-matching messages sitting in the mailbox.
    def start(name, trace) do
      spawn(fn -> loop(name, trace) end)
    end

    defp loop(name, trace) do
      receive do
        {:wanted, n} ->
          Trace.record(trace, {name, :wanted, n})
          loop(name, trace)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Build the ping-pong system, freeze it, run a seeded schedule, return the
  # trace. Everything the scheduler needs is created before anything can run.
  defp ping_pong_run(seed, opts \\ []) do
    trace = Trace.new()
    names = [:a, :b, :c]

    {:ok, sup} =
      Supervisor.start_link(
        Enum.map(names, fn n ->
          Supervisor.child_spec({Pinger, {n, trace}}, id: n)
        end),
        strategy: :one_for_one
      )

    pids =
      Supervisor.which_children(sup)
      |> Enum.map(fn {id, pid, _, _} -> {id, pid} end)
      |> Map.new()

    for {n, pid} <- pids do
      GenServer.cast(pid, {:peers, for(m <- names, m != n, do: Map.fetch!(pids, m))})
    end

    # Let the peer casts land and every server block, so the frozen state is the
    # same on every run.
    Process.sleep(20)

    all = Map.values(pids)
    sched = @sched.new(%{seed: seed}) |> @sched.register(all)

    GenServer.cast(Map.fetch!(pids, :a), {:ping, Keyword.get(opts, :depth, 4)})

    sched = @sched.run(sched, 500)
    events = Trace.events(trace)

    # Inspect before releasing: `release/1` shuts the scheduler down, so the
    # handle is dead afterwards.
    result = {events, @sched.choices(sched), @sched.stats(sched)}

    @sched.release(sched)
    Supervisor.stop(sup)
    :ets.delete(trace)

    result
  end

  # ---------------------------------------------------------------------------
  # Exit criterion 1 — replayability
  # ---------------------------------------------------------------------------

  describe "determinism" do
    test "one seed produces an identical trace across many runs" do
      # The design doc's Phase 0 bar, at its stated number. Each run builds and tears
      # down a real supervision tree; 1000 of them costs ~23s, which is worth
      # paying for the one assertion the whole phase rests on. Exact equality, so
      # a single divergence anywhere fails it.
      runs = for _ <- 1..1000, do: ping_pong_run(42)

      traces = Enum.map(runs, fn {events, _, _} -> events end)
      choices = Enum.map(runs, fn {_, c, _} -> c end)

      assert length(Enum.uniq(traces)) == 1,
             """
             #{length(Enum.uniq(traces))} distinct traces across 1000 runs of seed 42.
             First:  #{inspect(Enum.at(Enum.uniq(traces), 0), limit: 20)}
             Second: #{inspect(Enum.at(Enum.uniq(traces), 1), limit: 20)}
             """

      assert length(Enum.uniq(choices)) == 1, "the choice sequence itself diverged"

      # Non-vacuity: a trace of nothing would satisfy the above trivially.
      [trace | _] = traces
      assert length(trace) > 5, "expected a non-trivial trace, got #{inspect(trace)}"
    end

    test "different seeds explore different schedules" do
      traces = for s <- 1..12, do: elem(ping_pong_run(s, depth: 5), 0)

      # Every schedule must contain the same *events* — the workload is fixed —
      # while their order differs. That is the property that matters: the
      # scheduler is reordering execution, not changing what executes.
      [first | _] = traces

      assert Enum.all?(traces, &(Enum.sort(&1) == Enum.sort(first))),
             "seeds changed which events occurred, not just their order"

      assert length(Enum.uniq(traces)) > 1,
             "12 seeds produced one interleaving — the scheduler is not exploring"
    end

    test "a recorded choice sequence replays exactly, without the seed" do
      {events, choices, _} = ping_pong_run(7)

      # Rebuild the same system and drive it from the recorded ids alone, with a
      # different RNG seed to prove the sequence is what determines the outcome.
      trace = Trace.new()
      names = [:a, :b, :c]

      {:ok, sup} =
        Supervisor.start_link(
          Enum.map(names, fn n -> Supervisor.child_spec({Pinger, {n, trace}}, id: n) end),
          strategy: :one_for_one
        )

      pids =
        Supervisor.which_children(sup)
        |> Enum.map(fn {id, pid, _, _} -> {id, pid} end)
        |> Map.new()

      for {n, pid} <- pids do
        GenServer.cast(pid, {:peers, for(m <- names, m != n, do: Map.fetch!(pids, m))})
      end

      Process.sleep(20)

      sched = @sched.new(%{seed: 999_999}) |> @sched.register(Map.values(pids))
      GenServer.cast(Map.fetch!(pids, :a), {:ping, 4})

      {result, sched} = @sched.replay(sched, choices)
      replayed = Trace.events(trace)

      @sched.release(sched)
      Supervisor.stop(sup)
      :ets.delete(trace)

      assert result == :ok, "replay diverged: #{inspect(result)}"
      assert replayed == events
    end
  end

  # ---------------------------------------------------------------------------
  # Exit criterion 2 — selective receive
  # ---------------------------------------------------------------------------

  describe "selective receive" do
    test "a process blocked on a non-matching mailbox is not runnable" do
      trace = Trace.new()
      pid = SelectiveReceiver.start(:sr, trace)
      Process.sleep(20)

      sched = @sched.new(%{seed: 1}) |> @sched.register([pid])
      [id] = @sched.ids(sched)

      # Two messages it cannot match. The naive check (status == :waiting and
      # queue == 0) would call this runnable forever.
      send(pid, {:unwanted, 1})
      send(pid, {:unwanted, 2})

      assert @sched.runnable(sched) == [id], "a non-empty mailbox should offer a first step"

      {outcome, sched} = @sched.step(sched, id)
      assert outcome == :no_progress, "it cannot match either message"

      assert @sched.runnable(sched) == [],
             "a process blocked against its current mailbox must not stay runnable"

      # A message it *can* match re-arms it.
      send(pid, {:wanted, 42})
      assert @sched.runnable(sched) == [id], "a new message must make it runnable again"

      {outcome, sched} = @sched.step(sched, id)
      assert outcome == :progress

      assert Trace.events(trace) == [{:sr, :wanted, 42}]

      # Blocked again, with the two unwanted messages still queued.
      assert @sched.runnable(sched) == []

      @sched.release(sched)
      Process.exit(pid, :kill)
      :ets.delete(trace)
    end

    test "run/2 terminates with a permanently-blocked process" do
      # The termination condition ("nothing runnable") has to be reachable even
      # when a mailbox is non-empty forever, or a soak hangs instead of finishing.
      trace = Trace.new()
      pid = SelectiveReceiver.start(:sr, trace)
      Process.sleep(20)

      sched = @sched.new(%{seed: 1}) |> @sched.register([pid])
      send(pid, {:unwanted, :never_matches})

      sched = @sched.run(sched, 1_000)

      assert @sched.stats(sched).steps < 5,
             "run/2 kept stepping a process that cannot progress"

      assert @sched.runnable(sched) == []
      assert Trace.events(trace) == []

      @sched.release(sched)
      Process.exit(pid, :kill)
      :ets.delete(trace)
    end

    test "a gen_server call's reply-matching receive is handled" do
      # gen_server:call blocks the caller in a selective receive on a monitor ref
      # while unrelated messages may be queued — the real-world instance of the
      # case above.
      trace = Trace.new()
      {:ok, server} = Pinger.start_link({:srv, trace})
      Process.sleep(20)

      caller_parent = self()

      caller =
        spawn(fn ->
          receive do
            :go -> :ok
          end

          send(caller_parent, {:result, GenServer.call(server, :whoami, :infinity)})
        end)

      Process.sleep(20)

      sched = @sched.new(%{seed: 3}) |> @sched.register([server, caller])

      # Queue noise the caller cannot match, then let it make the call.
      send(caller, {:noise, 1})
      send(caller, :go)

      sched = @sched.run(sched, 200)

      assert Trace.events(trace) == [{:srv, :whoami}]
      assert_receive {:result, :srv}, 1_000

      @sched.release(sched)
      Process.exit(caller, :kill)
      GenServer.stop(server)
      :ets.delete(trace)
    end
  end

  # ---------------------------------------------------------------------------
  # The scheduler shares a mailbox with the driver
  # ---------------------------------------------------------------------------

  describe "driver mailbox" do
    # Phase 0 ran the scheduler in the driver's process, reading trace events from
    # the driver's mailbox. An early version used a catch-all `other ->` clause
    # while waiting for those events, which consumed and discarded everything else
    # — including replies the system under test had sent the driver. It presented
    # as a gen_server reply vanishing with its caller exiting `:normal`.
    #
    # Phase 3 moved the scheduler into its own process, so this is now structural
    # rather than a discipline the scheduler has to keep. The test stays because
    # the property is what matters, not the mechanism that currently provides it —
    # and because it would catch a future change that moved it back.
    test "the scheduler does not share the driver's process" do
      sched = @sched.new()
      pid = @sched.pid(sched)

      assert is_pid(pid)
      assert pid != self(), "the scheduler is running in the driver's process"
      assert Process.alive?(pid)

      @sched.release(sched)
      refute Process.alive?(pid), "release/1 left the scheduler running"
    end

    test "messages sent to the driver by the system under test are not consumed" do
      driver = self()

      talker =
        spawn(fn ->
          loop = fn f ->
            receive do
              {:say, term} ->
                send(driver, {:from_sut, term})
                f.(f)
            end
          end

          loop.(loop)
        end)

      Process.sleep(20)
      sched = @sched.new(%{seed: 11}) |> @sched.register([talker])

      # Pre-load the driver's mailbox too, so we also prove pre-existing messages
      # survive a run.
      send(driver, {:planted, :before})

      for n <- 1..5, do: send(talker, {:say, n})
      sched = @sched.run(sched, 100)

      assert_receive {:planted, :before}, 500

      for n <- 1..5 do
        assert_receive {:from_sut, ^n}, 500, "the scheduler ate message #{n}"
      end

      @sched.release(sched)
      Process.exit(talker, :kill)
    end
  end

  # ---------------------------------------------------------------------------
  # Exit criterion 3 — spawned processes are adopted
  # ---------------------------------------------------------------------------

  describe "auto-registration" do
    test "a process spawned by a scheduled process comes under control" do
      trace = Trace.new()
      parent_of_test = self()

      spawner =
        spawn(fn ->
          receive do
            {:spawn_child, n} ->
              for i <- 1..n do
                spawn(fn ->
                  receive do
                    :work ->
                      Trace.record(trace, {:child, i})
                      send(parent_of_test, {:child_done, i})
                  end
                end)
                |> then(&send(parent_of_test, {:child, i, &1}))
              end
          end

          receive do
            :never -> :ok
          end
        end)

      Process.sleep(20)
      sched = @sched.new(%{seed: 5}) |> @sched.register([spawner])
      before = @sched.stats(sched).processes

      send(spawner, {:spawn_child, 3})
      {_outcome, sched} = @sched.step(sched, 0)

      assert @sched.stats(sched).processes == before + 3,
             "spawned children were not adopted: #{inspect(@sched.stats(sched))}"

      # Adopted children are suspended, so they do no work until stepped.
      children =
        for _ <- 1..3 do
          assert_receive {:child, _i, pid}, 1_000
          pid
        end

      for c <- children, do: send(c, :work)
      refute_receive {:child_done, _}, 100

      sched = @sched.run(sched, 100)
      assert length(Trace.events(trace)) == 3

      @sched.release(sched)
      for c <- children, do: Process.exit(c, :kill)
      Process.exit(spawner, :kill)
      :ets.delete(trace)
    end
  end

  describe "cold code" do
    # The failure this catches is the one `eta_run`'s docs used to end with
    # "nothing catches it": a process waiting on `code_server` is, to the
    # scheduler, a process blocked in a receive, so if it is the last thing
    # standing the run ends at what looks exactly like quiescence and reports
    # success.
    #
    # Reproducing that by racing a real demand-load is what `eta_2pc`'s
    # `maybe_touch_lazy/2` declines to do, and for a good reason: whether the
    # code server answers before the run reaches quiescence is a question about
    # real time, and it was measured at 2 failures in 14 runs. A flaky test in a
    # project about determinism is not a test.
    #
    # So the race is removed rather than run. The code server is suspended, which
    # turns "parked on the code server" from a moment into a state, and the
    # assertion is then about the detector rather than about the clock.
    setup do
      # Everything this test touches while the code server is down has to be warm
      # already, including the scheduler's own warning path — a load inside that
      # window would deadlock rather than fail.
      :ok = :eta_run.preload([])
      Code.ensure_loaded!(:eta_cold_probe)
      _ = :logger.warning("eta_sched_test: warming the warning path", [])
      :ok
    end

    # Suspends the code server from a short-lived process of its own. A suspend
    # is released when the suspending process dies, so a hang in the test cannot
    # leave the VM without a code server and take the rest of the suite with it.
    defp without_code_server(fun) do
      parent = self()
      cs = Process.whereis(:code_server)

      keeper =
        spawn(fn ->
          true = :erlang.suspend_process(cs)
          send(parent, {self(), :suspended})

          receive do
            :resume -> :ok
          after
            10_000 -> :ok
          end
        end)

      assert_receive {^keeper, :suspended}, 1_000

      try do
        fun.()
      after
        send(keeper, :resume)
      end
    end

    test "a run that ends with a process on the code server says so" do
      if :erlang.module_loaded(:eta_cold) do
        :code.purge(:eta_cold)
        :code.delete(:eta_cold)
        :code.purge(:eta_cold)
      end

      refute :erlang.module_loaded(:eta_cold)

      sched = @sched.new(%{seed: 1})

      victim =
        without_code_server(fn ->
          victim = spawn(fn -> :eta_cold_probe.touch() end)
          assert parked?(victim), "the probe never reached the code server"

          # Nothing is runnable: the victim's mailbox is empty and will stay that
          # way for as long as the code server is down. So the run ends here,
          # believing the system quiescent — which is the whole failure.
          sched = sched |> @sched.register([victim]) |> @sched.run(200)

          assert %{cold_code: [entry]} = @sched.stats(sched)

          assert %{pid: ^victim, at: {:eta_cold_probe, :touch, 0}} = entry,
                 "the entry has to name the frame that reached the cold module, " <>
                   "or it says a run is spoiled without saying what to preload"

          victim
        end)

      @sched.release(sched)
      Process.exit(victim, :kill)
    end

    test "a healthy run reports none" do
      pid = spawn(fn -> receive do: (:never -> :ok) end)
      sched = @sched.new(%{seed: 1}) |> @sched.register([pid]) |> @sched.run(50)

      assert %{cold_code: []} = @sched.stats(sched)

      @sched.release(sched)
      Process.exit(pid, :kill)
    end
  end

  defp parked?(pid, tries \\ 200)
  defp parked?(_pid, 0), do: false

  defp parked?(pid, tries) do
    case Process.info(pid, :current_function) do
      {:current_function, {:code_server, :call, 1}} ->
        true

      _ ->
        Process.sleep(5)
        parked?(pid, tries - 1)
    end
  end
end
