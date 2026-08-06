defmodule DstRunTest do
  @moduledoc """
  `eta_run` and the `eta_harness` contract — Phase 3 of the DST framework.

  Driven against `eta_2pc`, deliberately not against the registry this framework grew
  out of. The claim
  Phase 3 has to support is that the framework is reusable, and a framework
  validated only against the system it grew out of has not been shown to be
  anything of the sort. Two-phase commit shares nothing with the registry: no
  leader, no replication, no durable backend, a different invariant.

  The load-bearing test is `finds the planted defect`. A framework that has only
  ever been pointed at correct systems has not been shown to find anything, so the
  SUT ships with a coordinator bug selectable by config — and one that is *only*
  reachable through an unlucky interleaving, so finding it is a property of the
  scheduler rather than of the workload.
  """
  use ExUnit.Case, async: false

  @run :eta_run
  @sut :eta_2pc

  # Enough operations to get interesting interleavings without making a failure
  # tedious to read.
  @opts %{max_ops: 25, max_steps: 20_000}

  defp run(seed, config \\ %{}) do
    @run.run(@sut, Map.merge(@opts, %{seed: seed, config: config}))
  end

  describe "a correct system" do
    test "no seed produces an atomicity violation" do
      results = for seed <- 1..40, do: run(seed)

      # NB: `assert/2`'s message is an ordinary argument and is evaluated whether
      # or not the assertion holds, so it must be safe on the passing path.
      outcomes = results |> Enum.map(& &1.outcome) |> Enum.uniq()

      # Also covers "ended abnormally": a run that exhausted its budget did not
      # demonstrate the absence of a violation, it just stopped early.
      assert outcomes == [:ok],
             "correct 2PC did not come out clean: #{inspect(outcomes, pretty: true)}"
    end

    test "the runs actually did something" do
      # Non-vacuity for the test above. A framework that silently executes nothing
      # satisfies "no violations" perfectly.
      results = for seed <- 1..10, do: run(seed)

      assert Enum.all?(results, &(&1.ops == 25)), "not every operation was injected"

      assert Enum.all?(results, &(&1.steps > 50)),
             "suspiciously few steps: #{inspect(Enum.map(results, & &1.steps))}"

      # A stalling participant leaves the coordinator's 30s prepare timeout as the
      # only way forward, so the run deadlocks unless the idle callback advances
      # the clock. Every one of these seeds generates at least one stall, and the
      # seeds are fixed, so `all` is deterministic rather than optimistic.
      assert Enum.all?(results, &(&1.clock_ms >= 30_000)),
             "the virtual clock did not reach a prepare timeout: " <>
               inspect(Enum.map(results, & &1.clock_ms))

      # And it cost nothing: 30 simulated seconds per run, forty runs, well under
      # a second of real time.
      assert Enum.sum(Enum.map(results, & &1.clock_ms)) >= 300_000
    end
  end

  describe "the schedule is what decides" do
    # The sharpest evidence available that this is deterministic *simulation* and
    # not just a test loop, and the only test here that isolates the claim.
    #
    # The workload is pinned — every transaction has the same votes, so
    # `generate/2` draws nothing from the seed. The seed therefore varies one
    # thing: the order in which the three participants' votes reach the
    # coordinator. `first_vote_wins` commits if a `yes` gets there first and
    # aborts if the `no` does, so the same workload is either a violation or not
    # depending on nothing but the interleaving.
    #
    # Measured at ~26% of seeds. The assertion is only that both outcomes occur,
    # since pinning the rate would be pinning the RNG.
    test "one workload, different schedules, different outcomes" do
      config = %{mode: :first_vote_wins, plan: [:yes, :no, :yes]}

      outcomes =
        for seed <- 1..40 do
          @run.run(@sut, %{seed: seed, max_ops: 1, max_steps: 5_000, config: config}).outcome
        end

      violated = Enum.count(outcomes, &match?({:violation, _}, &1))

      assert violated > 0,
             "no schedule reached the defect — the scheduler is not reordering the votes"

      assert violated < 40,
             "every schedule reached it, so the outcome is not schedule-dependent " <>
               "and this test is proving nothing"
    end

    test "the same pinned workload is clean against the correct coordinator" do
      # The control. A `no` vote means abort under any interleaving when the
      # coordinator waits for all the votes, so the schedule must stop mattering.
      config = %{plan: [:yes, :no, :yes]}

      outcomes =
        for seed <- 1..40 do
          @run.run(@sut, %{seed: seed, max_ops: 1, max_steps: 5_000, config: config}).outcome
        end

      assert Enum.uniq(outcomes) == [:ok]
    end
  end

  describe "finding a bug" do
    test "finds the planted defect within a seed budget" do
      # With a 25-transaction workload this is easy — measured at 60/60 seeds,
      # usually within the first ten steps. That is worth stating plainly rather
      # than dressing up: a defect with 25 chances to fire is not evidence that
      # the scheduler explores anything. The schedule-dependence is established
      # above, one transaction at a time; what this test adds is that a run
      # *reports* the violation usefully — the right property, the coordinator's
      # decision, and the participant states that contradict it.
      config = %{mode: :first_vote_wins}

      found =
        Enum.find(1..60, fn seed ->
          match?({:violation, _}, run(seed, config).outcome)
        end)

      assert found, "60 seeds did not reach the planted defect"

      %{outcome: {:violation, details}} = run(found, config)

      assert details.property == :atomicity
      assert details.decision == :commit

      states = details.participants |> Enum.map(&elem(&1, 1)) |> Enum.sort() |> Enum.uniq()

      assert :committed in states and :aborted in states,
             "the violation was reported without mixed states: #{inspect(details)}"
    end

    test "the correct coordinator survives the seeds that break the broken one" do
      # The other half of the claim: the seeds are not simply pathological. The
      # same workloads must be clean against the correct coordinator, or the test
      # above is finding a bug in the harness rather than in the system.
      broken =
        Enum.filter(1..60, fn seed ->
          match?({:violation, _}, run(seed, %{mode: :first_vote_wins}).outcome)
        end)

      assert broken != [], "no seed broke the broken coordinator"

      for seed <- broken do
        assert run(seed).outcome == :ok,
               "seed #{seed} fails against the correct coordinator too"
      end
    end
  end

  describe "determinism and replay" do
    test "one seed produces one trace" do
      traces = for _ <- 1..5, do: run(9).trace

      assert length(Enum.uniq(traces)) == 1,
             "#{length(Enum.uniq(traces))} distinct traces from one seed"

      assert length(hd(traces)) > 20, "the trace is too short to mean anything"
    end

    test "a recorded trace replays to the same outcome" do
      config = %{mode: :first_vote_wins}

      seed =
        Enum.find(1..60, fn s ->
          match?({:violation, _}, run(s, config).outcome)
        end)

      assert seed, "no failing seed to replay"
      original = run(seed, config)

      replayed =
        @run.replay(
          @sut,
          original.trace,
          Map.merge(@opts, %{seed: seed, config: config})
        )

      assert replayed.outcome == original.outcome,
             "replay produced #{inspect(replayed.outcome)}, not #{inspect(original.outcome)}"

      # The trace is a prefix: replay stops at the violation just as the original
      # did, having executed the same entries in the same order.
      assert replayed.trace == original.trace
    end

    test "a replay that diverges says so rather than pretending" do
      original = run(3)

      # A step naming a process that will not be runnable at that point. Ids are
      # assigned in registration order, so a high one names a client that does not
      # exist yet at the start of the run.
      corrupted = [{:step, 999} | original.trace]

      assert %{outcome: {:error, {:diverged, 999, _runnable}}} =
               @run.replay(@sut, corrupted, Map.merge(@opts, %{seed: 3}))
    end
  end

  describe "the check contract" do
    test "an invariant that calls into a suspended process is reported, not hung" do
      # The trap `eta_harness` warns about, made concrete. `check/1` here does what an
      # invariant naturally wants to do — ask a process how it is doing — which
      # cannot be served while that process is suspended.
      assert %{outcome: {:error, :check_blocked}} =
               @run.run(:eta_blocking_check_sut, %{seed: 1, max_ops: 1, max_steps: 100})
    end
  end

  # Loading a module on demand is a synchronous call into `code_server`, which
  # the scheduler does not own, so a scheduled process that reaches an unloaded
  # module blocks outside the schedule and is made runnable again on wall clock
  # time. It is the cause of "the first run in a fresh VM diverges", and it is
  # otherwise invisible: nothing is logged and `adopted_late` stays 0.
  describe "modules_loaded" do
    test "is empty when the run preloads its code" do
      # `preload` defaults to `[kernel, stdlib, eta]`, and this harness lives in
      # the eta application, so one run is enough. No warm-up, no second pass.
      assert %{modules_loaded: []} =
               :eta_run.run(:eta_2pc, %{seed: 1, max_ops: 5, max_steps: 5_000})
    end

    test "preload is what makes that hold across seeds" do
      # The claim worth pinning. Warming is per *code path*, not per VM: a seed
      # that first reaches a stalling participant runs the coordinator's timeout
      # branch and loads code no earlier seed touched. Running a seed twice fixes
      # that seed and not the next one. Preloading the application fixes all of
      # them, which is why it is the recommended answer rather than a warm-up.
      for seed <- 1..10 do
        result = :eta_run.run(:eta_2pc, %{seed: seed, max_ops: 10, max_steps: 10_000})

        # `audit/1` rather than three assertions: it knows which fields mean
        # "wall clock got in", and it keeps knowing as more are added.
        assert :eta_run.audit(result) == :ok, "seed #{seed}: #{inspect(:eta_run.audit(result))}"
      end
    end

    test "names a module a scheduled process demand-loaded" do
      # Plant the defect: `eta_2pc_lazy` is reachable only from inside a client,
      # which is a process the scheduler owns. `preload: false` is required here
      # — it ships in the eta application, so the default preload would warm the
      # very module this test needs cold. That is the escape hatch working.
      #
      # Unloading only if it is loaded: ExUnit randomises test order, and
      # `:code.delete/1` answers false for a module that was never loaded.
      if :erlang.module_loaded(:eta_2pc_lazy) do
        :code.purge(:eta_2pc_lazy)
        :code.delete(:eta_2pc_lazy)
        :code.purge(:eta_2pc_lazy)
      end

      refute :erlang.module_loaded(:eta_2pc_lazy)

      result =
        :eta_run.run(:eta_2pc, %{
          seed: 1,
          max_ops: 5,
          max_steps: 5_000,
          preload: false,
          config: %{lazy: true}
        })

      assert :eta_2pc_lazy in result.modules_loaded,
             "a module loaded mid-run must be reported, or the failure it causes " <>
               "is silent: #{inspect(:eta_run.summary(result), pretty: true)}"
    end

    test "preloading catches it before the run starts" do
      # The other half: the same cold module, with preloading on. It is warm
      # before there is a scheduler to be outside of, so the run is clean.
      if :erlang.module_loaded(:eta_2pc_lazy) do
        :code.purge(:eta_2pc_lazy)
        :code.delete(:eta_2pc_lazy)
        :code.purge(:eta_2pc_lazy)
      end

      assert %{modules_loaded: []} =
               :eta_run.run(:eta_2pc, %{
                 seed: 1,
                 max_ops: 5,
                 max_steps: 5_000,
                 config: %{lazy: true}
               })
    end

    test "a misnamed application fails loudly" do
      # Silently preloading nothing would be the same class of failure the
      # option exists to remove.
      assert_raise ErlangError, fn ->
        :eta_run.run(:eta_2pc, %{seed: 1, max_ops: 1, preload: [:no_such_app_here]})
      end
    end
  end

  # A step ends when the process blocks in a receive. If no scheduling event
  # arrives at all, the scheduler eventually gives up and suspends the process
  # wherever it happens to be — a suspension point chosen by wall clock rather
  # than by the schedule. This used to be a 5ms deadline and was silent.
  describe "steps that never finished" do
    test "a healthy run reports none" do
      %{sched: sched} = :eta_run.run(:eta_2pc, %{seed: 4, max_ops: 10, max_steps: 10_000})

      assert sched.timeouts == 0
    end

    test "the two determinism counters are what a suite should assert on" do
      # All three should be clean, and all three are silent failures otherwise.
      # This is the assertion worth copying into your own suite, next to the
      # invariants.
      #
      # Warming is per *code path*, not per VM, which is worth knowing before
      # you write this assertion yourself. A seed that first reaches a stalling
      # participant runs the coordinator's timeout branch and loads code no
      # earlier seed touched, so one warm-up run does not cover the next seed.
      # Either warm each seed, as here, or preload the application in `init/2`.
      for seed <- 1..10 do
        opts = %{seed: seed, max_ops: 10, max_steps: 10_000}
        :eta_run.run(:eta_2pc, opts)

        %{modules_loaded: modules, sched: sched} = :eta_run.run(:eta_2pc, opts)

        assert sched.adopted_late == 0, "seed #{seed} adopted #{sched.adopted_late} late"
        assert sched.timeouts == 0, "seed #{seed} had #{sched.timeouts} unfinished steps"
        assert modules == [], "seed #{seed} loaded #{inspect(modules)} mid-run"
      end
    end
  end

  describe "reading a result" do
    test "summary/1 swaps the trace for its length" do
      result = :eta_run.run(:eta_2pc, %{seed: 1, max_ops: 5, max_steps: 5_000})
      summary = :eta_run.summary(result)

      # The point: inspecting a result whole renders a few hundred trace entries
      # and elides the fields you were looking for underneath them.
      refute Map.has_key?(summary, :trace)
      assert summary.trace_length == length(result.trace)
      assert summary.trace_length > 0
      assert summary.outcome == result.outcome
      assert summary.sched == result.sched
    end

    test "summary/1 takes a shrink result too" do
      config = %{mode: :first_vote_wins}

      seed =
        Enum.find(1..60, fn s ->
          match?({:violation, _}, run(s, config).outcome)
        end)

      opts = Map.merge(@opts, %{seed: seed, config: config})
      shrunk = :eta_shrink.shrink(@sut, run(seed, config).trace, opts)
      summary = :eta_run.summary(shrunk)

      refute Map.has_key?(summary, :trace)

      # On a shrink result `trace_length` says which trace you were handed:
      # `shrunk` when verified, `original` when the search found something
      # smaller that did not survive strict replay.
      expected = if shrunk.verified, do: shrunk.shrunk, else: shrunk.original
      assert summary.trace_length == expected
    end

    test "audit/1 passes a healthy run" do
      result = :eta_run.run(:eta_2pc, %{seed: 1, max_ops: 5, max_steps: 5_000})

      assert :eta_run.audit(result) == :ok
    end

    test "audit/1 names what went wrong" do
      # Same planted defect as the modules_loaded tests: a module reachable only
      # from a scheduled process, with preloading off so it stays cold.
      if :erlang.module_loaded(:eta_2pc_lazy) do
        :code.purge(:eta_2pc_lazy)
        :code.delete(:eta_2pc_lazy)
        :code.purge(:eta_2pc_lazy)
      end

      refute :erlang.module_loaded(:eta_2pc_lazy), "the module did not unload"

      result =
        :eta_run.run(:eta_2pc, %{
          seed: 1,
          max_ops: 5,
          max_steps: 5_000,
          preload: false,
          config: %{lazy: true}
        })

      assert :erlang.module_loaded(:eta_2pc_lazy),
             "the run never reached the module: #{inspect(:eta_run.summary(result), pretty: true)}"

      assert {:suspect, reasons} = :eta_run.audit(result)
      assert {:modules_loaded, loaded} = List.keyfind(reasons, :modules_loaded, 0)
      assert :eta_2pc_lazy in loaded
    end

    test "audit/1 reports a process the run left waiting on the code server" do
      # The other half of the cold-code problem, and the one `modules_loaded`
      # structurally cannot see: the load had not finished when the run did, so
      # there is nothing in `modules_loaded` to find. `eta_sched` catches it from
      # the other side and reports it in `sched.cold_code`; this is the check
      # that `audit/1` is actually looking at that field.
      #
      # Fabricated rather than provoked. Producing the state for real means
      # racing the code server, which is what `eta_sched_test`'s "cold code"
      # describe does properly — by suspending it. What is under test here is the
      # plumbing between the two modules, which a real run cannot exercise any
      # more directly than this can.
      cold = %{id: 3, pid: self(), at: {:my_client, :commit, 2}}

      result = %{
        modules_loaded: [],
        sched: %{adopted_late: 0, timeouts: 0, cold_code: [cold]}
      }

      assert {:suspect, reasons} = :eta_run.audit(result)
      assert {:cold_code, [^cold]} = List.keyfind(reasons, :cold_code, 0)
    end
  end
end
