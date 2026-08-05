defmodule EtaNetRunTest do
  @moduledoc """
  `:eta_net` under `:eta_run` — the network as part of a real run rather than
  driven by hand.

  Two-phase commit is the system under test because its traffic is exactly the
  shape a network makes interesting: the coordinator casts `prepare` to three
  participants **inside one scheduler step**, which is the state
  `README.md` says the schedule alone cannot produce —

  > A client that sends to 3 peers does so inside one scheduler step, so a
  > partially-delivered broadcast isn't a state the scheduler can produce.

  With a network it is one drop away.
  """
  use ExUnit.Case, async: false

  @run :eta_run
  @net :eta_net
  @sut :eta_2pc

  @opts %{max_ops: 12, max_steps: 8_000, preload: [:eta]}

  setup do
    on_exit(fn ->
      @net.stop()
      :eta_time.stop()
    end)

    :ok
  end

  defp run(opts), do: @run.run(@sut, Map.merge(@opts, opts))

  describe "a perfect network" do
    test "produces exactly the trace of no network at all" do
      # The control run, and the thing that makes `drop_p => 0.0` meaningful: a
      # network that drops and delays nothing must be indistinguishable from not
      # having one, or every result measured against it is measured against a
      # different system.
      without = run(%{seed: 5})
      with_net = run(%{seed: 5, net: true})

      assert without.trace == with_net.trace
      assert without.outcome == with_net.outcome
      assert :ok = @run.audit(with_net)
    end

    test "reports what it carried" do
      result = run(%{seed: 5, net: true})

      assert %{net: %{delivered: delivered, dropped: 0, in_flight: 0}} = result
      assert delivered > 0, "the network was installed but nothing went through it"
    end

    test "a run with no network reports an empty net map rather than zeroes" do
      assert %{net: %{}} = result = run(%{seed: 5})
      assert map_size(result.net) == 0
    end

    test "`net: false` means no network even if one was left running" do
      # The network is a named table, so it is global to the VM and outlives its
      # starter. Without this, a run that injects its faults in its own workload —
      # which is what docs/06 teaches — would silently also be losing messages
      # under whatever policy an earlier test installed.
      :ok = @net.start(%{seed: 99, policy: %{drop_p: 1.0}})

      result = run(%{seed: 5})

      refute @net.running(), "the run left someone else's network in place"
      assert map_size(result.net) == 0
      assert result.outcome == :ok
    end
  end

  describe "a lossy network" do
    @lossy %{net: %{policy: %{drop_p: 0.25, scope: {:tags, [:prepare, :vote, :decision]}}}}

    test "loses protocol messages and the system still agrees" do
      result = run(Map.merge(%{seed: 9}, @lossy))

      assert %{outcome: :ok} = result
      assert %{net: %{dropped: dropped}} = result

      # Non-vacuity. A run whose network never lost anything tested the same
      # system a perfect one would and reports the same `ok`.
      assert dropped > 0, "the lossy run dropped nothing; it proves nothing"
      assert :ok = @run.audit(result)
    end

    test "the fault schedule is a function of the seed" do
      a = run(Map.merge(%{seed: 9}, @lossy))
      b = run(Map.merge(%{seed: 9}, @lossy))

      assert a.trace == b.trace
      assert a.net == b.net
      assert a.net.dropped > 0
    end

    test "a different seed loses different messages" do
      a = run(Map.merge(%{seed: 9}, @lossy))
      b = run(Map.merge(%{seed: 21}, @lossy))

      assert a.net.dropped != b.net.dropped or a.trace != b.trace
    end

    test "a recorded trace replays under the same faults" do
      # Replay draws no choices from the seed, but the network still makes
      # decisions — and it makes the same ones, because they are a function of the
      # order sends happen in, which the trace fixes.
      recorded = run(Map.merge(%{seed: 9}, @lossy))

      replayed =
        @run.replay(@sut, recorded.trace, Map.merge(@opts, Map.merge(%{seed: 9}, @lossy)))

      assert replayed.outcome == recorded.outcome
      assert replayed.net == recorded.net
      assert replayed.skipped == 0
    end
  end

  describe "sends outside the schedule" do
    test "are counted, and audit/1 says so" do
      # `eta_sched:stepping/0` is the exact test: during a step exactly one
      # process runs, so a send from any other one happened at a moment wall-clock
      # timing chose. Nothing else in a result catches that — the run audits clean
      # and the trace diverges anyway.
      #
      # 2PC has no such process, so this asserts the clean case. The counter
      # earning its keep is the point: a harness that acquires one finds out.
      result = run(%{seed: 5, net: true})

      assert %{net: %{unmanaged: 0}} = result
      assert :ok = @run.audit(result)
    end
  end

  # ---------------------------------------------------------------------------
  # Link events as part of the schedule
  # ---------------------------------------------------------------------------

  describe "partitions, node kills and monitors under a run" do
    @faults :eta_net_faults
    @fault_opts %{
      max_ops: 30,
      max_steps: 20_000,
      preload: [:eta],
      net: %{policy: %{drop_p: 0.2, delay_p: 0.2, max_delay: 20}}
    }

    @describetag timeout: 300_000

    defp fault_run(seed) do
      @run.run(@faults, Map.merge(@fault_opts, %{seed: seed}))
    end

    test "the same seed produces an identical trace" do
      # The bar every message this feature introduces has to clear. A signal or a
      # synthetic DOWN is part of the schedule, so a fan-out ordered by anything
      # the run did not decide — `ets:match/2`'s row order, a corpse reaped at
      # its own pace — shows up here as two traces from one seed.
      #
      # Ten runs, because an order that is merely usually right passes twice.
      runs = for _ <- 1..10, do: fault_run(4)

      traces = Enum.map(runs, & &1.trace)

      assert length(Enum.uniq(traces)) == 1,
             "one seed produced #{length(Enum.uniq(traces))} distinct traces in 10 runs"

      nets = Enum.map(runs, & &1.net)
      assert length(Enum.uniq(nets)) == 1, "one seed produced distinct network outcomes"

      outcomes = Enum.map(runs, & &1.outcome)
      assert length(Enum.uniq(outcomes)) == 1

      # Non-vacuity, one clause per thing the run claims to exercise.
      net = hd(nets)
      assert net.noconnection > 0, "no monitor was ever severed; the run proves nothing"
      assert net.signalled > 0, "no link event was ever delivered"
      assert net.dropped > 0, "the network lost nothing"
    end

    test "a different seed produces a different one" do
      a = fault_run(4)
      b = fault_run(19)

      assert a.trace != b.trace or a.net != b.net
    end

    test "a recorded trace replays under the same link events" do
      recorded = fault_run(4)

      replayed = @run.replay(@faults, recorded.trace, Map.merge(@fault_opts, %{seed: 4}))

      assert replayed.outcome == recorded.outcome
      assert replayed.net == recorded.net
      assert replayed.skipped == 0
    end

    test "a process on another simulated node never learns the real exit reason" do
      # `check/1` is the assertion — the harness fails the run if any process ever
      # records a DOWN whose reason is not `noconnection` (or `noproc`, for a
      # monitor created on a peer that had already gone). In one VM a killed
      # process yields `killed` to every monitor it has, so this only holds
      # because the remote monitors are retired before the process dies.
      for seed <- [4, 19, 41] do
        result = fault_run(seed)
        assert result.outcome == :ok, "seed #{seed}: #{inspect(result.outcome)}"
      end
    end

    test "a perfect network still delivers the link events" do
      # The events are not faults and must not be subject to the fault policy: a
      # partition on a perfect network still severs monitors and still signals.
      result = @run.run(@faults, Map.merge(@fault_opts, %{seed: 4, net: true}))

      assert result.outcome == :ok
      assert result.net.dropped == 0
      assert result.net.signalled > 0
      assert result.net.noconnection > 0
    end
  end

  describe "delay" do
    test "is virtual, so a slow network costs no wall-clock time" do
      opts = %{
        seed: 3,
        net: %{policy: %{delay_p: 0.5, max_delay: 30_000, scope: {:tags, [:prepare, :vote]}}}
      }

      {micros, result} = :timer.tc(fn -> run(opts) end)

      assert %{outcome: :ok, net: %{delayed: delayed}} = result
      assert delayed > 0, "nothing was delayed; the run proves nothing about delay"

      assert micros < 10_000_000,
             "a run with 30s virtual delays took #{div(micros, 1_000)}ms of real time"

      assert :ok = @run.audit(result)
    end
  end
end
