defmodule DstStrayTest do
  @moduledoc """
  A timer outliving the scheduler's ownership of its process.

  The minimal form of a divergence found in `dgen_registry`, where two runs of one
  seed produced traces identical for hundreds of entries and then differing by a
  single `{clock, 2500}` entry with no step behind it:

      A: step: 5, step: 8,              clock: 8000, step: 7, step: 4
      B: step: 5, step: 8, clock: 6500, clock: 8000, step: 7, step: 4

  `dst_stray` reproduces that shape in eighty lines. See its moduledoc for the
  mechanism.

  **This was a real defect and it is fixed.** The driver now advances only to a
  deadline belonging to a process the scheduler owns and that is still alive; see
  `dst_time:advance_to_next/1`. These tests were written against the broken
  library and asserted the damage, so what they assert now is its absence. The
  reproduction itself is unchanged, which is the point — it still constructs a
  stray timer both ways, and the run no longer cares.

  The reproduction is deliberately **not** a flaky test. In the system it came
  from, whether the stray existed was decided by a real-scheduler race during
  startup, which is what made it take a day to find. Here it is a config key.
  """
  use ExUnit.Case, async: false

  # A system with periodic timers never quiesces, so "nothing runnable and nothing
  # pending" cannot end this run — the workers heartbeat forever. `settle_steps` is
  # what ends it: a fixed number of steps after the last operation is injected.
  # `max_steps` only has to be large enough not to be what stops it first, or the
  # run comes back `{error, step_budget_exhausted}` and the outcome says nothing
  # about the system.
  @opts %{max_ops: 12, max_steps: 20_000, settle_steps: 300, preload: [:dst]}

  @deadline :dst_stray.deadline()

  defp run(seed, stray) do
    :dst_run.run(:dst_stray_harness, Map.merge(@opts, %{seed: seed, config: %{stray: stray}}))
  end

  defp clocks(trace), do: for({:clock, ms} <- trace, do: ms)

  describe "the control" do
    test "a clean run advances the clock only on heartbeats" do
      r = run(1, :none)

      assert r.outcome == :ok
      assert :dst_run.audit(r) == :ok
      assert r.ops == 12
      assert r.stray_timers == 0

      # Non-vacuity for everything below: the driver has to be advancing the clock
      # routinely, or an extra advance would not have been hiding among anything.
      period = :dst_stray.heartbeat()
      assert length(clocks(r.trace)) > 10
      assert Enum.all?(clocks(r.trace), &(rem(&1, period) == 0))
      refute @deadline in clocks(r.trace)
    end
  end

  for mode <- [:dead, :unowned] do
    describe "a #{mode} process's timer" do
      @mode mode

      test "does not change the schedule of a run with the same seed" do
        clean = run(1, :none)
        strayed = run(1, @mode)

        # What the defect cost: these were equal for hundreds of entries and then
        # differed by one inserted advance. A seed has to name the whole
        # execution, and a timer nobody owns is not part of the seed.
        assert clean.trace == strayed.trace
      end

      test "never becomes the deadline the clock advances to" do
        strayed = run(1, @mode)

        refute {:clock, @deadline} in strayed.trace,
               "the clock advanced to a deadline whose owner can never take a step"
      end

      test "leaves the clock advances identical to the clean run" do
        clean = run(1, :none)
        strayed = run(1, @mode)

        assert clocks(strayed.trace) == clocks(clean.trace)
      end

      test "does not stop the clock from passing it" do
        # Skipped, not cancelled. The stray sits at 2500 and the heartbeats carry
        # the clock over it. Refusing to advance *to* a deadline must not turn
        # into refusing to advance *past* it, or a stray would freeze the run.
        strayed = run(1, @mode)

        assert Enum.max(clocks(strayed.trace)) > @deadline
      end

      test "is reported by audit/1" do
        strayed = run(1, @mode)

        # The framework's own statement that a run was deterministic. It used to
        # check only properties of the *schedule* — late adoption, module loads,
        # scheduler timeouts — and a timer nobody owns is none of those.
        assert strayed.stray_timers >= 1
        assert {:suspect, suspect} = :dst_run.audit(strayed)
        assert {:stray_timers, strayed.stray_timers} in suspect

        # And nothing else is wrong with the run, which is what made this so hard
        # to see: every other signal is clean.
        assert strayed.sched.adopted_late == 0
        assert strayed.modules_loaded == []
        assert strayed.outcome == :ok
      end

      test "produces a trace that replays strictly against itself" do
        strayed = run(1, @mode)

        replayed =
          :dst_run.replay(
            :dst_stray_harness,
            strayed.trace,
            Map.merge(@opts, %{seed: 1, config: %{stray: @mode}})
          )

        # Replay has to apply the same schedulability rule the run did. Under a
        # laxer one it would advance to a deadline the run stepped over and report
        # `{:clock_diverged, 3000, 2500}` against a trace that is perfectly good.
        assert replayed.outcome == :ok
        assert replayed.skipped == 0
        assert replayed.trace == strayed.trace
      end
    end
  end
end
