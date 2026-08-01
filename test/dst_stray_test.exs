defmodule DstStrayTest do
  @moduledoc """
  A timer outliving the scheduler's ownership of its process.

  The minimal form of a divergence found in `dgen_registry`, where two runs of one
  seed produced traces identical for hundreds of entries and then differing by a
  single `{clock, 2500}` entry with no step behind it:

      A: step: 5, step: 8,              clock: 8000, step: 7, step: 4
      B: step: 5, step: 8, clock: 6500, clock: 8000, step: 7, step: 4

  `dst_stray` reproduces that shape in eighty lines. See its moduledoc for the
  mechanism; this file states what it costs.

  The reproduction is deliberately **not** a flaky test. In the system it came
  from, whether the stray existed was decided by a real-scheduler race during
  startup, which is what made it take a day to find. Here it is a config key. That
  makes the same claim — a run's schedule depends on something that is not the
  seed — while failing the same way every time.
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

      # Non-vacuity for everything below: the driver has to be advancing the clock
      # routinely, or an extra advance would not be hiding among anything.
      period = :dst_stray.heartbeat()
      assert length(clocks(r.trace)) > 10
      assert Enum.all?(clocks(r.trace), &(rem(&1, period) == 0))
      refute @deadline in clocks(r.trace)
    end
  end

  for mode <- [:dead, :unowned] do
    describe "a #{mode} process's timer" do
      @mode mode

      test "diverges the schedule of a run with the same seed" do
        clean = run(1, :none)
        strayed = run(1, @mode)

        refute clean.trace == strayed.trace,
               "the stray made no difference, so this no longer reproduces anything"
      end

      test "stays invisible until it becomes the earliest deadline" do
        clean = run(1, :none)
        strayed = run(1, @mode)

        prefix = Enum.find_index(Enum.zip(clean.trace, strayed.trace), fn {a, b} -> a != b end)

        # The property that made the original expensive. A stray deadline changes
        # nothing while some other timer is due sooner, so the two runs agree for
        # a long time and then stop — which reads like a bug in whatever the system
        # happened to be doing at that entry, and is not.
        assert prefix > 20,
               "diverged at entry #{prefix}; the stray should sit in the wheel doing " <>
                 "nothing until the clock reaches it"
      end

      test "inserts exactly one advance and changes nothing else" do
        clean = run(1, :none)
        strayed = run(1, @mode)

        assert clocks(strayed.trace) -- clocks(clean.trace) == [@deadline]

        # The whole defect in one line: the strayed run *is* the clean run with a
        # single entry inserted. Nothing the system computed differed. Both traces
        # are truncated at the step budget, so the comparison runs to the shorter.
        deleted = List.delete(strayed.trace, {:clock, @deadline})
        n = min(length(deleted), length(clean.trace))

        assert Enum.take(deleted, n) == Enum.take(clean.trace, n)
      end

      test "advances time on behalf of a process that can never take a step" do
        strayed = run(1, @mode)
        i = Enum.find_index(strayed.trace, &(&1 == {:clock, @deadline}))

        # Every `{step, _}` names a process the scheduler owns. The stray is not
        # one — dead in one mode, never adopted in the other — so nothing can
        # follow its deadline. An advance that makes nothing runnable is the
        # signature to recognise in a real trace.
        refute match?({:step, _}, Enum.at(strayed.trace, i + 1)),
               "something took a step after the stray fired: " <>
                 inspect(Enum.slice(strayed.trace, i..(i + 2)))
      end

      test "is invisible to audit/1" do
        strayed = run(1, @mode)

        # `audit/1` is the framework's own statement that a run was deterministic,
        # and it is the right thing to assert in a suite. It checks late adoption,
        # module loads and scheduler timeouts — every one of them a property of the
        # *schedule*. A timer nobody owns is none of those.
        #
        # If this test ever fails, the library grew a check for this and the news
        # is good: delete the test.
        assert :dst_run.audit(strayed) == :ok
        assert strayed.sched.adopted_late == 0
        assert strayed.modules_loaded == []
      end

      test "produces a trace that replays strictly against itself" do
        strayed = run(1, @mode)

        replayed =
          :dst_run.replay(
            :dst_stray_harness,
            strayed.trace,
            Map.merge(@opts, %{seed: 1, config: %{stray: @mode}})
          )

        # Self-consistent, which is the last reason this survives: there is no
        # check the framework performs that a strayed run fails. It is only wrong
        # relative to a run whose wheel was different, and nothing compares those.
        assert replayed.outcome == :ok
        assert replayed.skipped == 0
        assert replayed.trace == strayed.trace
      end
    end
  end
end
