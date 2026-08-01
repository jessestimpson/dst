defmodule DstExclusiveTest do
  @moduledoc """
  One clock, one log and one scheduler per VM, enforced rather than documented.

  All three keep their state in **named** ETS tables, so there is exactly one of
  each in a VM and a run has to have it to itself. `eta_time:start/1` used to open
  with an unconditional `stop()`, which meant a second concurrent run silently
  destroyed the first's tables — and the first then died on its next clock read
  with an ETS `badarg` in a stack that named nothing to do with the cause.

  Measured before the guard existed:

      owner started clock, pending = 1
      second process started a clock: pending = 0, now = 500
      first run's timer survived? pending = undefined
      ** (ArgumentError) the table identifier does not refer to an existing ETS table

  A named table's owner is alive by construction, since the VM destroys the table
  when the owner exits. So the table *is* the lock and there is no liveness race
  to lose.
  """
  use ExUnit.Case, async: false

  # Runs `fun` in another process and reports how it ended, so a refusal can be
  # asserted on rather than crashing this test.
  defp elsewhere(fun) do
    parent = self()

    spawn(fn ->
      # `eta_sched:new/1` links, so a scheduler that refuses in `init/1` would
      # take this process with it before it could report anything.
      Process.flag(:trap_exit, true)

      result =
        try do
          {:ok, fun.()}
        catch
          kind, reason -> {kind, reason}
        end

      send(parent, {:result, result})
    end)

    receive do
      {:result, r} -> r
    after
      2000 -> flunk("the other process never answered")
    end
  end

  describe "eta_time" do
    setup do
      :ok = :eta_time.start(%{start_ms: 0})
      on_exit(&:eta_time.stop/0)
    end

    test "refuses a second owner, and names the one it has" do
      assert {:error, {:eta_time, {:clock_in_use, owner, hint}}} =
               elsewhere(fn -> :eta_time.start(%{start_ms: 500}) end)

      assert owner == self()
      assert is_binary(hint), "hints are binaries, never charlists"
    end

    test "leaves the running clock untouched when it refuses" do
      :eta_time.send_after(1000, self(), :mine)

      elsewhere(fn -> :eta_time.start(%{start_ms: 500}) end)

      # The whole point. A refusal that still clobbered the tables would be no
      # better than the silent version.
      assert :eta_time.pending() == 1
      assert :eta_time.now_ms() == 0
    end

    test "the owner may restart its own clock" do
      :eta_time.send_after(1000, self(), :mine)
      assert :ok = :eta_time.start(%{start_ms: 0})
      assert :eta_time.pending() == 0, "a restart is still a reset"
    end
  end

  describe "eta_log" do
    setup do
      :ok = :eta_log.trace(:start)
      on_exit(&:eta_log.stop/0)
    end

    test "refuses a second owner" do
      assert {:error, {:eta_log, {:log_in_use, owner, hint}}} =
               elsewhere(fn -> :eta_log.trace(:start) end)

      assert owner == self()
      assert is_binary(hint)
    end

    test "leaves the collected events alone" do
      :eta_log.log(:before)
      elsewhere(fn -> :eta_log.trace(:stamps) end)
      :eta_log.log(:after)

      assert length(:eta_log.events()) == 2
    end
  end

  describe "eta_sched" do
    test "refuses a second scheduler while one is active" do
      sched = :eta_sched.new(%{seed: 1})

      try do
        # `new/1` matches on `{ok, Pid}`, so the refusal surfaces through the
        # badmatch rather than as a return value. What matters is that the cause
        # is named in there rather than being a bare `badarg` from `ets:new/2`.
        assert {:error, {:badmatch, {:error, reason}}} =
                 elsewhere(fn -> :eta_sched.new(%{seed: 2}) end)

        assert {{:eta_sched, {:scheduler_in_use, _owner, hint}}, _stack} = reason
        assert is_binary(hint)
      after
        :eta_sched.release(sched)
      end
    end

    test "and allows one once the first has been released" do
      :eta_sched.release(:eta_sched.new(%{seed: 1}))
      second = :eta_sched.new(%{seed: 2})
      assert :eta_sched.release(second) == :ok
    end
  end

  describe "a whole run" do
    test "one of two concurrent runs is refused, and the other is unharmed" do
      opts = %{seed: 1, max_ops: 6, max_steps: 20_000, preload: [:eta]}

      attempt = fn ->
        try do
          {:ok, :eta_run.run(:eta_2pc, opts)}
        catch
          kind, reason -> {kind, reason}
        end
      end

      # Symmetric on purpose. Which side gets the clock first is a real-scheduler
      # race, and an earlier version of this test assumed the test process won —
      # it lost, and failed with the very error it was written to check for.
      task = Task.async(attempt)
      a = attempt.()
      b = Task.await(task, 30_000)

      results = [a, b]
      won = for {:ok, r} <- results, do: r
      lost = results -- Enum.map(won, &{:ok, &1})

      assert length(won) == 1, "both runs proceeded, so the guard did nothing"
      assert length(lost) == 1

      # The winner reaches a normal outcome rather than dying on an ETS badarg
      # from having its tables deleted underneath it.
      #
      # Deliberately no `audit/1`: `Task.async` loads modules while the run is in
      # flight, so `modules_loaded` is legitimately non-empty. That is the same
      # interference this guard exists to surface, and asserting on it here would
      # only make the test flaky.
      assert [%{outcome: :ok}] = won

      # And the loser says which global it collided with, rather than failing
      # somewhere unrelated later on.
      assert [{:error, reason}] = lost
      assert inspect(reason) =~ ~r/clock_in_use|log_in_use|scheduler_in_use/
    end
  end
end
