defmodule DstTimeTest do
  @moduledoc """
  Phase 1 exit criteria for `:eta_time` and `:eta_transform`
  (see docs/design/eta_framework_design.md).

  The bar: a system under test whose timers and clock reads are virtualised runs
  deterministically *and* without wall-clock waiting — a run that would have spent
  seconds asleep finishes in milliseconds.
  """
  use ExUnit.Case, async: false

  @time :eta_time
  @sched :eta_sched
  @sut :eta_timer_sut

  setup do
    on_exit(fn -> @time.stop() end)
    :ok
  end

  defp new_trace, do: :ets.new(:eta_time_trace, [:public, :ordered_set])
  defp events(tab), do: :ets.tab2list(tab) |> Enum.map(&elem(&1, 1))

  # ---------------------------------------------------------------------------
  # The clock itself
  # ---------------------------------------------------------------------------

  describe "virtual clock" do
    test "time does not move on its own" do
      :ok = @time.start(%{start_ms: 0})

      first = @time.monotonic_time(:millisecond)
      Process.sleep(50)
      second = @time.monotonic_time(:millisecond)

      assert first == second,
             "the virtual clock advanced by itself — it must only move when driven"

      assert @time.advance(50) == 0
      assert @time.monotonic_time(:millisecond) == 50
    end

    test "system_time tracks the virtual clock but reads as a plausible wall clock" do
      real_before = System.system_time(:millisecond)
      :ok = @time.start(%{start_ms: 0})

      sys = @time.system_time(:millisecond)

      assert_in_delta sys,
                      real_before,
                      5_000,
                      "system_time should look like a real wall clock, got #{sys}"

      @time.advance(1_000)
      assert @time.system_time(:millisecond) == sys + 1_000
    end

    test "units convert correctly" do
      :ok = @time.start(%{start_ms: 1_500})

      assert @time.monotonic_time(:millisecond) == 1_500
      assert @time.monotonic_time(:second) == 1
      assert @time.monotonic_time(:microsecond) == 1_500_000
    end
  end

  # ---------------------------------------------------------------------------
  # Timers
  # ---------------------------------------------------------------------------

  describe "timers" do
    test "send_after delivers only when the clock reaches the deadline" do
      :ok = @time.start()
      @time.send_after(100, self(), :late)

      assert @time.pending() == 1
      refute_receive :late, 50

      assert @time.advance(99) == 0
      refute_received(:late)

      assert @time.advance(1) == 1
      assert_received :late
      assert @time.pending() == 0
    end

    test "start_timer wraps the message the way erlang does" do
      :ok = @time.start()
      ref = @time.start_timer(10, self(), :payload)

      @time.advance(10)
      assert_received {:timeout, ^ref, :payload}
    end

    test "advance_to_next jumps straight to the earliest deadline" do
      :ok = @time.start()
      @time.send_after(5_000, self(), :far)
      @time.send_after(10, self(), :near)

      assert @time.next_deadline() == 10
      assert @time.advance_to_next() == true
      assert @time.now_ms() == 10
      assert_received :near
      refute_received(:far)

      assert @time.advance_to_next() == true
      assert @time.now_ms() == 5_000
      assert_received :far

      # Nothing pending: the signal a run is over.
      assert @time.advance_to_next() == false
    end

    test "timers sharing a deadline fire in creation order" do
      :ok = @time.start()
      for n <- 1..5, do: @time.send_after(10, self(), {:n, n})

      @time.advance_to_next()

      for n <- 1..5, do: assert_received({:n, ^n})
    end

    test "cancel_timer reports the time remaining and prevents delivery" do
      :ok = @time.start()
      ref = @time.send_after(100, self(), :cancelled)

      @time.advance(30)
      assert @time.cancel_timer(ref) == 70
      assert @time.cancel_timer(ref) == false

      @time.advance(1_000)
      refute_received(:cancelled)
    end

    test "read_timer reports the time remaining without consuming it" do
      :ok = @time.start()
      ref = @time.send_after(100, self(), :msg)

      @time.advance(40)
      assert @time.read_timer(ref) == 60
      assert @time.read_timer(ref) == 60

      @time.advance(60)
      assert_received :msg
      assert @time.read_timer(ref) == false
    end
  end

  # ---------------------------------------------------------------------------
  # Falling back to the real BIFs
  # ---------------------------------------------------------------------------

  describe "inert when no clock is running" do
    # This is what makes the transform safe to leave enabled: a module built with
    # it behaves exactly as before outside a simulation.
    test "clock reads delegate to erlang" do
      refute @time.running()

      a = @time.monotonic_time(:millisecond)
      Process.sleep(20)
      b = @time.monotonic_time(:millisecond)

      assert b > a, "without a virtual clock, time must come from the real one"
    end

    test "timers delegate to erlang and fire on the real clock" do
      refute @time.running()
      @time.send_after(10, self(), :real)
      assert_receive :real, 500
    end
  end

  # ---------------------------------------------------------------------------
  # A transformed module
  # ---------------------------------------------------------------------------

  describe "eta_transform" do
    test "a transformed module's timers and clock reads are virtualised" do
      :ok = @time.start(%{start_ms: 0})
      trace = new_trace()

      pid = @sut.start(trace, 250)
      # Give it a moment to reach its receive; nothing can fire, since only the
      # driver moves the clock.
      Process.sleep(20)

      assert @time.pending() == 1, "the SUT's send_after did not reach eta_time"
      assert events(trace) == []

      @time.advance_to_next()
      Process.sleep(20)

      assert events(trace) == [{:fired, 250}],
             "the SUT observed the wrong elapsed time: #{inspect(events(trace))}"

      Process.exit(pid, :kill)
      :ets.delete(trace)
    end

    test "the module's own clock reads come from the virtual clock" do
      :ok = @time.start(%{start_ms: 0})
      @time.advance(7_000)

      assert @sut.elapsed(0) == 7_000
    end
  end

  # ---------------------------------------------------------------------------
  # Exit criterion — no wall-clock waiting
  # ---------------------------------------------------------------------------

  describe "speed" do
    test "a run spanning simulated hours completes in milliseconds" do
      :ok = @time.start(%{start_ms: 0})
      trace = new_trace()

      # 200 ticks at 30s each: 100 minutes of simulated time. On a real clock this
      # test could not exist.
      #
      # Driven through eta_sched rather than by calling advance_to_next/0 in a
      # loop. Without the scheduler the SUT runs concurrently with the driver, so
      # the clock can reach the next deadline before the SUT has processed the
      # last tick and armed its next timer — the loop then sees nothing pending
      # and stops early. Under the scheduler that race cannot exist: the SUT runs
      # to quiescence before the idle callback is ever consulted.
      interval = 30_000
      count = 200
      pid = @sut.start_periodic(trace, interval, count)
      Process.sleep(20)

      sched = @sched.new(%{seed: 1}) |> @sched.register([pid])

      {micros, sched} =
        :timer.tc(fn ->
          @sched.run(sched, count * 4, fn -> @time.advance_to_next() end)
        end)

      ticks = events(trace)
      assert length(ticks) == count, "expected #{count} ticks, got #{length(ticks)}"

      assert @time.now_ms() == interval * count,
             "simulated time advanced to #{@time.now_ms()}, expected #{interval * count}"

      # The whole point of the phase: 100 minutes of simulated time, in well
      # under a second of real time.
      assert micros < 1_000_000,
             "advancing #{interval * count}ms of simulated time took #{micros}us of real time"

      @sched.release(sched)
      Process.exit(pid, :kill)
      :ets.delete(trace)
    end
  end

  # ---------------------------------------------------------------------------
  # The discrete-event loop
  # ---------------------------------------------------------------------------

  describe "event loop with eta_sched" do
    test "scheduler and clock together drive a timer-driven SUT deterministically" do
      run = fn seed ->
        :ok = @time.start(%{start_ms: 0})
        trace = new_trace()

        pid = @sut.start_periodic(trace, 1_000, 6)
        Process.sleep(20)

        sched = @sched.new(%{seed: seed}) |> @sched.register([pid])
        sched = @sched.run(sched, 500, fn -> @time.advance_to_next() end)

        result = {events(trace), @time.now_ms()}

        @sched.release(sched)
        Process.exit(pid, :kill)
        :ets.delete(trace)
        @time.stop()
        result
      end

      # Same seed, many runs: identical events *and* identical simulated time.
      results = for _ <- 1..25, do: run.(1)

      assert length(Enum.uniq(results)) == 1,
             "the timer-driven run is not deterministic: #{inspect(Enum.uniq(results))}"

      {ticks, elapsed} = hd(results)
      assert length(ticks) == 6
      assert elapsed == 6_000, "expected 6 ticks of 1000ms, clock read #{elapsed}"
    end

    test "the loop ends when neither processes nor timers have work" do
      :ok = @time.start(%{start_ms: 0})
      trace = new_trace()

      pid = @sut.start(trace, 100)
      Process.sleep(20)

      sched = @sched.new(%{seed: 2}) |> @sched.register([pid])
      sched = @sched.run(sched, 500, fn -> @time.advance_to_next() end)

      # One timer, fired; the SUT re-blocks and sets no more, so the run stops
      # rather than spinning on an idle callback that keeps returning true.
      assert events(trace) == [{:fired, 100}]
      assert @time.pending() == 0
      assert @sched.stats(sched).steps < 10

      @sched.release(sched)
      Process.exit(pid, :kill)
      :ets.delete(trace)
    end
  end

  # ---------------------------------------------------------------------------
  # Timer faults
  # ---------------------------------------------------------------------------

  describe "faults" do
    test "drop_p prevents a timer from ever firing, deterministically per seed" do
      fire_counts =
        for _ <- 1..3 do
          :ok = @time.start(%{start_ms: 0, seed: 99, faults: %{drop_p: 0.5}})
          for n <- 1..40, do: @time.send_after(n, self(), {:t, n})
          @time.advance(1_000)
          count = @time.stats().fired
          drained = drain_mailbox(0)
          @time.stop()
          {count, drained}
        end

      assert length(Enum.uniq(fire_counts)) == 1,
             "the same seed produced different drop decisions: #{inspect(fire_counts)}"

      {fired, _} = hd(fire_counts)
      assert fired > 0 and fired < 40, "expected some but not all of 40 timers, got #{fired}"
    end

    test "skew_ms perturbs deadlines without moving them into the past" do
      :ok = @time.start(%{start_ms: 0, seed: 5, faults: %{skew_ms: 20}})

      refs = for _ <- 1..30, do: @time.send_after(50, self(), :x)
      remaining = Enum.map(refs, &@time.read_timer/1)

      assert Enum.all?(remaining, &(&1 >= 30 and &1 <= 70)),
             "skewed deadlines out of the ±20ms band: #{inspect(Enum.min_max(remaining))}"

      assert length(Enum.uniq(remaining)) > 1, "skew produced no variation at all"
      assert Enum.all?(remaining, &(&1 >= 0)), "a deadline was skewed into the past"
    end

    test "a dropped timer is still cancellable and readable" do
      # A dropped timer models a lost message, not a timer that was never set —
      # the API must behave identically until it fails to deliver.
      :ok = @time.start(%{start_ms: 0, seed: 1, faults: %{drop_p: 1.0}})
      ref = @time.send_after(100, self(), :never)

      assert @time.read_timer(ref) == 100
      assert @time.pending() == 1
      assert @time.cancel_timer(ref) == 100

      :ok = @time.stop()
      :ok = @time.start(%{start_ms: 0, seed: 1, faults: %{drop_p: 1.0}})
      _ = @time.send_after(100, self(), :never)
      @time.advance(1_000)

      refute_received(:never)
      assert @time.stats().dropped == 1
    end

    test "drop_p does not reach a deadline armed through arm_after/2" do
      # `drop_p` models a timer lost with the process or connection it belonged
      # to. A receive timeout has neither: a process arms it for itself, and the
      # BEAM does not lose one. Dropping it is not a fault, it is a hang — the
      # waiting process has nothing left that can wake it, and the trace says a
      # timeout never fired rather than that a message was lost.
      :ok = @time.start(%{start_ms: 0, seed: 1, faults: %{drop_p: 1.0}})

      ref = make_ref()
      tref = @time.arm_after(100, ref)
      @time.advance(1_000)

      assert_received {:"$eta_after", ^ref}
      assert @time.stats().dropped == 0

      # Still an ordinary timer in every other way.
      assert @time.cancel_timer(tref) == false
    end

    test "skew_ms still reaches one, which is what breaks a tie between two" do
      # Two timeouts written with the same number are the common case — every
      # `gen_server:call` default is 5000 — and under a virtual clock they land
      # on one deadline and fire in creation order, the same way every run.
      # `skew_ms` is the knob that separates them.
      :ok = @time.start(%{start_ms: 0, seed: 5, faults: %{skew_ms: 20}})

      remaining =
        for _ <- 1..30 do
          @time.read_timer(@time.arm_after(50, make_ref()))
        end

      assert length(Enum.uniq(remaining)) > 1, "skew did not reach an armed deadline"
      assert Enum.all?(remaining, &(&1 >= 30 and &1 <= 70))
    end
  end

  defp drain_mailbox(n) do
    receive do
      {:t, _} -> drain_mailbox(n + 1)
    after
      0 -> n
    end
  end

  describe "schedulable deadlines" do
    setup do
      :ok = :eta_time.start()
      on_exit(&:eta_time.stop/0)
    end

    test "advance_to_next/1 steps over a deadline its predicate rejects" do
      mine = self()
      other = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(other, :kill) end)

      :eta_time.send_after(500, other, :theirs)
      :eta_time.send_after(1000, mine, :mine)

      assert :eta_time.next_deadline() == 500

      only_mine = fn dest -> dest == mine end
      assert :eta_time.next_deadline(only_mine) == 1000

      assert :eta_time.advance_to_next(only_mine)
      assert :eta_time.now_ms() == 1000
      assert_received :mine
    end

    test "stepping over one is not cancelling it" do
      # The rejected timer must still be delivered when the clock passes it for
      # another reason, or refusing to advance *to* a stray would quietly turn
      # into never firing it at all.
      mine = self()
      other = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(other, :kill) end)

      :eta_time.send_after(500, other, :theirs)
      :eta_time.send_after(1000, mine, :mine)

      assert :eta_time.advance_to_next(fn dest -> dest == mine end)

      assert :eta_time.pending() == 0, "the stepped-over timer should have fired on the way"
      assert :eta_time.stats().fired == 2
    end

    test "strays/0 counts each stepped-over timer once" do
      mine = self()
      other = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(other, :kill) end)

      assert :eta_time.strays() == 0

      :eta_time.send_after(500, other, :theirs)
      :eta_time.send_after(1000, mine, :first)
      :eta_time.send_after(2000, mine, :second)

      only_mine = fn dest -> dest == mine end

      # Two advances, and the same stray is the earliest deadline for the first
      # of them only — but the count is by ref, so a stray skipped repeatedly
      # would still read 1.
      assert :eta_time.advance_to_next(only_mine)
      assert :eta_time.advance_to_next(only_mine)

      assert :eta_time.strays() == 1
    end

    test "advance_to_next/0 accepts everything, for driving by hand" do
      other = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(other, :kill) end)

      :eta_time.send_after(500, other, :theirs)

      assert :eta_time.advance_to_next()
      assert :eta_time.now_ms() == 500
      assert :eta_time.strays() == 0
    end
  end
end
