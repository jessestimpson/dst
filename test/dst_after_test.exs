defmodule DstAfterTest do
  @moduledoc """
  `dst_after_transform` — putting `receive ... after` on the virtual clock.

  The framework's plan assumed this could not be done: `after` is a language
  construct rather than a call, so `dst_transform` has nothing to redirect, and
  Phase 5 was scoped around working without it. The assumption was wrong. A
  transform sees `receive Cs after T -> B end` as the five-element abstract form
  and can rewrite it into a receive with no real-time dependence at all.

  These tests are the evidence for that claim, and — more usefully — for the three
  ways the obvious rewrite is wrong. Each of those was found by running it:

  - rewriting `after 0` turns a non-blocking mailbox poll into a blocking wait;
  - disarming after the receive instead of inside each clause destroys tail
    position, so `loop() -> receive ... -> loop() end` grows stack without bound;
  - matching the bound ref in the pattern warns on every rewritten receive.

  The interesting assertions are therefore the boring-looking ones. See
  `docs/design/dst_framework_design.md`.
  """
  use ExUnit.Case, async: false

  @sut :dst_after_sut
  @control :dst_after_control

  # Recompile the SUT from source before anything runs.
  #
  # Not belt and braces — without it these tests are not a regression suite at all.
  # Mix does not treat a `parse_transform` as a compile-time dependency, so editing
  # `dst_after_transform` recompiles only the transform and leaves every module
  # built with it stale. Verified by deliberately reintroducing the tail-position
  # bug: the suite stayed green until `dst_after_sut.erl` was touched by hand, and
  # then reported 200,000 extra words of stack. A transform test that silently
  # exercises yesterday's transform is worse than no test.
  #
  # (The same gap applies to `dst_transform` and `dst_timer_sut`.)
  setup_all do
    src = Path.join([__DIR__, "support", "dst_after_sut.erl"]) |> String.to_charlist()

    case :compile.file(src, [:binary, :return_errors, :debug_info]) do
      {:ok, mod, bin} ->
        :code.purge(mod)
        {:module, ^mod} = :code.load_binary(mod, src, bin)
        :ok

      other ->
        flunk("could not recompile the SUT against the current transform: #{inspect(other)}")
    end
  end

  setup do
    :ok = :dst_time.start()
    on_exit(fn -> :dst_time.stop() end)
    :ok
  end

  # A process running `fun`, reporting its result back here.
  defp proc(fun) do
    parent = self()
    spawn(fn -> send(parent, {:result, self(), fun.()}) end)
  end

  defp await(pid, timeout \\ 2_000) do
    receive do
      {:result, ^pid, r} -> r
    after
      timeout -> :no_answer
    end
  end

  # Wait for the SUT process to reach its receive and arm its virtual timer.
  # Real-time polling, because this is the driver's side of the boundary.
  defp await_armed(deadline \\ 200)
  defp await_armed(0), do: flunk("the SUT never armed a virtual timer")

  defp await_armed(n) do
    if :dst_time.pending() > 0 do
      :ok
    else
      Process.sleep(1)
      await_armed(n - 1)
    end
  end

  defp real_ms(fun) do
    t0 = System.monotonic_time(:millisecond)
    r = fun.()
    {r, System.monotonic_time(:millisecond) - t0}
  end

  describe "the timeout becomes virtual" do
    test "a 60s timeout resolves by advancing the clock, not by waiting" do
      {result, elapsed} =
        real_ms(fn ->
          pid = proc(fn -> @sut.wait(60_000) end)
          await_armed()
          assert :dst_time.next_deadline() == 60_000
          assert :dst_time.advance_to_next()
          await(pid)
        end)

      assert result == :timed_out
      assert :dst_time.now_ms() == 60_000

      # The whole point. Generous bound — the claim is "no real waiting", and
      # 60_000 vs a few ms is not a measurement that needs precision.
      assert elapsed < 1_000,
             "a virtual 60s timeout cost #{elapsed}ms of real time"
    end

    test "without the transform the same receive ignores the clock" do
      pid = proc(fn -> @control.wait(60_000) end)
      Process.sleep(20)

      assert :dst_time.pending() == 0, "an untransformed `after` armed a virtual timer"
      refute :dst_time.advance_to_next(), "there was something to advance to"
      assert await(pid, 100) == :no_answer, "the control resolved without the real clock"

      Process.exit(pid, :kill)
    end
  end

  describe "inertness" do
    test "with no clock running, a rewritten receive keeps real-time semantics" do
      # Same property that makes dst_transform safe to leave on in every build.
      :ok = :dst_time.stop()
      refute :dst_time.running()

      {result, elapsed} = real_ms(fn -> @sut.wait(120) end)

      assert result == :timed_out
      assert elapsed >= 100, "the timeout did not actually elapse (#{elapsed}ms)"
    end
  end

  describe "a message beats the timeout" do
    test "the normal clause wins, the timer is cancelled, and nothing leaks" do
      send(self(), {:msg, :hello})

      assert @sut.wait(60_000) == {:got, :hello}
      assert :dst_time.pending() == 0, "the timer was not cancelled"

      refute_received {:"$dst_after", _}, "a timeout message was left in the mailbox"
    end
  end

  describe "after 0 stays a poll" do
    test "a literal zero neither blocks nor arms a timer" do
      {result, elapsed} = real_ms(fn -> @sut.poll() end)

      assert result == :empty
      assert elapsed < 100, "`after 0` blocked for #{elapsed}ms"
      assert :dst_time.pending() == 0, "`after 0` armed a virtual timer"

      send(self(), {:msg, :queued})
      assert @sut.poll() == {:got, :queued}
    end

    test "a zero the transform cannot see behaves the same" do
      {result, elapsed} = real_ms(fn -> @sut.poll_var(0) end)

      assert result == :empty
      assert elapsed < 100, "a runtime `after 0` blocked for #{elapsed}ms"
      assert :dst_time.pending() == 0

      # Ordering still holds: an already-queued message is preferred over the
      # timeout, because a selective receive scans in arrival order.
      send(self(), {:msg, :queued})
      assert @sut.poll_var(0) == {:got, :queued}
      refute_received {:"$dst_after", _}, "the immediate timeout message leaked"
    end
  end

  describe "tail position is preserved" do
    # The one that is invisible without a comparison, and the one that would make
    # this transform unusable on any long-running process loop.
    test "recursing out of a receive clause stays constant-stack" do
      n = 50_000

      transformed = drive(&@sut.loop/1, n)
      control = drive(&@control.loop/1, n)

      assert transformed == control,
             "the transform cost #{transformed - control} extra words of stack " <>
               "over #{n} iterations — the receive is no longer in tail position"
    end

    defp drive(fun, n) do
      parent = self()

      pid =
        spawn(fn ->
          receive do
            :go -> :ok
          end

          send(parent, {:result, self(), fun.(n)})
        end)

      for i <- 1..n, do: send(pid, {:msg, i})
      send(pid, :go)

      assert stack = await(pid, 30_000)
      assert is_integer(stack), "the loop did not finish: #{inspect(stack)}"
      stack
    end
  end

  describe "under the scheduler" do
    # Everything above drives `advance_to_next/0` by hand, which leaves the claim
    # that this composes with `dst_sched` as inference rather than measurement.
    # It is the composition that matters: a rewritten timeout is only useful if
    # the discrete-event loop resolves it without the driver doing anything.
    test "the event loop resolves a rewritten timeout with no special handling" do
      parent = self()
      pid = spawn(fn -> send(parent, {:result, self(), @sut.wait(60_000)}) end)
      await_armed()

      sched = :dst_sched.new(%{seed: 1}) |> :dst_sched.register([pid])

      {result, elapsed} =
        real_ms(fn ->
          # No runnable process — the SUT is blocked on a timeout — so the loop
          # goes straight to the idle callback, which is the whole mechanism.
          :dst_sched.run(sched, 100, fn -> :dst_time.advance_to_next() end)
          await(pid)
        end)

      steps = :dst_sched.stats(sched).steps
      :dst_sched.release(sched)

      assert result == :timed_out
      assert :dst_time.now_ms() == 60_000, "the clock did not reach the deadline"
      assert elapsed < 1_000, "the scheduler waited #{elapsed}ms of real time"

      # Non-vacuity: the process must actually have been stepped by the scheduler
      # after the timer fired, not have escaped and run on its own.
      assert steps > 0, "the scheduler never stepped the woken process"
    end
  end

  describe "the awkward shapes" do
    test "after infinity arms nothing and still takes a message" do
      pid = proc(fn -> @sut.forever() end)
      Process.sleep(20)

      assert :dst_time.pending() == 0, "`after infinity` armed a timer"
      refute :dst_time.advance_to_next()

      send(pid, {:msg, :woke})
      assert await(pid) == {:got, :woke}
    end

    test "variables bound in the after body are still exported" do
      pid = proc(fn -> @sut.exports(60_000) end)
      await_armed()
      assert :dst_time.advance_to_next()

      assert await(pid) == :defaulted
    end

    test "nested receives are both virtual" do
      {result, elapsed} =
        real_ms(fn ->
          pid = proc(fn -> @sut.nested(30_000, 5_000) end)

          await_armed()
          assert :dst_time.advance_to_next()
          await_armed()
          assert :dst_time.advance_to_next()

          await(pid)
        end)

      assert result == {:outer_timed_out, :inner_timed_out}
      assert :dst_time.now_ms() == 35_000, "the inner timeout did not run on virtual time"
      assert elapsed < 1_000
    end

    test "a non-local exit from the after body propagates" do
      parent = self()
      pid = spawn(fn -> send(parent, {:result, self(), run_escaping()}) end)

      await_armed()
      assert :dst_time.advance_to_next()

      assert await(pid) == {:exited, :deadline}
    end

    defp run_escaping do
      @sut.escaping(60_000)
    catch
      :exit, reason -> {:exited, reason}
    end
  end
end
