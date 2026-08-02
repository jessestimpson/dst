defmodule EtaLoggerTest do
  @moduledoc """
  `logger` calls from a system under test, routed into `eta_log`.

  A `logger` call hands its event to a handler process the scheduler does not
  own, and under load that handoff turns synchronous — a call into an OTP service
  process, the same class of real-time dependency as `code_server`. So the
  transform points `logger` at `eta_logger`, which records into the run's log
  while a run is collecting and delegates to `logger` when one is not.
  """
  use ExUnit.Case, async: false

  setup do
    on_exit(&:eta_log.stop/0)
    :ok
  end

  defp events, do: for({_seq, _label, what} <- :eta_log.events(), do: what)

  describe "while a run is collecting" do
    setup do
      :ok = :eta_log.trace(:start)
      :ok
    end

    test "a level call becomes an event instead of reaching a handler" do
      :eta_spawn_sut.say("hello")

      assert events() == [{:info, ["hello"]}]
    end

    test "format and arguments are kept as given, not rendered" do
      # Rendering here would run a formatter on the hot path and throw away the
      # structure a reader might want to match on.
      :eta_spawn_sut.say_fmt("~p failed", [:replica_2])

      assert events() == [{:warning, ["~p failed", [:replica_2]]}]
    end

    test "reports and logger:log/2 carry their level" do
      :eta_spawn_sut.say_report(%{what: :broke})
      :eta_spawn_sut.say_level(:debug, "quiet")

      assert events() == [{:error, [%{what: :broke}]}, {:debug, ["quiet"]}]
    end

    test "events share the sequence counter with everything else" do
      # The reason this is worth doing at all: a system's own logging lands in
      # the narrative interleaved with the scheduler's decisions, at a comparable
      # sequence number, rather than scrolling past in a separate stream.
      a = :eta_log.log(:before)
      :eta_spawn_sut.say("middle")
      b = :eta_log.log(:after)

      assert b - a == 2
      assert events() == [:before, {:info, ["middle"]}, :after]
    end

    test "every level is routed" do
      for level <- [:emergency, :alert, :critical, :error, :warning, :notice, :info, :debug] do
        :eta_spawn_sut.say_level(level, "x")
      end

      assert events() == [
               {:emergency, ["x"]},
               {:alert, ["x"]},
               {:critical, ["x"]},
               {:error, ["x"]},
               {:warning, ["x"]},
               {:notice, ["x"]},
               {:info, ["x"]},
               {:debug, ["x"]}
             ]
    end

    test "nothing is filtered by level" do
      # `eta_log` is a record of what happened, not an operator's console.
      # Deciding at record time what a reader will want is how you lose the line
      # that mattered.
      :eta_spawn_sut.say_level(:debug, "the one that mattered")
      assert events() == [{:debug, ["the one that mattered"]}]
    end
  end

  test "with no run in progress it delegates to logger" do
    refute :eta_log.running()

    # Delegating means it reaches the real handler, so this both must not raise
    # and must not collect. `:notice` keeps it off the default console.
    :eta_spawn_sut.say_level(:notice, "straight through")

    assert :eta_log.events() == []
  end
end
