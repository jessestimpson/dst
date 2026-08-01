defmodule DstHrlTest do
  @moduledoc """
  `include/eta.hrl` — the compile-time contract.

  `eta` is a test-only dependency, so a shipped module holding a bare
  `eta_log:log/1` call is an `undef` waiting for the first production run. That
  is what makes instrumentation something people strip out again, and stripping
  it out is how a system ends up unreadable when it fails.

  The header removes the calls instead. Under `-D DST` the macros expand to
  `eta_log` and the parse transform is applied; without it they expand to
  nothing and the module has no relationship to this library at all. Same shape
  as EUnit's `TEST`.

  These tests read the compiled beams rather than trusting that claim.
  """
  use ExUnit.Case, async: true

  defp compile(module, opts) do
    src =
      [__DIR__, "support", "#{module}.erl"]
      |> Path.join()
      |> String.to_charlist()

    assert {:ok, ^module, bin} = :compile.file(src, [:binary, :return_errors | opts])
    bin
  end

  # Every module this beam calls into.
  defp calls(bin) do
    {:ok, {_mod, [imports: imports]}} = :beam_lib.chunks(bin, [:imports])
    imports |> Enum.map(&elem(&1, 0)) |> Enum.uniq()
  end

  describe "a release build" do
    test "has no dependency on eta whatsoever" do
      for module <- [:eta_2pc_participant, :eta_2pc_coordinator] do
        called = calls(compile(module, []))

        refute :eta_log in called,
               "#{module} still calls eta_log without the DST define"

        refute :eta_time in called,
               "#{module} still calls eta_time without the DST define"

        refute :eta_sched in called,
               "#{module} still calls eta_sched without the DST define"
      end
    end

    test "keeps the real erlang calls the transform would have rewritten" do
      # The coordinator's prepare timeout. Untransformed it is an ordinary
      # `erlang:send_after/3`, which is exactly what should ship.
      assert :erlang in calls(compile(:eta_2pc_coordinator, []))
    end

    test "compiles without warnings, so the macros do not strand variables" do
      # An argument that appears *only* inside `?ETA_LOG` becomes unused once the
      # macro expands to nothing. That is documented in the header, and this is
      # the check that the shipped example does not fall into it.
      src =
        [__DIR__, "support", "eta_2pc_participant.erl"]
        |> Path.join()
        |> String.to_charlist()

      assert {:ok, :eta_2pc_participant, _bin, []} =
               :compile.file(src, [:binary, :return_errors, :return_warnings])
    end
  end

  describe "a simulation build" do
    test "wires up the log" do
      called = calls(compile(:eta_2pc_participant, [{:d, :DST}]))

      assert :eta_log in called, "?ETA_LOG did not expand under the DST define"
    end

    test "applies the parse transform the header carries" do
      # Nothing in the coordinator's source names `eta_transform`. Including the
      # header is the entire opt-in, and this is the evidence: its
      # `erlang:send_after/3` has become `eta_time:send_after/3`.
      called = calls(compile(:eta_2pc_coordinator, [{:d, :DST}]))

      assert :eta_time in called,
             "the transform did not run, so the prepare timeout is on the real clock"
    end
  end
end
