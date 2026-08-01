defmodule DstFixtureTest do
  @moduledoc """
  Trace fixtures — pinning a reproduction to disk instead of to a seed.

  A seed names a schedule only in the context of a particular `generate/2`, so a
  seed-pinned regression test is coupled to the workload generator. Change the
  operation mix and every pinned seed quietly starts testing something else; a
  test asserting on a violation fails and you find out, while one asserting `ok`
  goes vacuous in silence.

  `replay/3` never calls `generate/2`, so a trace has no such coupling. These
  tests pin that difference down rather than asserting it.
  """
  use ExUnit.Case, async: false

  @harness :eta_2pc
  @buggy %{max_ops: 25, max_steps: 20_000, config: %{mode: :first_vote_wins}}

  setup do
    dir = Path.join(System.tmp_dir!(), "eta_fixture_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    {:ok, path: Path.join(dir, "atomicity.eta")}
  end

  # A shrunk, verified reproduction of the planted 2PC defect.
  defp repro do
    seed =
      Enum.find(1..60, fn s ->
        match?({:violation, _}, :eta_run.run(@harness, Map.put(@buggy, :seed, s)).outcome)
      end)

    assert seed, "no seed reached the planted defect"
    opts = Map.put(@buggy, :seed, seed)

    %{trace: trace, verified: true} =
      :eta_shrink.shrink(@harness, :eta_run.run(@harness, opts).trace, opts)

    {trace, opts}
  end

  describe "saving" do
    test "writes a fixture that carries its harness and options", %{path: path} do
      {trace, opts} = repro()

      assert {:ok, {:violation, %{property: :atomicity}}} =
               :eta_run.save_fixture(path, @harness, trace, opts)

      fixture = :eta_run.load_fixture(path)

      assert fixture.harness == @harness
      assert fixture.trace == trace
      # The options matter as much as the trace. A reproduction that needs
      # `mode: :first_vote_wins` and is replayed without it does not reproduce.
      assert fixture.opts.config == %{mode: :first_vote_wins}
      assert {:violation, _} = fixture.outcome
    end

    test "refuses a trace that does not replay", %{path: path} do
      {trace, opts} = repro()

      # Reversed, so the recorded steps name processes that are not runnable.
      assert {:error, {:did_not_replay, {:diverged, _, _}}} =
               :eta_run.save_fixture(path, @harness, Enum.reverse(trace), opts)

      refute File.exists?(path),
             "a fixture that does not reproduce must not reach disk"
    end
  end

  describe "replaying" do
    test "a test names a file and nothing else", %{path: path} do
      {trace, opts} = repro()
      {:ok, _} = :eta_run.save_fixture(path, @harness, trace, opts)

      assert %{outcome: {:violation, %{property: :atomicity}}} =
               :eta_run.replay_fixture(path)

      assert :eta_run.check_fixture(path) == :ok
    end

    test "the trace carries the repro, not the seed", %{path: path} do
      # The claim that makes fixtures worth having. Replay under a completely
      # different seed and the failure is still there, because nothing in a
      # replay is drawn from the seed.
      {trace, opts} = repro()
      {:ok, expected} = :eta_run.save_fixture(path, @harness, trace, opts)

      elsewhere = Map.put(opts, :seed, opts.seed + 9_999)

      assert %{outcome: ^expected} = :eta_run.replay(@harness, trace, elsewhere)
    end

    test "dropping the config it was saved with breaks it", %{path: path} do
      # Why the fixture carries the options. Against a *correct* coordinator the
      # recorded schedule is not even a valid one, which is a divergence rather
      # than a clean run — the same reason you cannot write the mirror test.
      {trace, opts} = repro()
      {:ok, _} = :eta_run.save_fixture(path, @harness, trace, opts)

      %{outcome: outcome} = :eta_run.replay(@harness, trace, Map.delete(opts, :config))

      refute match?({:violation, _}, outcome),
             "the defect reproduced without the config that plants it"
    end
  end

  describe "rejecting a stale file" do
    test "an unreadable term is named, not badmatched", %{path: path} do
      File.write!(path, :erlang.term_to_binary(%{version: 999}))

      assert_raise ErlangError, fn -> :eta_run.load_fixture(path) end
    end
  end
end
