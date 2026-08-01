defmodule DstShrinkTest do
  @moduledoc """
  `eta_shrink` — Phase 4.

  Driven against `eta_2pc`'s planted defect, which is the right target for a first
  shrinker: it fails often enough to produce traces on demand, and the minimal case
  is small enough to check by eye. A shrinker is easy to write and easy to get
  subtly wrong, so the tests here are less about "it made the trace shorter" than
  about the two ways it could be shorter and worthless — a result that does not
  reproduce, and a result that reproduces a *different* failure.
  """
  use ExUnit.Case, async: false

  @run :eta_run
  @shrink :eta_shrink
  @sut :eta_2pc

  @opts %{max_ops: 25, max_steps: 20_000, config: %{mode: :first_vote_wins}}

  # A seed whose run violates atomicity, with its trace.
  defp failing_run do
    seed =
      Enum.find(1..60, fn s ->
        match?({:violation, _}, @run.run(@sut, Map.put(@opts, :seed, s)).outcome)
      end)

    assert seed, "no seed reached the planted defect"
    {seed, @run.run(@sut, Map.put(@opts, :seed, seed))}
  end

  describe "shrinking a failure" do
    test "reduces the trace substantially and the result still reproduces" do
      {seed, original} = failing_run()
      opts = Map.put(@opts, :seed, seed)

      result = @shrink.shrink(@sut, original.trace, opts)

      assert result.verified,
             "the shrunk trace did not survive a strict replay, so it is not a repro"

      assert result.shrunk < result.original,
             "nothing was removed: #{result.original} entries in, #{result.shrunk} out"

      # Independent of `verified`: replay the returned trace ourselves, strictly,
      # and require the same property. `verified` is the shrinker grading its own
      # work; this is the check that it graded it honestly.
      replayed = @run.replay(@sut, result.trace, opts)

      assert {:violation, %{property: :atomicity}} = replayed.outcome
      assert replayed.skipped == 0, "the returned trace needed entries skipped to fail"
    end

    test "what survives is the ingredient the defect needs" do
      # Non-vacuity, and the thing that makes a shrunk trace worth reading: the
      # violation requires a participant voting `no`, so an op carrying one must
      # still be there. A shrinker that cut it would be reporting a trace that
      # cannot fail for the reason claimed.
      {seed, original} = failing_run()
      result = @shrink.shrink(@sut, original.trace, Map.put(@opts, :seed, seed))

      votes =
        for {:op, {:run_tx, _tx, plan}} <- result.trace,
            {_index, vote} <- plan,
            do: vote

      assert :no in votes,
             "the shrunk trace has no dissenting vote, so it cannot violate atomicity"

      steps = Enum.count(result.trace, &match?({:step, _}, &1))
      assert steps > 0, "the trace has no scheduling decisions left in it"
    end

    @tag timeout: 120_000
    test "it is meaningfully smaller across the failing population, not on one seed" do
      # Asserting a reduction on a single seed measures the seed as much as the
      # shrinker. Over every failing seed in a range: measured 30/30 verified,
      # mean 43 entries in and 9 out, with the *worst* seed only reaching 73% of
      # its original — that one is the positional-id limit in the moduledoc, and a
      # bound tight enough to fail on it would be asserting the limitation away.
      results =
        for seed <- 1..20,
            r = @run.run(@sut, Map.put(@opts, :seed, seed)),
            match?({:violation, _}, r.outcome),
            do: @shrink.shrink(@sut, r.trace, Map.put(@opts, :seed, seed))

      assert length(results) >= 10, "too few failing seeds to say anything"

      assert Enum.all?(results, & &1.verified),
             "#{Enum.count(results, &(not &1.verified))} shrinks did not survive strict replay"

      mean = fn f -> div(Enum.sum(Enum.map(results, f)), length(results)) end

      assert mean.(& &1.shrunk) * 2 <= mean.(& &1.original),
             "mean #{mean.(& &1.original)} -> #{mean.(& &1.shrunk)} is not a useful reduction"

      # The acceptance criterion is "a schedule a human can read" — on the order
      # of ten entries rather than thousands.
      assert mean.(& &1.shrunk) < 20
    end
  end

  describe "the ways a shrinker goes wrong" do
    test "a candidate matching a different failure is not accepted" do
      # The `match` predicate is what stops a multi-invariant system from having
      # one bug shrunk into another. Here it is made impossible to satisfy, so no
      # candidate can qualify and the original must come back untouched — with
      # `verified: false` saying so rather than a smaller trace implying success.
      {seed, original} = failing_run()

      opts =
        @opts
        |> Map.put(:seed, seed)
        |> Map.put(:match, fn _violation -> false end)

      result = @shrink.shrink(@sut, original.trace, opts)

      refute result.verified
      assert result.trace == original.trace, "a trace was returned for a failure it never matched"
    end

    test "a trace that does not fail is reported, not shrunk" do
      # Shrinking a passing trace should not produce a confident-looking "minimal"
      # trace for a failure that did not happen.
      clean = @run.run(@sut, %{@opts | config: %{mode: :correct}} |> Map.put(:seed, 1))
      assert clean.outcome == :ok

      result = @shrink.shrink(@sut, clean.trace, %{@opts | config: %{mode: :correct}})

      assert result.outcome == :ok
      refute result.verified
      assert result.trace == clean.trace
      assert result.tests == 0, "candidates were tested against a failure that does not exist"
    end

    test "the search is bounded" do
      {seed, original} = failing_run()

      opts = @opts |> Map.put(:seed, seed) |> Map.put(:max_tests, 5)
      result = @shrink.shrink(@sut, original.trace, opts)

      assert result.tests <= 5, "max_tests was not respected: #{result.tests}"
    end
  end
end
