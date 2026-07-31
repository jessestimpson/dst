defmodule DstLogTest do
  @moduledoc """
  `dst_log` — the record of what the *system* did, alongside what the scheduler
  chose.

  A trace on its own is a scheduler artifact. `{step, 8}` names a process by a
  position assigned at registration time and says nothing about what that
  process did, so turning one into an explanation means reconstructing every
  mailbox state by hand. These tests cover the three phases that fix that, and
  the two properties the whole thing rests on:

  - **it must not perturb the schedule**, or every failure you investigate is a
    different failure from the one you set out to investigate;
  - **the data must outlive the run**, because a log deleted at teardown is
    unreadable exactly when you want to read it.
  """
  use ExUnit.Case, async: false

  @harness :dst_2pc
  @opts %{max_ops: 8, max_steps: 20_000}

  setup do
    on_exit(&:dst_log.stop/0)
    :ok
  end

  describe "collection" do
    test "is on by default and records both streams" do
      :dst_run.run(@harness, Map.put(@opts, :seed, 1))

      roles = for %{role: r} <- :dst_log.profile(), do: r

      assert :"$dst" in roles, "the driver's own decisions are missing"
      assert Enum.any?(roles, &match?({:participant, _}, &1)), "the system's events are missing"
    end

    test "log => false collects nothing" do
      :dst_run.run(@harness, @opts |> Map.put(:seed, 1) |> Map.put(:log, false))

      refute :dst_log.running()
      assert :dst_log.profile() == []
    end

    test "log/1 is inert with no collection running, so a system can stay instrumented" do
      :dst_log.stop()

      assert :dst_log.log(:anything) == 0
      assert :dst_log.seq() == 0
    end

    test "starting a run discards the previous one" do
      :dst_run.run(@harness, Map.put(@opts, :seed, 1))
      first = :dst_log.seq()
      assert first > 0

      :dst_run.run(@harness, Map.put(@opts, :seed, 2))
      entries = :dst_log.profile()

      assert [%{seq: 1} | _] = entries, "sequence numbers restart with the run"
    end
  end

  describe "the data outlives the run" do
    test "the log is readable after terminate/1 has run" do
      :dst_run.run(@harness, Map.put(@opts, :seed, 1))

      assert :dst_log.running()
      assert length(:dst_log.profile()) > 0
    end

    test "stop/0 discards it, and is safe to call twice" do
      :dst_run.run(@harness, Map.put(@opts, :seed, 1))

      assert :dst_log.stop() == :ok
      assert :dst_log.stop() == :ok
      refute :dst_log.running()
      assert :dst_log.events() == []
    end
  end

  describe "correlation" do
    test "every system event is attributed to the step it happened inside" do
      :dst_run.run(@harness, Map.put(@opts, :seed, 1))

      # The first entry is the driver's, before any step has been chosen, so it
      # belongs to no step. Everything after the first `{step, _}` does.
      [first | rest] = :dst_log.profile()
      assert first.step == :undefined

      after_first_step =
        rest
        |> Enum.drop_while(&(&1.step == :undefined))

      assert after_first_step != []
      assert Enum.all?(after_first_step, &is_integer(&1.step))
    end

    test "the boundary between the run and its teardown is marked" do
      :dst_run.run(@harness, Map.put(@opts, :seed, 1))

      profile = :dst_log.profile()
      simulated = Enum.filter(profile, & &1.simulated)
      teardown = Enum.reject(profile, & &1.simulated)

      # `dst_run` releases every suspended process before tearing the system
      # down, so whatever those processes had left to do runs on the real
      # scheduler. It is real, it is nondeterministic, and it is not the run.
      assert simulated != []
      assert teardown != []

      assert Enum.max(Enum.map(simulated, & &1.seq)) <
               Enum.min(Enum.map(teardown, & &1.seq)),
             "teardown events must all follow the release marker"
    end

    test "label/2 names the processes a step chose" do
      :dst_run.run(@harness, Map.put(@opts, :seed, 1))

      names = step_names()

      assert Enum.all?(names, &is_binary/1), "names are binaries, never charlists"
      assert "coordinator" in names
      assert Enum.any?(names, &String.starts_with?(&1, "participant-"))

      refute Enum.any?(names, &Regex.match?(~r/^p\d+$/, &1)),
             "every process this harness creates should be named by label/2"
    end

    test "unnamed processes fall back to positional names" do
      # Driven by hand rather than through a run, which is both the tighter test
      # and the only coverage of the manual `trace/1` path.
      :ok = :dst_log.trace(:start)
      :ok = :dst_log.role(:"$dst")
      :dst_log.log({:step, 0})
      :dst_log.log({:step, 7})

      assert step_names() == ["p0", "p7"]
    end

    test "a harness that never defines label/2 still profiles" do
      # `dst_blocking_check_sut` implements the six required callbacks and not
      # the optional one, which is the case every adopter starts from. It also
      # fails in `check/1` before a single step runs, so this is the degenerate
      # case: a log with no steps in it at all must not break correlation.
      assert %{outcome: {:error, :check_blocked}} =
               :dst_run.run(:dst_blocking_check_sut, %{seed: 1, max_ops: 1, max_steps: 200})

      assert step_names() == []
      assert is_list(:dst_log.profile())
    end
  end

  defp step_names do
    for %{role: :"$dst", what: {:step, _}, name: n} <- :dst_log.profile(), do: n
  end

  describe "presentation" do
    test "until stops where the story does" do
      :dst_run.run(@harness, Map.put(@opts, :seed, 1))

      path = Path.join(System.tmp_dir!(), "dst_log_until.txt")
      :ok = :dst_log.analyze(%{until: 5, dest: path})

      lines = path |> File.read!() |> String.split("\n", trim: true)
      assert length(lines) == 5
      File.rm!(path)
    end

    test "driver => false leaves only what the system did" do
      :dst_run.run(@harness, Map.put(@opts, :seed, 1))

      path = Path.join(System.tmp_dir!(), "dst_log_system.txt")
      :ok = :dst_log.analyze(%{driver: false, dest: path})

      text = File.read!(path)
      refute text =~ "$dst"
      assert text =~ "coordinator"
      File.rm!(path)
    end
  end

  describe "the property everything else rests on" do
    test "collecting the log does not change the schedule" do
      opts = Map.put(@opts, :seed, 3)

      %{trace: with_log} = :dst_run.run(@harness, opts)
      %{trace: without_log} = :dst_run.run(@harness, Map.put(opts, :log, false))

      # ETS operations do not block, and a step ends when a process blocks in a
      # receive, so logging cannot move a step boundary. That is the argument;
      # this is the evidence. Observability that changed the schedule would be
      # worse than none.
      assert with_log == without_log
    end

    test "a seed still reproduces itself with collection on" do
      opts = Map.put(@opts, :seed, 5)
      traces = for _ <- 1..3, do: :dst_run.run(@harness, opts).trace

      assert length(Enum.uniq(traces)) == 1
    end
  end
end
