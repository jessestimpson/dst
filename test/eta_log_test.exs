defmodule DstLogTest do
  @moduledoc """
  `eta_log` — the record of what the *system* did, alongside what the scheduler
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

  @harness :eta_2pc
  @opts %{max_ops: 8, max_steps: 20_000}

  setup do
    on_exit(&:eta_log.stop/0)
    :ok
  end

  describe "collection" do
    test "is on by default and records both streams" do
      :eta_run.run(@harness, Map.put(@opts, :seed, 1))

      labels = for %{label: r} <- :eta_log.profile(), do: r

      assert :"$eta" in labels, "the driver's own decisions are missing"
      assert Enum.any?(labels, &match?({:participant, _}, &1)), "the system's events are missing"
    end

    test "log => false collects nothing but keeps the clock" do
      :eta_run.run(@harness, @opts |> Map.put(:seed, 1) |> Map.put(:log, false))

      assert :eta_log.profile() == [], "events were recorded despite log => false"

      # The counter has to survive, and this is not a nicety. A harness stamps
      # its operations from `log/1` so that a violation indexes into the
      # narrative. If those stamps all read 0, an invariant phrased as "this
      # finished before that started" compares 0 < 0, holds for no pair, and
      # stops checking anything — a vacuous run reported as a clean pass.
      assert :eta_log.seq() > 0, "the sequence counter stopped with the recording"
    end

    test "stamps keep increasing with recording off" do
      :ok = :eta_log.trace(:stamps)

      first = :eta_log.log(:a)
      second = :eta_log.log(:b)

      assert is_integer(first) and first > 0
      assert second == first + 1
      assert :eta_log.events() == []
    end

    test "log/1 is inert with no collection running, so a system can stay instrumented" do
      :eta_log.stop()

      assert :eta_log.log(:anything) == 0
      assert :eta_log.seq() == 0
    end

    test "starting a run discards the previous one" do
      :eta_run.run(@harness, Map.put(@opts, :seed, 1))
      first = :eta_log.seq()
      assert first > 0

      :eta_run.run(@harness, Map.put(@opts, :seed, 2))
      entries = :eta_log.profile()

      assert [%{seq: 1} | _] = entries, "sequence numbers restart with the run"
    end
  end

  describe "the data outlives the run" do
    test "the log is readable after terminate/1 has run" do
      :eta_run.run(@harness, Map.put(@opts, :seed, 1))

      assert :eta_log.running()
      assert length(:eta_log.profile()) > 0
    end

    test "stop/0 discards it, and is safe to call twice" do
      :eta_run.run(@harness, Map.put(@opts, :seed, 1))

      assert :eta_log.stop() == :ok
      assert :eta_log.stop() == :ok
      refute :eta_log.running()
      assert :eta_log.events() == []
    end
  end

  describe "correlation" do
    test "every system event is attributed to the step it happened inside" do
      :eta_run.run(@harness, Map.put(@opts, :seed, 1))

      # The first entry is the driver's, before any step has been chosen, so it
      # belongs to no step. Everything after the first `{step, _}` does.
      [first | rest] = :eta_log.profile()
      assert first.step == :undefined

      after_first_step =
        rest
        |> Enum.drop_while(&(&1.step == :undefined))

      assert after_first_step != []
      assert Enum.all?(after_first_step, &is_integer(&1.step))
    end

    test "the boundary between the run and its teardown is marked" do
      :eta_run.run(@harness, Map.put(@opts, :seed, 1))

      profile = :eta_log.profile()
      simulated = Enum.filter(profile, & &1.simulated)
      teardown = Enum.reject(profile, & &1.simulated)

      # `eta_run` releases every suspended process before tearing the system
      # down, so whatever those processes had left to do runs on the real
      # scheduler. It is real, it is nondeterministic, and it is not the run.
      assert simulated != []
      assert teardown != []

      assert Enum.max(Enum.map(simulated, & &1.seq)) <
               Enum.min(Enum.map(teardown, & &1.seq)),
             "teardown events must all follow the release marker"
    end

    test "labels/1 names the processes a step chose" do
      :eta_run.run(@harness, Map.put(@opts, :seed, 1))

      names = step_names()

      assert Enum.all?(names, &is_binary/1), "names are binaries, never charlists"
      assert "coordinator" in names
      assert Enum.any?(names, &String.starts_with?(&1, "participant-"))

      refute Enum.any?(names, &Regex.match?(~r/^p\d+$/, &1)),
             "every process this harness creates should be named by labels/1"
    end

    test "unnamed processes fall back to positional names" do
      # Driven by hand rather than through a run, which is both the tighter test
      # and the only coverage of the manual `trace/1` path.
      :ok = :eta_log.trace(:start)
      :ok = :eta_log.label(:"$eta")
      :eta_log.log({:step, 0})
      :eta_log.log({:step, 7})

      assert step_names() == ["p0", "p7"]
    end

    test "a process that labels itself needs no harness callback" do
      :ok = :eta_log.trace(:start)
      parent = self()

      pid =
        spawn(fn ->
          :ok = :eta_log.label({:worker, 1})
          send(parent, :labelled)
          receive do: (:stop -> :ok)
        end)

      assert_receive :labelled
      assert :eta_log.self_labels() == %{pid => {:worker, 1}}
      send(pid, :stop)
    end

    test "self-reported and harness-supplied names merge" do
      # The discriminating case, because 2PC uses both. Participants and the
      # coordinator include the header and name themselves; clients are
      # anonymous funs handed to `spawn_op/1`, so only `labels/1` knows them.
      :eta_run.run(@harness, Map.put(@opts, :seed, 1))

      self_reported = Map.values(:eta_log.self_labels())

      assert :coordinator in self_reported
      assert Enum.any?(self_reported, &match?({:participant, _}, &1))

      refute Enum.any?(self_reported, &match?({:client, _}, &1)),
             "clients cannot label themselves, which is what `labels/1` is for"

      assert Enum.any?(step_names(), &String.starts_with?(&1, "client-")),
             "the harness's names must survive the merge"
    end

    test "a label is permanent, and a relabel is refused loudly" do
      :ok = :eta_log.trace(:start)

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          :ok = :eta_log.label(:follower)
          :ok = :eta_log.label(:leader)
        end)

      assert log =~ "already labelled"
      :eta_log.log(:promoted)

      assert [%{label: :follower}] = :eta_log.profile()

      assert :eta_log.self_labels() == %{self() => :follower},
             "the first label is the one that reaches the step lines"
    end

    test "a harness that never defines labels/1 still profiles" do
      # `eta_blocking_check_sut` implements the six required callbacks and not
      # the optional one, which is the case every adopter starts from. It also
      # fails in `check/1` before a single step runs, so this is the degenerate
      # case: a log with no steps in it at all must not break correlation.
      assert %{outcome: {:error, :check_blocked}} =
               :eta_run.run(:eta_blocking_check_sut, %{seed: 1, max_ops: 1, max_steps: 200})

      assert step_names() == []
      assert is_list(:eta_log.profile())
    end
  end

  defp step_names do
    for %{label: :"$eta", what: {:step, _}, name: n} <- :eta_log.profile(), do: n
  end

  describe "presentation" do
    test "until stops where the story does" do
      :eta_run.run(@harness, Map.put(@opts, :seed, 1))

      path = Path.join(System.tmp_dir!(), "eta_log_until.txt")
      :ok = :eta_log.analyze(%{until: 5, dest: path})

      lines = path |> File.read!() |> String.split("\n", trim: true)
      assert length(lines) == 5
      File.rm!(path)
    end

    test "driver => false leaves only what the system did" do
      :eta_run.run(@harness, Map.put(@opts, :seed, 1))

      path = Path.join(System.tmp_dir!(), "eta_log_system.txt")
      :ok = :eta_log.analyze(%{driver: false, dest: path})

      text = File.read!(path)
      refute text =~ "$eta"
      assert text =~ "coordinator"
      File.rm!(path)
    end
  end

  describe "the property everything else rests on" do
    test "collecting the log does not change the schedule" do
      opts = Map.put(@opts, :seed, 3)

      %{trace: with_log} = :eta_run.run(@harness, opts)
      %{trace: without_log} = :eta_run.run(@harness, Map.put(opts, :log, false))

      # ETS operations do not block, and a step ends when a process blocks in a
      # receive, so logging cannot move a step boundary. That is the argument;
      # this is the evidence. Observability that changed the schedule would be
      # worse than none.
      assert with_log == without_log
    end

    test "a seed still reproduces itself with collection on" do
      opts = Map.put(@opts, :seed, 5)
      traces = for _ <- 1..3, do: :eta_run.run(@harness, opts).trace

      assert length(Enum.uniq(traces)) == 1
    end
  end
end
