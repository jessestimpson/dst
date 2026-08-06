defmodule EtaSpawnTest do
  @moduledoc """
  Every spawn and start form `eta_transform` rewrites.

  Two properties per form, and they are different claims.

  **Gated under a scheduler.** A child created by a rewritten call must not run
  before the scheduler owns it. The window between "the process exists" and "the
  scheduler suspended it" is where `adopted_late` comes from, and on a system
  that spawns a helper per operation it was measured at 1212 of 1233 processes.

  **Inert without one.** The same module, run with no scheduler, has to behave
  exactly as it did before the transform — including `proc_lib`'s spawns giving
  the child `$ancestors` and `$initial_call`, which is the whole difference
  between `proc_lib:spawn/1` and `erlang:spawn/1`.
  """
  use ExUnit.Case, async: false

  # These tests drive `eta_sched` directly rather than through `eta_run`, so
  # nothing has preloaded the code a gated child reaches — and a gated child
  # reaches plenty of it, because a transformed module's rewritten sends land in
  # `eta_net`.
  #
  # Without this the file fails at most seeds, and always in the first test or
  # two, which is the tell. The first gated child to run is the one that finds
  # `eta_net` cold: it calls `code_server`, `eta_sched` reads that as a process
  # blocked in a receive, nothing else is runnable, and `run/2` returns at what
  # looks exactly like quiescence with the child still parked. The child is
  # released by the code server a moment later, with nothing left to step it.
  #
  # That is the failure `eta_run`'s moduledoc describes under "the worse
  # failure, and why `preload` is on by default", reached from the one direction
  # that has no `modules_loaded` to report it afterwards.
  setup_all do
    :ok = :eta_run.preload([])
  end

  @sut :eta_spawn_sut

  defp pid_of(p) when is_pid(p), do: p
  defp pid_of({p, ref}) when is_pid(p) and is_reference(ref), do: p

  # A scheduler is linked to the test process, so by the time cleanup runs it is
  # usually already gone. `catch_exit/1` reads right and is not: it *requires* an
  # exit, so it fails on the runs where the cleanup wins the race and releases
  # the scheduler cleanly.
  defp quietly(fun) do
    fun.()
  catch
    :exit, _ -> :ok
  end

  describe "inert with no scheduler running" do
    setup do
      refute :eta_sched.active(), "a scheduler leaked from another test"
      :ok
    end

    for f <- [
          :plain,
          :plain_mfa,
          :linked,
          :linked_mfa,
          :monitored,
          :monitored_mfa,
          :opted,
          :opted_mfa,
          :bare,
          :bare_opt,
          :plib,
          :plib_mfa,
          :plib_linked,
          :plib_opted,
          :plib_opted_mfa
        ] do
      @f f
      test "#{f} runs immediately and is an ordinary process" do
        child = pid_of(apply(@sut, @f, [self()]))
        assert_receive {:ran, ^child}, 1000
      end
    end

    test "proc_lib's spawns still make an OTP process" do
      # The reason `proc_lib` gets its own rewrite targets. Without these two
      # dictionary entries the child is not an OTP process: no crash-report
      # formatting, and `sys` cannot introspect it.
      parent = self()

      pid =
        :eta_spawn_sut.plib(parent)

      assert_receive {:ran, ^pid}, 1000

      # Prove the distinction is real by checking the plain spawn does *not* get
      # them, using a child that stays alive long enough to be inspected.
      plain = :erlang.spawn(fn -> Process.sleep(200) end)
      plib = :proc_lib.spawn(fn -> Process.sleep(200) end)

      {:dictionary, plain_d} = Process.info(plain, :dictionary)
      {:dictionary, plib_d} = Process.info(plib, :dictionary)

      refute Keyword.has_key?(plain_d, :"$ancestors")
      assert Keyword.has_key?(plib_d, :"$ancestors")
      assert Keyword.has_key?(plib_d, :"$initial_call")
    end
  end

  describe "gated under a scheduler" do
    setup do
      sched = :eta_sched.new(%{seed: 1})
      on_exit(fn -> quietly(fn -> :eta_sched.release(sched) end) end)
      {:ok, sched: sched}
    end

    for f <- [
          :plain,
          :plain_mfa,
          :linked,
          :linked_mfa,
          :monitored,
          :monitored_mfa,
          :opted,
          :opted_mfa,
          :bare,
          :bare_opt,
          :plib,
          :plib_mfa,
          :plib_linked,
          :plib_opted,
          :plib_opted_mfa
        ] do
      @f f
      test "#{f} does not run until the scheduler lets it", %{sched: sched} do
        child = pid_of(apply(@sut, @f, [self()]))

        # The claim. It exists, it is alive, and it has done nothing.
        assert Process.alive?(child)
        refute_receive {:ran, ^child}, 50

        # `register/2` is what hands a gated child its token when its parent is
        # not traced, which is exactly this test process.
        sched |> :eta_sched.register([child]) |> :eta_sched.run(200)
        assert_receive {:ran, ^child}, 1000
      end
    end

    test "a gated proc_lib child still gets its dictionary entries", %{sched: sched} do
      child = :eta_spawn_sut.plib_linked(self())
      sched = :eta_sched.register(sched, [child])

      # Still gated, so the dictionary can be inspected before it runs a line.
      {:dictionary, d} = Process.info(child, :dictionary)
      assert d == [], "the entries are set by the child, after its token"

      :eta_sched.run(sched, 200)
      assert_receive {:ran, ^child}, 1000
    end
  end

  describe "the distributed forms" do
    test "raise while a run is active, rather than running locally" do
      sched = :eta_sched.new(%{seed: 1})

      try do
        for f <- [:remote, :remote_mfa, :remote_link, :remote_opt, :plib_remote] do
          assert_raise ErlangError, fn -> apply(:eta_spawn_sut, f, [node(), self()]) end
        end
      after
        :eta_sched.release(sched)
      end
    end

    test "the error says what is wrong and what to do instead" do
      sched = :eta_sched.new(%{seed: 1})

      try do
        {:error, {:eta_sched, {:no_distribution, mfa, hint}}} =
          try do
            :eta_spawn_sut.remote(node(), self())
          catch
            :error, reason -> {:error, reason}
          end

        assert mfa == {:erlang, :spawn, 2}
        assert hint =~ "does not simulate distribution"
      after
        :eta_sched.release(sched)
      end
    end

    test "delegate with no run in progress" do
      # A module built with the transform still works outside a simulation, which
      # is the bargain every other rewrite in the table makes.
      refute :eta_sched.active()
      child = :eta_spawn_sut.remote(node(), self())
      assert_receive {:ran, ^child}, 1000
    end
  end

  describe "gen_server starts" do
    # A gated start cannot complete on its own: the child does not run until the
    # scheduler steps it, and the caller waits for its acknowledgement. So the
    # call has to happen *inside* the schedule, which is how a real system reaches
    # it — a scheduled process starting a child.
    defp under_scheduler(fun) do
      sched = :eta_sched.new(%{seed: 1})
      parent = self()
      caller = :eta_sched.spawn(fn -> send(parent, {:result, fun.()}) end)

      sched =
        sched
        |> :eta_sched.register([caller])
        |> :eta_sched.run(2000)

      :ok = :eta_sched.release(sched)

      receive do
        {:result, r} -> r
      after
        2000 -> flunk("the gated caller never finished")
      end
    end

    test "start_link/3 gates its child and still returns a live server" do
      assert {:ok, pid} = under_scheduler(fn -> :eta_spawn_sut.start_link(self()) end)
      assert Process.alive?(pid)
    end

    for {f, kind} <- [start_link_named: :link, start_named: :plain, start_monitor_named: :monitor] do
      @f f
      @kind kind
      test "#{f} registers the name and enters the loop under it" do
        name = :"eta_spawn_#{@f}"
        result = under_scheduler(fn -> apply(:eta_spawn_sut, @f, [{:local, name}, self()]) end)

        pid =
          case @kind do
            :monitor ->
              assert {:ok, {p, ref}} = result
              assert is_reference(ref)
              p

            _ ->
              assert {:ok, p} = result
              p
          end

        # Registered by the child before `init/1`, which is where `gen` does it.
        assert Process.whereis(name) == pid

        # And the loop was entered *under the name*, or a call addressed to the
        # name would never be answered. The scheduler has been released, so the
        # server can answer.
        assert :gen_server.call(name, :who) == pid
        GenServer.stop(pid)
      end
    end

    test "a name already taken is reported, not crashed on" do
      name = :eta_spawn_taken

      assert {:ok, first} =
               under_scheduler(fn -> :eta_spawn_sut.start_named({:local, name}, self()) end)

      assert {:error, {:already_started, ^first}} =
               under_scheduler(fn -> :eta_spawn_sut.start_named({:local, name}, self()) end)

      GenServer.stop(first)
    end

    test "global and via are refused rather than silently ungated" do
      # Both register through a service process the scheduler does not own, so a
      # start would block on something outside the schedule. Refusing is the
      # honest answer; delegating would produce a run that quietly stopped being
      # reproducible.
      assert {:error, {:eta_sched, :unsupported_server_name, {:global, _}}} =
               under_scheduler(fn ->
                 :eta_spawn_sut.start_named({:global, :eta_spawn_global}, self())
               end)
    end
  end

  describe "gen_statem starts" do
    # The same gate, reached through `eta_sched:statem_start_*`. The child runs a
    # different `init/1` shape and enters a different loop, so it gets its own
    # entry point — and its own coverage, because a gated entry point the
    # scheduler does not recognise leaves its child waiting on a token nobody
    # sends.
    test "start_link/3 gates its child and still returns a live machine" do
      assert {:ok, pid} = under_scheduler(fn -> :eta_statem_sut.start_link(self()) end)
      assert Process.alive?(pid)
      assert :eta_statem_sut.state(pid) == :off
      :eta_statem_sut.stop(pid)
    end

    for {f, kind} <- [start_link_named: :link, start_named: :plain, start_monitor_named: :monitor] do
      @f f
      @kind kind
      test "#{f} registers the name and enters the loop under it" do
        name = :"eta_statem_#{@f}"
        result = under_scheduler(fn -> apply(:eta_statem_sut, @f, [{:local, name}, self()]) end)

        pid =
          case @kind do
            :monitor ->
              assert {:ok, {p, ref}} = result
              assert is_reference(ref)
              p

            _ ->
              assert {:ok, p} = result
              p
          end

        assert Process.whereis(name) == pid

        # A call addressed to the *name* proves the loop was entered under it.
        assert :eta_statem_sut.who(name) == pid

        # And it is a genuine OTP process, not something that merely answers.
        assert {:off, %{flips: 0}} = :sys.get_state(pid)
        assert :proc_lib.translate_initial_call(pid) == {:eta_statem_sut, :init, 1}

        :eta_statem_sut.stop(pid)
      end
    end

    test "the gated machine is stepped by the scheduler, not running ahead of it" do
      # `init/1` labels the process through `?ETA_LABEL`, which only a process the
      # scheduler owns reaches — so a machine that answered here without being
      # adopted would be one that ran outside the schedule.
      assert {:ok, pid} = under_scheduler(fn -> :eta_statem_sut.start_link(self()) end)
      assert :eta_statem_sut.state(pid) == :off
      :eta_statem_sut.flip(pid)
      assert :eta_statem_sut.state(pid) == :on
      :eta_statem_sut.stop(pid)
    end

    test "a name already taken is reported, not crashed on" do
      name = :eta_statem_taken

      assert {:ok, first} =
               under_scheduler(fn -> :eta_statem_sut.start_named({:local, name}, self()) end)

      assert {:error, {:already_started, ^first}} =
               under_scheduler(fn -> :eta_statem_sut.start_named({:local, name}, self()) end)

      :eta_statem_sut.stop(first)
    end

    test "global and via are refused rather than silently ungated" do
      assert {:error, {:eta_sched, :unsupported_server_name, {:global, _}}} =
               under_scheduler(fn ->
                 :eta_statem_sut.start_named({:global, :eta_statem_global}, self())
               end)
    end
  end
end
