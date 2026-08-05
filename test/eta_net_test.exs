defmodule EtaNetTest do
  @moduledoc """
  `:eta_net` — the simulated network.

  The bar these tests hold it to:

  1. **Inert unless started.** A module built with the transform behaves exactly
     as it did before, and so does a run that starts no network.
  2. **Only faults real Erlang can produce.** Loss and delay, never reordering
     within an ordered pair.
  3. **Seeded.** The same seed makes the same decisions about the same messages.
  """
  use ExUnit.Case, async: false

  @net :eta_net
  @time :eta_time

  setup do
    on_exit(fn ->
      @net.stop()
      @time.stop()
    end)

    :ok
  end

  # A process that parks every message it receives where a test can read them.
  defp sink do
    me = self()
    spawn(fn -> sink_loop(me) end)
  end

  defp sink_loop(owner) do
    receive do
      {:__drain__, ref} ->
        send(owner, {ref, :lists.reverse(Process.get(:msgs, []))})
        sink_loop(owner)

      # Drain and reset, for a test that reads the same sink at several points in
      # a sequence of link events and wants each read to be that step alone.
      {:__take__, ref} ->
        send(owner, {ref, :lists.reverse(Process.get(:msgs, []))})
        Process.put(:msgs, [])
        sink_loop(owner)

      {:__send__, dest, msg} ->
        :eta_net.send(dest, msg)
        sink_loop(owner)

      # Monitors have to be created *by* the watching process, so a test that
      # wants one across a link has to ask a sink to make it.
      {:__monitor__, reply_to, ref, dest} ->
        send(reply_to, {ref, :eta_net.monitor(:process, dest)})
        sink_loop(owner)

      {:__demonitor__, reply_to, ref, mref} ->
        send(reply_to, {ref, :eta_net.demonitor(mref, [:flush])})
        sink_loop(owner)

      {:__call__, reply_to, ref, dest, msg} ->
        result =
          try do
            {:ok, :eta_net_echo.call(dest, msg)}
          rescue
            e in ErlangError -> {:error, e.original}
          catch
            :exit, reason -> {:exit, reason}
          end

        send(reply_to, {ref, result})
        sink_loop(owner)

      msg ->
        Process.put(:msgs, [msg | Process.get(:msgs, [])])
        sink_loop(owner)
    end
  end

  # The echo is linked to the test process, so it is usually already gone by the
  # time cleanup runs. Racing that is not worth a failed test.
  defp stop_echo(name) do
    :eta_net_echo.stop(name)
  catch
    :exit, _ -> :ok
  end

  # Send *from* a sink, so the sender is a process the topology knows about. A
  # send from the test process is a send from nowhere, and nowhere is not on any
  # link.
  #
  # Draining the sender afterwards is the sync: its loop is sequential, so an
  # answer to the drain proves the forward already happened.
  defp send_from(from, dest, msg) do
    send(from, {:__send__, dest, msg})
    _ = drained(from)
    :ok
  end

  # Issue a call *from* a placed process, and report what happened. The refusal is
  # an `error/1` raised in the caller, so it has to be caught where it is raised.
  defp call_from(from, dest, msg) do
    ref = make_ref()
    send(from, {:__call__, self(), ref, dest, msg})

    receive do
      {^ref, result} -> result
    after
      2_000 -> flunk("no answer from the calling sink")
    end
  end

  # A call the test does not wait for, so the clock can be advanced while the
  # caller is still blocked in it.
  defp call_async(from, dest, msg) do
    ref = make_ref()
    send(from, {:__call__, self(), ref, dest, msg})
    ref
  end

  defp await_call(ref) do
    receive do
      {^ref, result} -> result
    after
      2_000 -> flunk("the call never finished")
    end
  end

  defp stop_plain(name) do
    :eta_net_plain.stop(name)
  catch
    :exit, _ -> :ok
  end

  # Ask a sink to monitor something, and hand back the ref it got.
  defp monitor_from(watcher, target) do
    ref = make_ref()
    send(watcher, {:__monitor__, self(), ref, target})

    receive do
      {^ref, mref} -> mref
    after
      1_000 -> flunk("sink never monitored")
    end
  end

  defp demonitor_from(watcher, mref) do
    ref = make_ref()
    send(watcher, {:__demonitor__, self(), ref, mref})

    receive do
      {^ref, result} -> result
    after
      1_000 -> flunk("sink never demonitored")
    end
  end

  defp drained(pid) do
    ref = make_ref()
    send(pid, {:__drain__, ref})

    receive do
      {^ref, msgs} -> msgs
    after
      1_000 -> flunk("sink never answered")
    end
  end

  defp wait_until(fun, tries \\ 400) do
    cond do
      fun.() -> :ok
      tries == 0 -> flunk("condition never came true")
      true -> Process.sleep(5) && wait_until(fun, tries - 1)
    end
  end

  defp taken(pid) do
    ref = make_ref()
    send(pid, {:__take__, ref})

    receive do
      {^ref, msgs} -> msgs
    after
      1_000 -> flunk("sink never answered")
    end
  end

  # ---------------------------------------------------------------------------
  # Inertness
  # ---------------------------------------------------------------------------

  describe "with no network running" do
    test "send/2 delivers and returns the message, exactly as `!` does" do
      s = sink()
      assert @net.send(s, {:hello, 1}) == {:hello, 1}
      assert drained(s) == [{:hello, 1}]
    end

    test "running/0 is false and nothing is counted" do
      refute @net.running()
    end

    test "cast/2 and reply/2 fall through to gen_server" do
      {:ok, _} = :eta_net_echo.start_link(:inert_echo)
      on_exit(fn -> stop_echo(:inert_echo) end)

      s = sink()
      assert @net.cast(:inert_echo, {:forward, s, :through}) == :ok
      assert :eta_net_echo.call(:inert_echo, {:echo, 7}) == {:echoed, 7}
      Process.sleep(20)
      assert drained(s) == [:through]
    end

    test "a cast to a name nobody has registered does not raise" do
      assert @net.cast(:nobody_at_all, :msg) == :ok
    end
  end

  # ---------------------------------------------------------------------------
  # The transform actually points a module here
  # ---------------------------------------------------------------------------

  describe "eta_transform's network pass" do
    setup do
      :ok = @net.start(%{seed: 1})
      {:ok, pid} = :eta_net_echo.start_link(:pass_echo)
      on_exit(fn -> stop_echo(:pass_echo) end)

      # The test process and the echo share a node, so the calls below are local
      # API calls rather than network round trips and `eta_net:call/3` lets them
      # through. Their replies are still *routed* — they just cannot be faulted,
      # which is the point: this describe is about the rewrite reaching them.
      :ok = @net.place(:here, [self(), pid])
      :ok
    end

    test "routes `!`, erlang:send/2, gen_server:cast/2 and gen_server:reply/2" do
      s = sink()

      :eta_net_echo.bang(s, :from_bang)
      :eta_net_echo.qualified(s, :from_qualified)
      :eta_net_echo.cast(:pass_echo, {:forward, s, :from_cast})
      assert :eta_net_echo.call(:pass_echo, {:echo, 9}) == {:echoed, 9}

      Process.sleep(20)
      assert Enum.sort(drained(s)) == [:from_bang, :from_cast, :from_qualified]

      # bang + qualified + the echo's forward + the reply. The cast itself is sent
      # by the test process, which is not transformed, so it does not count.
      assert %{delivered: n} = @net.stats()
      assert n >= 4, "expected every rewritten send to be routed, got #{n}"
    end

    test "a routed gen_server:reply still satisfies the caller's selective receive" do
      # The reply is addressed to the caller's pid rather than the alias in the
      # tag. This is the check that OTP's call machinery does not mind.
      for i <- 1..5 do
        assert :eta_net_echo.call(:pass_echo, {:echo, i}) == {:echoed, i}
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Synchronous calls
  # ---------------------------------------------------------------------------

  describe "a call across a link" do
    setup do
      :ok = @time.start(%{start_ms: 0})
      :ok = @net.start(%{seed: 1})
      {:ok, pid} = :eta_net_echo.start_link(:call_echo)
      on_exit(fn -> stop_echo(:call_echo) end)

      caller = sink()
      :ok = @net.place(:a, [caller])
      :ok = @net.place(:b, [pid])
      %{pid: pid, caller: caller}
    end

    test "works, with both legs on the network", %{caller: caller} do
      # The request is routed by `eta_net:call/3`; the reply is routed because the
      # transform rewrote the callee's `{reply, R, S}` into `eta_net:reply/2`.
      assert {:ok, {:echoed, 1}} = call_from(caller, :call_echo, {:echo, 1})

      # Two crossings, not one. A run that could only fault the request would be
      # covering half the channel.
      assert %{delivered: n} = @net.stats()
      assert n >= 2, "expected request and reply to both be routed, got #{n}"
    end

    test "a lost reply times out the caller, though the work happened", %{caller: caller} do
      # The asymmetric fault that actually bites, and the reason routing only the
      # request would have been pointless: the server did the work and answered,
      # and the caller never learned it.
      :ok = @net.set_policy(%{drop_p: 1.0, scope: {:tags, [:"$gen_reply"]}})

      task = call_async(caller, :call_echo, {:echo, 2})
      # Nothing runnable will ever satisfy the receive, so only the clock can end
      # it — and it is a virtual clock, so this costs nothing.
      Process.sleep(20)
      @time.advance(5_000)

      assert {:exit, {:timeout, _}} = await_call(task)
      assert %{dropped: 1} = @net.stats()
    end

    test "a lost request times out the caller too", %{caller: caller} do
      :ok = @net.set_policy(%{drop_p: 1.0, scope: {:tags, [:echo]}})

      task = call_async(caller, :call_echo, {:echo, 3})
      Process.sleep(20)
      @time.advance(5_000)

      assert {:exit, {:timeout, _}} = await_call(task)
    end

    test "the timeout is virtual, so waiting one out costs no real time", %{caller: caller} do
      :ok = @net.set_policy(%{drop_p: 1.0, scope: {:tags, [:echo]}})

      {micros, result} =
        :timer.tc(fn ->
          task = call_async(caller, :call_echo, {:echo, 4})
          Process.sleep(20)
          @time.advance(5_000)
          await_call(task)
        end)

      assert {:exit, {:timeout, _}} = result
      assert micros < 1_000_000, "a 5s virtual timeout took #{div(micros, 1_000)}ms"
    end

    test "a reply that comes around the network is detected", %{caller: caller} do
      # `eta_net_plain` has no transform, so `gen_server` answers it through
      # `gen:reply/2` — a raw send from inside OTP. The call succeeds; what has
      # failed is the fault model, which is silently missing one direction.
      {:ok, plain} = :eta_net_plain.start_link(:plain_echo)
      on_exit(fn -> stop_plain(:plain_echo) end)
      :ok = @net.place(:b, [plain])

      assert {:error, {:eta_net, {:unrouted_reply, _, :echo, _}}} =
               call_from(caller, :plain_echo, {:echo, 5})
    end

    test "a dead callee is an exit, not a hang", %{caller: caller, pid: pid} do
      # Unlink first: the echo is started with start_link from the test process.
      Process.unlink(pid)
      Process.exit(pid, :kill)
      Process.sleep(10)

      assert {:exit, {:noproc, _}} = call_from(caller, :call_echo, {:echo, 6})
    end
  end

  describe "functions eta_net does not implement" do
    test "raise while a network is running, naming the function" do
      :ok = @net.start(%{seed: 1})

      result =
        try do
          {:ok, :eta_net_echo.broadcast(:nobody, :msg)}
        rescue
          e in ErlangError -> {:error, e.original}
        end

      assert {:error, {:eta_net, {:unsupported, {:gen_server, :abcast, 2}, _}}} = result
    end

    test "pass straight through with no network" do
      # A module holding one of these on a path no simulation reaches still builds
      # and still behaves normally outside a run.
      assert :eta_net_echo.broadcast(:nobody, :msg) == :abcast
    end
  end

  describe "a call with no network" do
    test "is an ordinary gen_server:call" do
      {:ok, _} = :eta_net_echo.start_link(:bare_echo)
      on_exit(fn -> stop_echo(:bare_echo) end)

      assert :eta_net_echo.call(:bare_echo, {:echo, 7}) == {:echoed, 7}
    end
  end

  # ---------------------------------------------------------------------------
  # Loss
  # ---------------------------------------------------------------------------

  describe "dropping" do
    test "drop_p 1.0 loses everything, and the counter says so" do
      :ok = @net.start(%{seed: 1, policy: %{drop_p: 1.0}})
      s = sink()

      for i <- 1..10, do: @net.send(s, {:m, i})

      assert drained(s) == []
      assert %{dropped: 10, delivered: 0} = @net.stats()
    end

    test "drop_p 0.0 is exactly a perfect network" do
      :ok = @net.start(%{seed: 1})
      s = sink()

      for i <- 1..10, do: @net.send(s, {:m, i})

      assert drained(s) == for(i <- 1..10, do: {:m, i})
      assert %{dropped: 0, delivered: 10} = @net.stats()
    end

    test "a partial drop rate loses some and keeps order among the survivors" do
      :ok = @net.start(%{seed: 3, policy: %{drop_p: 0.5}})
      s = sink()

      for i <- 1..40, do: @net.send(s, {:m, i})

      got = drained(s)
      assert got != [], "seed dropped everything; pick another"
      assert length(got) < 40, "seed dropped nothing; pick another"

      indexes = for {:m, i} <- got, do: i
      assert indexes == Enum.sort(indexes), "the network reordered an ordered pair"
    end

    test "drop_matching/5 skips K of one tag and drops the next, leaving others alone" do
      # The fault that severs a channel part-way through one batch: a raw message
      # count cannot say it, because the batch is interleaved with other traffic.
      :ok = @net.start(%{seed: 1})
      s = sink()

      :ok = @net.drop_matching(self(), s, :batch, 1, 2)

      @net.send(s, {:batch, 1})
      @net.send(s, {:ack, 1})
      @net.send(s, {:batch, 2})
      @net.send(s, {:batch, 3})
      @net.send(s, {:ack, 2})
      @net.send(s, {:batch, 4})

      assert drained(s) == [{:batch, 1}, {:ack, 1}, {:ack, 2}, {:batch, 4}]
    end

    test "drop_next/3 loses exactly the next K, then stops" do
      :ok = @net.start(%{seed: 1})
      s = sink()

      :ok = @net.drop_next(self(), s, 2)
      for i <- 1..5, do: @net.send(s, {:m, i})

      assert drained(s) == [{:m, 3}, {:m, 4}, {:m, 5}]
    end
  end

  # ---------------------------------------------------------------------------
  # Scope
  # ---------------------------------------------------------------------------

  describe "scope" do
    test "{tags, _} faults only the named traffic" do
      :ok = @net.start(%{seed: 1, policy: %{drop_p: 1.0, scope: {:tags, [:batch]}}})
      s = sink()

      @net.send(s, {:batch, 1})
      @net.send(s, {:ack, 1})
      @net.send(s, {:batch, 2})

      assert drained(s) == [{:ack, 1}]
    end

    test "a predicate can scope by channel" do
      :ok = @net.start(%{seed: 1})
      a = sink()
      b = sink()
      :ok = @net.set_policy(%{drop_p: 1.0, scope: fn _from, dest, _msg -> dest == a end})

      @net.send(a, :to_a)
      @net.send(b, :to_b)

      assert drained(a) == []
      assert drained(b) == [:to_b]
    end

    test "a perfect policy consumes no fault schedule at all" do
      # The rule: the generator advances only when the answer could have been
      # otherwise. This is not an optimisation — it is what makes a fault schedule
      # a function of its seed.
      #
      # A cluster that must sync before it can survive faults starts perfect and
      # turns loss on at the end of `init/2`, and everything it sends while
      # starting up runs on the real scheduler, varying in order and count from
      # run to run. Charging those messages for a decision that could only come out
      # one way leaves the generator at a different offset every time. It cost five
      # distinct schedules in five runs of one seed on a 3-member registry.
      pattern = fn startup ->
        :ok = @net.start(%{seed: 5})
        s = sink()

        # "Startup": traffic sent under a perfect policy, in a quantity that
        # varies, exactly as a real cluster's does.
        for i <- 1..startup//1, do: @net.send(s, {:startup, i})
        _ = drained(s)

        :ok = @net.set_policy(%{drop_p: 0.5})
        for i <- 1..30, do: @net.send(s, {:m, i})

        got = for {:m, i} <- drained(s), do: i
        @net.stop()
        got
      end

      assert pattern.(0) == pattern.(17)
      assert pattern.(0) == pattern.(200)
    end

    test "out-of-scope traffic does not consume the fault schedule" do
      # The trap the Elixir original hit: anything drawing from the RNG before the
      # traffic under test shifts which messages get dropped. Two runs of the same
      # seed, differing only in how much out-of-scope traffic they carry, must make
      # the same decisions about the in-scope messages.
      pattern = fn extra ->
        :ok = @net.start(%{seed: 11, policy: %{drop_p: 0.5, scope: {:tags, [:batch]}}})
        s = sink()

        for i <- 1..20 do
          for _ <- 1..extra//1, do: @net.send(s, {:noise, i})
          @net.send(s, {:batch, i})
        end

        got = for {:batch, i} <- drained(s), do: i
        @net.stop()
        got
      end

      assert pattern.(0) == pattern.(3)
    end
  end

  # ---------------------------------------------------------------------------
  # Channel identity
  # ---------------------------------------------------------------------------

  describe "channel identity" do
    setup do
      :ok = @net.start(%{seed: 1})
      {:ok, pid} = :eta_net_echo.start_link(:named_echo)
      on_exit(fn -> stop_echo(:named_echo) end)
      %{pid: pid}
    end

    test "a name, a {Name, node()} tuple and the pid are one channel", %{pid: pid} do
      # `gen_server:cast/2` to a member named `{Name, Node}` is what dgen's
      # registry does, and it has to land on the same channel as a send to the
      # same process by pid — or each gets its own ordering clamp and one can
      # overtake the other.
      :ok = @net.cut(self(), pid)

      s = sink()
      @net.send(:named_echo, {:forward, s, :by_name})
      @net.send({:named_echo, node()}, {:forward, s, :by_tuple})
      @net.send(pid, {:forward, s, :by_pid})

      Process.sleep(20)
      assert drained(s) == [], "a cut by pid did not cover the same target by name"
      assert %{dropped: 3} = @net.stats()
    end

    test "a cut can be expressed by name too", %{pid: _pid} do
      :ok = @net.cut(self(), :named_echo)
      s = sink()

      @net.send(:named_echo, {:forward, s, :nope})
      Process.sleep(20)

      assert drained(s) == []
    end

    test "an unregistered name is not a channel, and behaves as erlang:send does" do
      assert_raise ArgumentError, fn -> @net.send(:no_such_name, :m) end
    end
  end

  # ---------------------------------------------------------------------------
  # Topology
  # ---------------------------------------------------------------------------

  describe "node placement" do
    test "faults only cross the link, never stay inside a node" do
      :ok = @net.start(%{seed: 1, policy: %{drop_p: 1.0}})
      a1 = sink()
      a2 = sink()
      b1 = sink()

      :ok = @net.place(:a, [a1, a2])
      :ok = @net.place(:b, [b1])

      :ok = send_from(a1, b1, :crosses)
      assert drained(b1) == [], "a cross-link send was not faulted"

      # Same node: not a network at all, so not a thing a network can break.
      :ok = send_from(a1, a2, :within)
      assert drained(a2) == [:within], "a same-node send was faulted"
    end

    test "an unplaced process is off the network in both directions" do
      :ok = @net.start(%{seed: 1, policy: %{drop_p: 1.0}})
      placed = sink()
      stranger = sink()

      :ok = @net.place(:a, [placed])

      :ok = send_from(placed, stranger, :to_stranger)
      assert drained(stranger) == [:to_stranger], "an unplaced destination was faulted"

      :ok = send_from(stranger, placed, :from_stranger)
      assert drained(placed) == [:from_stranger], "an unplaced sender was faulted"
    end

    test "a node cut survives the process on the far side being replaced" do
      # The difference between cutting a link and cutting a pid. A pid-level cut
      # names a process; a partition names a place, and whatever is standing there
      # after a restart is still on the wrong side of it.
      :ok = @net.start(%{seed: 1})
      a = sink()
      b = sink()
      :ok = @net.place(:na, [a])
      :ok = @net.place(:nb, [b])
      :ok = @net.partition(:na, :nb)

      replacement = sink()
      :ok = @net.place(:nb, [replacement])

      :ok = send_from(a, replacement, :should_not_arrive)
      assert drained(replacement) == []
    end

    test "with no topology at all every link is faultable" do
      # `place/2` is opt-in: a network that was never given one behaves exactly as
      # it did before there was such a thing.
      :ok = @net.start(%{seed: 1, policy: %{drop_p: 1.0}})
      s = sink()

      @net.send(s, :lost)
      assert drained(s) == []
    end

    test "a child is placed where its parent is" do
      # Without this a topology is a snapshot rather than a description: the first
      # worker the system spawns sends across a link nobody declared.
      :ok = @net.start(%{seed: 1})
      parent = sink()
      :ok = @net.place(:a, [parent])

      child = spawn(fn -> Process.sleep(:infinity) end)
      :ok = @net.inherit(parent, child)

      assert @net.node_of(child) == :a
    end

    test "inheriting from an unplaced parent places nothing" do
      :ok = @net.start(%{seed: 1})
      parent = sink()
      child = sink()

      :ok = @net.inherit(parent, child)
      assert @net.node_of(child) == :undefined
    end
  end

  describe "node-level partitions" do
    setup do
      :ok = @net.start(%{seed: 1})
      a = sink()
      b = sink()
      :ok = @net.place(:na, [a])
      :ok = @net.place(:nb, [b])
      %{a: a, b: b}
    end

    test "partition/2 names nodes and cuts both directions", %{a: a, b: b} do
      :ok = @net.partition(:na, :nb)

      :ok = send_from(a, b, :from_a_side)
      assert drained(b) == []
      :ok = send_from(b, a, :from_b_side)
      assert drained(a) == []

      :ok = @net.heal_partition(:na, :nb)
      :ok = send_from(a, b, :after_heal)
      assert drained(b) == [:after_heal]
    end

    test "the link signal reaches every process on both nodes" do
      a2 = sink()
      b2 = sink()
      :ok = @net.place(:na, [a2])
      :ok = @net.place(:nb, [b2])

      :ok = @net.partition(:na, :nb, %{signal: {:nodedown, :peer}})

      # Not two representative processes — everything on both sides, because a
      # system hangs its recovery off this event.
      assert drained(a2) == [{:nodedown, :peer}]
      assert drained(b2) == [{:nodedown, :peer}]
    end

    test "cutting one node to another is refused if the ends disagree", %{a: a} do
      assert_raise ErlangError, fn -> @net.cut(:na, a) end
    end
  end

  # ---------------------------------------------------------------------------
  # Cuts and partitions
  # ---------------------------------------------------------------------------

  describe "cuts" do
    test "cut/2 is one-directional and heal/2 restores it" do
      :ok = @net.start(%{seed: 1})
      s = sink()

      :ok = @net.cut(self(), s)
      @net.send(s, :while_cut)
      assert drained(s) == []

      :ok = @net.heal(self(), s)
      @net.send(s, :after_heal)
      assert drained(s) == [:after_heal]
    end

    test "partition/3 delivers the link signal to both ends, cut or not" do
      :ok = @net.start(%{seed: 1})
      a = sink()
      b = sink()

      :ok = @net.partition(a, b, %{signal: {:nodedown, node()}})

      assert drained(a) == [{:nodedown, node()}]
      assert drained(b) == [{:nodedown, node()}]
    end

    test "heal_all/0 clears cuts and pending drop_nexts" do
      :ok = @net.start(%{seed: 1})
      s = sink()

      :ok = @net.cut(self(), s)
      :ok = @net.drop_next(self(), s, 5)
      :ok = @net.drop_matching(self(), s, :m, 0, 5)
      :ok = @net.heal_all()

      @net.send(s, :m)
      assert drained(s) == [:m]
    end
  end

  # ---------------------------------------------------------------------------
  # Link events: signals
  # ---------------------------------------------------------------------------

  describe "a link signal" do
    setup do
      :ok = @net.start(%{seed: 1})
      a = sink()
      b = sink()
      :ok = @net.place(:na, [a])
      :ok = @net.place(:nb, [b])
      %{a: a, b: b}
    end

    test "given as an atom is derived per side", %{a: a, b: b} do
      # What a partition actually says: A learns B is gone and B learns A is.
      # One undifferentiated term tells both sides the same thing, which is never
      # what happened.
      :ok = @net.partition(:na, :nb, %{signal: :nodedown})

      assert taken(a) == [{:nodedown, :nb}]
      assert taken(b) == [{:nodedown, :na}]
    end

    test "given as a term is delivered as written, to both sides", %{a: a, b: b} do
      # The form that has callers. It has to keep meaning exactly what it did.
      :ok = @net.partition(:na, :nb, %{signal: {:nodedown, node()}})

      assert taken(a) == [{:nodedown, node()}]
      assert taken(b) == [{:nodedown, node()}]
    end

    test "given as {literal, Atom} is not derived", %{a: a, b: b} do
      :ok = @net.partition(:na, :nb, %{signal: {:literal, :split}})

      assert taken(a) == [:split]
      assert taken(b) == [:split]
    end

    test "reaches only the side that learns, while the cut stays symmetric", %{a: a, b: b} do
      # "A finds out that B is gone and B does not notice" — the asymmetry two
      # independently timing-out ends produce constantly, and the thing that used
      # to need two cut/2 calls and a hand-rolled fan-out.
      :ok = @net.partition(:na, :nb, %{signal: :nodedown, learns: :a})

      assert taken(a) == [{:nodedown, :nb}]
      assert taken(b) == [], "the side that was not told still heard about it"

      :ok = send_from(a, b, :from_a)
      assert taken(b) == [], "a one-sided signal made the cut one-sided too"
      :ok = send_from(b, a, :from_b)
      assert taken(a) == []
    end

    test "on heal derives the other direction the same way", %{a: a, b: b} do
      :ok = @net.partition(:na, :nb, %{signal: :nodedown})
      _ = taken(a)
      _ = taken(b)

      :ok = @net.heal_partition(:na, :nb, %{signal: :nodeup})

      assert taken(a) == [{:nodeup, :nb}]
      assert taken(b) == [{:nodeup, :na}]
    end

    test "between two processes names the pids, since there are no nodes to name" do
      :ok = @net.start(%{seed: 1})
      a = sink()
      b = sink()

      :ok = @net.partition(a, b, %{signal: :nodedown})

      assert taken(a) == [{:nodedown, b}]
      assert taken(b) == [{:nodedown, a}]
    end

    test "is counted, so a partition that reached nobody is visible" do
      :ok = @net.partition(:na, :nb, %{signal: :nodedown})
      assert %{signalled: 2} = @net.stats()

      :ok = @net.place(:empty, [])
      :ok = @net.partition(:empty, :na, %{signal: :nodedown})
      assert %{signalled: 3} = @net.stats()
    end

    test "rejects a `learns` it does not understand" do
      assert_raise ErlangError, fn -> @net.partition(:na, :nb, %{learns: :neither}) end
    end
  end

  describe "the order a link event fans out in" do
    # `on_node/1` and the monitor table are both read with `ets:match`, whose row
    # order is specified by nothing. These two assert the fan-out is ordered by
    # something the *run* decided instead — placement order and monitor-creation
    # order — because a fan-out that follows the table puts a different sequence
    # of messages in a mailbox from one run of a seed to the next.

    test "follows placement order for signals" do
      :ok = :eta_log.trace(:start)
      on_exit(fn -> :eta_log.stop() end)
      :ok = @net.start(%{seed: 1})

      side_a = for _ <- 1..8, do: sink()
      b = sink()
      for p <- side_a, do: :ok = @net.place(:na, [p])
      :ok = @net.place(:nb, [b])

      :ok = @net.partition(:na, :nb, %{signal: :nodedown})

      delivered =
        for {_seq, _label, {:net, :signal, pid, _tag}} <- :eta_log.events(),
            pid in side_a,
            do: pid

      assert delivered == side_a
    end

    test "follows monitor order for synthetic DOWNs" do
      :ok = @net.start(%{seed: 1})

      watcher = sink()
      :ok = @net.place(:na, [watcher])

      targets = for _ <- 1..8, do: sink()
      for t <- targets, do: :ok = @net.place(:nb, [t])
      refs = for t <- targets, do: monitor_from(watcher, t)

      :ok = @net.partition(:na, :nb)

      fired = for {:DOWN, ref, :process, _target, :noconnection} <- taken(watcher), do: ref
      assert fired == refs
    end
  end

  # ---------------------------------------------------------------------------
  # Located vs faultable
  # ---------------------------------------------------------------------------

  describe "attach/2" do
    setup do
      :ok = @net.start(%{seed: 1, policy: %{drop_p: 1.0}})
      member = sink()
      conn = sink()
      peer = sink()

      :ok = @net.place(:na, [member])
      :ok = @net.attach(:na, [conn])
      :ok = @net.place(:nb, [peer])

      %{member: member, conn: conn, peer: peer}
    end

    test "receives its node's link events while its traffic survives a policy that drops everything",
         %{member: member, conn: conn, peer: peer} do
      # The case that forces located and faultable apart. A connector's sends
      # model a durable-store operation rather than a message, so dropping one
      # injects a failure the real system cannot have — but it is still the thing
      # that has to hear about a dead peer.
      :ok = send_from(member, peer, :from_member)
      assert taken(peer) == [], "a cross-link send from a placed process was not faulted"

      :ok = send_from(conn, peer, :from_conn)
      assert taken(peer) == [:from_conn], "an attached process's send was faulted"

      :ok = send_from(peer, conn, :to_conn)
      assert taken(conn) == [:to_conn], "an attached process's inbound traffic was faulted"

      :ok = @net.partition(:na, :nb, %{signal: :nodedown})

      assert taken(conn) == [{:nodedown, :nb}], "an attached process missed its node's link event"
      assert taken(member) == [{:nodedown, :nb}]
    end

    test "is located, and says so", %{member: member, conn: conn, peer: peer} do
      assert @net.node_of(conn) == :na
      assert @net.faultable(conn) == false
      assert @net.faultable(member) == true
      assert @net.faultable(peer) == true
      assert @net.faultable(sink()) == false, "an unplaced process is not on the network"
    end

    test "is inherited by children, so a worker does not acquire a wire its parent lacks", %{
      conn: conn,
      peer: peer
    } do
      child = sink()
      :ok = @net.inherit(conn, child)

      assert @net.node_of(child) == :na
      assert @net.faultable(child) == false

      :ok = send_from(child, peer, :from_child)
      assert taken(peer) == [:from_child]
    end
  end

  # ---------------------------------------------------------------------------
  # Link events: synthetic noconnection DOWNs
  # ---------------------------------------------------------------------------

  describe "a monitor across a partition" do
    setup do
      :ok = @net.start(%{seed: 1})
      a = sink()
      b = sink()
      :ok = @net.place(:na, [a])
      :ok = @net.place(:nb, [b])
      %{a: a, b: b}
    end

    test "fires exactly one noconnection DOWN, and heal does not resurrect it", %{a: a, b: b} do
      mref = monitor_from(a, b)

      :ok = @net.partition(:na, :nb, %{signal: :nodedown})

      assert taken(a) == [
               {:DOWN, mref, :process, b, :noconnection},
               {:nodedown, :nb}
             ]

      assert %{noconnection: 1} = @net.stats()

      # Real Erlang does not re-arm a monitor after a noconnection DOWN, and the
      # monitoring process has to monitor again. Healing is a second event, not
      # an undo.
      :ok = @net.heal_partition(:na, :nb)
      assert taken(a) == []

      :ok = @net.partition(:na, :nb)
      assert taken(a) == [], "a retired monitor fired a second time"
      assert %{noconnection: 1} = @net.stats()
    end

    test "does not fire when its target is on the same side of the cut", %{a: a} do
      a2 = sink()
      :ok = @net.place(:na, [a2])
      _mref = monitor_from(a, a2)

      :ok = @net.partition(:na, :nb)

      assert taken(a) == []
      assert %{noconnection: 0} = @net.stats()
    end

    test "does not fire when it was established after the cut", %{a: a, b: b} do
      # A synthetic DOWN marks the moment a link failed. A monitor created while
      # the link is already down has no such moment behind it, so nothing is
      # retroactively severed — and the next real failure of that link fires it.
      :ok = @net.partition(:na, :nb)
      mref = monitor_from(a, b)

      assert taken(a) == []

      :ok = @net.heal_partition(:na, :nb)
      :ok = @net.partition(:na, :nb)

      assert taken(a) == [{:DOWN, mref, :process, b, :noconnection}]
    end

    test "fires only on the side that learns", %{a: a, b: b} do
      from_a = monitor_from(a, b)
      _from_b = monitor_from(b, a)

      :ok = @net.partition(:na, :nb, %{learns: :a})

      assert taken(a) == [{:DOWN, from_a, :process, b, :noconnection}]
      assert taken(b) == [], "the side that did not notice fired a monitor anyway"
    end

    test "can be cancelled, and then a partition fires nothing", %{a: a, b: b} do
      mref = monitor_from(a, b)
      assert demonitor_from(a, mref) == true

      :ok = @net.partition(:na, :nb)

      assert taken(a) == []
    end

    test "cancelling one that already fired is not an error", %{a: a, b: b} do
      mref = monitor_from(a, b)
      :ok = @net.partition(:na, :nb)
      assert [{:DOWN, ^mref, :process, ^b, :noconnection}] = taken(a)

      # Real Erlang does not raise for an already-fired monitor, and a system
      # tidying up after a DOWN must not crash on the tidying.
      assert demonitor_from(a, mref) == true
    end

    test "on a target that is already dead is a noproc DOWN, as erlang's is", %{a: a} do
      dead = sink()
      :ok = @net.place(:nb, [dead])
      Process.exit(dead, :kill)
      Process.sleep(10)

      mref = monitor_from(a, dead)

      assert taken(a) == [{:DOWN, mref, :process, dead, :noproc}]
    end

    test "between two processes with no topology is left to the VM" do
      # Simulation is confined to monitors that cross a *declared* link. Anything
      # else keeps machinery that cannot go wrong.
      :ok = @net.start(%{seed: 1})
      w = sink()
      t = sink()
      _mref = monitor_from(w, t)

      :ok = @net.partition(w, t)

      assert taken(w) == []
    end
  end

  describe "a simulated monitor whose target exits" do
    test "fires with the real reason once the scheduler reports it" do
      # A simulated monitor has no VM monitor behind it, so `eta_sched` is what
      # tells the network the target is gone. Without this a system that detects
      # a crashed peer by monitor would simply hang.
      :ok = @net.start(%{seed: 1})
      s = :eta_sched.new(%{seed: 1})
      on_exit(fn -> catch_exit(:eta_sched.release(s)) end)

      watcher = sink()
      target = sink()
      :ok = @net.place(:na, [watcher])
      :ok = @net.place(:nb, [target])
      mref = monitor_from(watcher, target)

      _ = :eta_sched.register(s, [watcher, target])
      Process.exit(target, :kill)

      # The scheduler is the tracer, so the exit reaches it as a trace event and
      # `eta_net:notify_exit/2` fires the monitor from there.
      wait_until(fn -> not Process.alive?(target) end)
      _ = :eta_sched.stats(s)
      :ok = :eta_sched.release(s)

      assert taken(watcher) == [{:DOWN, mref, :process, target, :killed}]
    end
  end

  # ---------------------------------------------------------------------------
  # Node death
  # ---------------------------------------------------------------------------

  describe "kill_node/2" do
    setup do
      :ok = @net.start(%{seed: 1})
      member = sink()
      conn = sink()
      peer = sink()
      observer = sink()

      :ok = @net.place(:na, [member])
      :ok = @net.attach(:na, [conn])
      :ok = @net.place(:nb, [peer])

      %{member: member, conn: conn, peer: peer, observer: observer}
    end

    test "kills every located process, signals the survivors, and reports noconnection to a remote monitor",
         %{member: member, conn: conn, peer: peer, observer: observer} do
      # Cross-node: simulated, so it can be told what distribution would have told
      # it. Unplaced: a real monitor in the same VM, so it sees the true reason.
      # That is the asymmetry real distribution has, seen from both sides.
      cross = monitor_from(peer, member)
      local = monitor_from(observer, member)

      :ok = @net.kill_node(:na, %{signal: :nodedown})

      refute Process.alive?(member)
      refute Process.alive?(conn), "an attached process did not die with its node"
      assert Process.alive?(peer)

      assert taken(peer) == [
               {:DOWN, cross, :process, member, :noconnection},
               {:nodedown, :na}
             ]

      assert taken(observer) == [{:DOWN, local, :process, member, :killed}]
    end

    test "a monitor held by a dying process is simply gone", %{member: member, peer: peer} do
      _outbound = monitor_from(member, peer)

      :ok = @net.kill_node(:na, %{signal: :nodedown})

      assert taken(peer) == [{:nodedown, :na}]
    end

    test "an unplaced process gets no signal", %{observer: observer} do
      :ok = @net.kill_node(:na, %{signal: :nodedown})
      assert taken(observer) == []
    end

    test "the node name survives, so placing on it again is a restart", %{peer: peer} do
      :ok = @net.kill_node(:na, %{signal: :nodedown})
      _ = taken(peer)

      replacement = sink()
      :ok = @net.place(:na, [replacement])
      assert @net.node_of(replacement) == :na

      :ok = send_from(replacement, peer, :back_up)
      assert taken(peer) == [:back_up]

      :ok = @net.partition(:na, :nb, %{signal: :nodedown})
      assert taken(replacement) == [{:nodedown, :nb}]
    end

    test "loses what was in flight to and from the node", %{member: member, peer: peer} do
      :ok = @time.start(%{start_ms: 0})
      :ok = @net.set_policy(%{delay_p: 1.0, max_delay: 50})

      :ok = send_from(peer, member, :in_flight)
      assert @net.in_flight() == 1

      :ok = @net.kill_node(:na)
      @time.advance(100)

      assert @net.in_flight() == 0
      assert %{cancelled: 1} = @net.stats()
    end

    test "naming a node nothing was ever placed on raises rather than doing nothing" do
      assert_raise ErlangError, fn -> @net.kill_node(:no_such_node) end
    end
  end

  # ---------------------------------------------------------------------------
  # Delay, on the virtual clock
  # ---------------------------------------------------------------------------

  describe "delay" do
    test "a delayed message is not delivered until the clock reaches it" do
      :ok = @time.start(%{start_ms: 0})
      :ok = @net.start(%{seed: 1, policy: %{delay_p: 1.0, max_delay: 10}})
      s = sink()

      @net.send(s, :later)

      assert drained(s) == [], "delivered without the clock moving"
      assert @net.in_flight() == 1
      assert %{delayed: 1} = @net.stats()

      @time.advance(10)
      assert drained(s) == [:later]
      assert @net.in_flight() == 0
    end

    test "a delay costs no wall-clock time" do
      :ok = @time.start(%{start_ms: 0})
      :ok = @net.start(%{seed: 1, policy: %{delay_p: 1.0, max_delay: 60_000}})
      s = sink()

      {micros, _} =
        :timer.tc(fn ->
          @net.send(s, :slow)
          @time.advance(60_000)
        end)

      assert drained(s) == [:slow]
      assert micros < 100_000, "a virtual delay took #{micros}us of real time"
    end

    test "a later undelayed message cannot overtake an earlier delayed one" do
      # The ordering guarantee, and the whole reason for the FIFO clamp: Erlang
      # never reorders within an ordered pair, so neither may this.
      :ok = @time.start(%{start_ms: 0})
      :ok = @net.start(%{seed: 1, policy: %{delay_p: 1.0, max_delay: 50}})
      s = sink()

      @net.send(s, :first)
      :ok = @net.set_policy(%{delay_p: 0.0})
      @net.send(s, :second)

      assert drained(s) == [], "the undelayed message overtook the delayed one"

      @time.advance(100)
      assert drained(s) == [:first, :second]
    end

    test "with no virtual clock the delay is a real one, so the module works outside a run" do
      # `eta_time` already delegates `send_after/3` to `erlang` with no clock
      # running, so a hand-driven network delays for real rather than silently
      # delivering at once — which is what a harness predating `eta_run` needs.
      :ok = @net.start(%{seed: 1, policy: %{delay_p: 1.0, max_delay: 30}})
      s = sink()

      @net.send(s, :real_delay)

      assert drained(s) == [], "delivered immediately with no clock"
      assert %{delayed: 1} = @net.stats()

      Process.sleep(80)
      assert drained(s) == [:real_delay]
    end

    test "cutting a channel loses what was in flight on it" do
      :ok = @time.start(%{start_ms: 0})
      :ok = @net.start(%{seed: 1, policy: %{delay_p: 1.0, max_delay: 50}})
      s = sink()

      @net.send(s, :in_flight)
      assert @net.in_flight() == 1

      :ok = @net.cut(self(), s)
      @time.advance(100)

      assert drained(s) == []
      assert %{cancelled: 1, dropped: 1} = @net.stats()
    end
  end

  # ---------------------------------------------------------------------------
  # Determinism
  # ---------------------------------------------------------------------------

  describe "the fault schedule" do
    test "is a function of the seed" do
      run = fn seed ->
        :ok = @net.start(%{seed: seed, policy: %{drop_p: 0.4}})
        s = sink()
        for i <- 1..60, do: @net.send(s, {:m, i})
        got = for {:m, i} <- drained(s), do: i
        @net.stop()
        got
      end

      assert run.(7) == run.(7)
      assert run.(7) != run.(8)
    end

    test "starting a network resets it" do
      :ok = @net.start(%{seed: 1, policy: %{drop_p: 1.0}})
      s = sink()
      @net.send(s, :lost)
      assert %{dropped: 1} = @net.stats()

      :ok = @net.start(%{seed: 1})
      assert %{dropped: 0, delivered: 0} = @net.stats()
      assert @net.policy().drop_p == 0.0
    end
  end

  describe "one network per VM" do
    test "starting over another live process's network raises rather than clobbering it" do
      owner = spawn_link(fn -> receive do: (:stop -> :ok) end)
      :ok = :erpc.call(node(), fn -> :ok end)

      # Start it from a process that stays alive, so the table has a live owner.
      me = self()

      spawn_link(fn ->
        :ok = @net.start(%{seed: 1})
        send(me, :started)
        receive do: (:stop -> :ok)
      end)

      assert_receive :started, 1_000

      assert_raise ErlangError, fn -> @net.start(%{seed: 2}) end

      send(owner, :stop)
    end
  end
end
