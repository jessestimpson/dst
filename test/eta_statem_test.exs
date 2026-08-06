defmodule EtaStatemTest do
  @moduledoc """
  `gen_statem` under `eta_transform`, held to the same bar as the `gen_server`
  path:

  1. **Both legs of a call are on the network.** The request through
     `eta_net:call/2,3`, the reply because the transform took the
     `{reply, From, Msg}` action out of the callback's return. A reply that comes
     around the network is detected rather than tolerated.
  2. **Every reply form is routed.** A reply action, `gen_statem:reply/1`,
     `gen_statem:reply/2`, and the `Replies` list of a `stop_and_reply` — a pass
     that catches some of them produces the half-routed channel `eta_net` warns
     about.
  3. **Both callback modes.** `handle_event_function` names its callback;
     `state_functions` names them after the states, so the pass has to work the
     set out.
  4. **Inert unless started**, as everywhere else.
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

  # A process that parks every message it receives where a test can read them,
  # and can be told to issue a call so the caller is a process the topology knows
  # about. Same shape as the one in `EtaNetTest`.
  defp sink do
    me = self()
    spawn(fn -> sink_loop(me) end)
  end

  defp sink_loop(owner) do
    receive do
      {:__drain__, ref} ->
        send(owner, {ref, :lists.reverse(Process.get(:msgs, []))})
        sink_loop(owner)

      {:__call__, reply_to, ref, dest, msg} ->
        result =
          try do
            {:ok, :eta_statem_echo.call(dest, msg)}
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

  defp drained(sink) do
    ref = make_ref()
    send(sink, {:__drain__, ref})

    receive do
      {^ref, msgs} -> msgs
    after
      2_000 -> flunk("no answer from the sink")
    end
  end

  defp call_from(from, dest, msg) do
    receive_call(call_async(from, dest, msg))
  end

  defp call_async(from, dest, msg) do
    ref = make_ref()
    send(from, {:__call__, self(), ref, dest, msg})
    ref
  end

  defp receive_call(ref) do
    receive do
      {^ref, result} -> result
    after
      2_000 -> flunk("no answer from the calling sink")
    end
  end

  defp stop_echo(name) do
    quietly(fn -> :eta_statem_echo.stop(name) end)
  end

  # Cleanup runs after the test process has gone, and a machine linked to it is
  # *usually* already dead by then — so `catch_exit/1` looks right and is not:
  # it requires an exit, and fails the test on the runs where the cleanup
  # arrives first and stops the machine cleanly.
  defp quietly(fun) do
    fun.()
  catch
    :exit, _ -> :ok
  end

  # ---------------------------------------------------------------------------
  # Inert without a network
  # ---------------------------------------------------------------------------

  describe "with no network running" do
    setup do
      {:ok, pid} = :eta_statem_echo.start_link(:inert_statem)
      on_exit(fn -> stop_echo(:inert_statem) end)
      %{pid: pid}
    end

    test "every reply form answers exactly as it did before the transform" do
      # The bargain every rewrite in the transform makes: a module built for
      # simulation is an ordinary module outside one.
      assert :eta_statem_echo.call(:inert_statem, {:echo, 1}) == {:echoed, 1}
      assert :eta_statem_echo.call(:inert_statem, {:defer, 2}) == {:deferred, 2}
      assert :eta_statem_echo.call(:inert_statem, {:defer_one, 3}) == {:deferred, 3}
      assert :eta_statem_echo.call(:inert_statem, {:echo_note, 4}) == {:echoed, 4}
    end

    test "cast/2 and `!` still deliver" do
      s = sink()
      :eta_statem_echo.bang(s, :from_bang)
      :eta_statem_echo.qualified(s, :from_qualified)
      :eta_statem_echo.cast(:inert_statem, {:forward, s, :from_cast})

      Process.sleep(20)
      assert Enum.sort(drained(s)) == [:from_bang, :from_cast, :from_qualified]
    end

    test "stop_and_reply answers and then stops", %{pid: pid} do
      Process.unlink(pid)
      assert :eta_statem_echo.call(:inert_statem, {:bye, 5}) == {:bye, 5}
      Process.sleep(20)
      refute Process.alive?(pid)
    end
  end

  # ---------------------------------------------------------------------------
  # The network pass reaches a state machine
  # ---------------------------------------------------------------------------

  describe "eta_transform's network pass" do
    setup do
      :ok = @net.start(%{seed: 1})
      {:ok, pid} = :eta_statem_echo.start_link(:pass_statem)
      on_exit(fn -> stop_echo(:pass_statem) end)

      # Both ends on one node, so these are local API calls rather than network
      # round trips and `eta_net:call/3` lets them through. Their replies are
      # still routed — which is what this describe is about.
      :ok = @net.place(:here, [self(), pid])
      %{pid: pid}
    end

    test "routes `!`, erlang:send/2 and gen_statem:cast/2" do
      s = sink()

      :eta_statem_echo.bang(s, :from_bang)
      :eta_statem_echo.qualified(s, :from_qualified)
      :eta_statem_echo.cast(:pass_statem, {:forward, s, :from_cast})
      assert :eta_statem_echo.call(:pass_statem, {:echo, 9}) == {:echoed, 9}

      Process.sleep(20)
      assert Enum.sort(drained(s)) == [:from_bang, :from_cast, :from_qualified]

      # bang + qualified + the machine's forward + the reply.
      assert %{delivered: n} = @net.stats()
      assert n >= 4, "expected every rewritten send to be routed, got #{n}"
    end

    for {label, req} <- [
          {"a reply action", {:echo, 1}},
          {"gen_statem:reply/2", {:defer, 1}},
          {"gen_statem:reply/1", {:defer_one, 1}}
        ] do
      @label label
      @req req
      test "#{label} is routed and still satisfies the caller's selective receive" do
        before = @net.stats().delivered
        assert :eta_statem_echo.call(:pass_statem, @req) |> elem(1) == 1
        assert @net.stats().delivered - before >= 2, "#{@label} did not cross the network"
      end
    end

    test "the actions a reply travelled with survive the rewrite" do
      # `{reply, From, R}` is taken out of the list; everything else in it has to
      # reach gen_statem untouched, or the rewrite silently swallows whatever
      # else the callback asked for.
      assert :eta_statem_echo.call(:pass_statem, {:echo_note, 2}) == {:echoed, 2}
      assert :eta_statem_echo.call(:pass_statem, {:echo_note, 3}) == {:echoed, 3}

      # The `next_event` actions the replies rode with were handled, which they
      # can only have been if gen_statem saw them.
      assert :eta_statem_echo.call(:pass_statem, :notes) == [2, 3]
    end

    test "stop_and_reply routes its replies", %{pid: pid} do
      Process.unlink(pid)
      before = @net.stats().delivered

      assert :eta_statem_echo.call(:pass_statem, {:bye, 3}) == {:bye, 3}
      assert @net.stats().delivered - before >= 2

      Process.sleep(20)
      refute Process.alive?(pid)
    end

    test "a call with {dirty_timeout, T} is routed like any other" do
      # The gen_statem-only timeout forms name how the *caller* waits. A routed
      # call waits in a receive of its own, so both have to reach the virtual
      # clock as a plain number rather than blowing up in `arm_after/2`.
      assert :eta_statem_echo.call_dirty(:pass_statem, {:echo, 4}) == {:echoed, 4}
    end
  end

  # ---------------------------------------------------------------------------
  # Both legs, and what happens when one is lost
  # ---------------------------------------------------------------------------

  describe "a call to a state machine across a link" do
    setup do
      :ok = @time.start(%{start_ms: 0})
      :ok = @net.start(%{seed: 1})
      {:ok, pid} = :eta_statem_echo.start_link(:link_statem)
      on_exit(fn -> stop_echo(:link_statem) end)

      caller = sink()
      :ok = @net.place(:a, [caller])
      :ok = @net.place(:b, [pid])
      %{pid: pid, caller: caller}
    end

    test "works, with both legs on the network", %{caller: caller} do
      assert {:ok, {:echoed, 1}} = call_from(caller, :link_statem, {:echo, 1})

      assert %{delivered: n} = @net.stats()
      assert n >= 2, "expected request and reply to both be routed, got #{n}"
    end

    test "a lost reply times out the caller, though the work happened", %{caller: caller} do
      # The asymmetric fault, reachable for a state machine for the first time:
      # the machine changed state and answered, and the caller never learned it.
      :ok = @net.set_policy(%{drop_p: 1.0, scope: {:tags, [:"$gen_reply"]}})

      task = call_async(caller, :link_statem, {:echo, 2})
      Process.sleep(20)
      @time.advance(5_000)

      assert {:exit, {:timeout, _}} = receive_call(task)
      assert %{dropped: 1} = @net.stats()
    end

    test "a lost request times out the caller too", %{caller: caller} do
      :ok = @net.set_policy(%{drop_p: 1.0, scope: {:tags, [:echo]}})

      task = call_async(caller, :link_statem, {:echo, 3})
      Process.sleep(20)
      @time.advance(5_000)

      assert {:exit, {:timeout, _}} = receive_call(task)
    end

    test "a reply that comes around the network is detected", %{caller: caller} do
      # `eta_statem_plain` has no transform, so `gen_statem` handles the reply
      # action itself and sends it through `gen:reply/2`. The call succeeds; what
      # has failed is the fault model, which is silently missing one direction.
      {:ok, plain} = :eta_statem_plain.start_link(:plain_statem)
      on_exit(fn -> quietly(fn -> :eta_statem_plain.stop(:plain_statem) end) end)
      :ok = @net.place(:b, [plain])

      assert {:error, {:eta_net, {:unrouted_reply, _, :echo, _}}} =
               call_from(caller, :plain_statem, {:echo, 5})
    end

    test "a dead callee is an exit, not a hang", %{caller: caller, pid: pid} do
      Process.unlink(pid)
      Process.exit(pid, :kill)
      Process.sleep(10)

      assert {:exit, {:noproc, _}} = call_from(caller, :link_statem, {:echo, 6})
    end
  end

  # ---------------------------------------------------------------------------
  # state_functions mode
  # ---------------------------------------------------------------------------

  describe "state_functions mode" do
    setup do
      :ok = @net.start(%{seed: 1})
      {:ok, pid} = :eta_statem_sut.start_link(self())
      on_exit(fn -> quietly(fn -> :eta_statem_sut.stop(pid) end) end)
      :ok = @net.place(:here, [self(), pid])
      %{pid: pid}
    end

    test "a state function's reply action is routed", %{pid: pid} do
      # The callbacks are named after the states, so there is no fixed list for
      # the pass to wrap — it has to read the export list and the callback mode.
      before = @net.stats().delivered
      assert :eta_statem_sut.state(pid) == :off
      assert @net.stats().delivered - before >= 2
    end

    test "the machine still transitions", %{pid: pid} do
      assert :eta_statem_sut.state(pid) == :off
      :eta_statem_sut.flip(pid)
      assert :eta_statem_sut.state(pid) == :on
      :eta_statem_sut.flip(pid)
      assert :eta_statem_sut.state(pid) == :off
    end

    test "terminate/3 is not mistaken for a state", %{pid: pid} do
      # It is the one exported arity-3 name in the module that is a callback
      # rather than a state. Wrapping it would put a `gen_statem` return shape
      # through a function whose return value means nothing, and a stop that does
      # not complete cleanly is how that would show.
      assert :eta_statem_sut.stop(pid) == :ok
      refute Process.alive?(pid)
    end
  end

  # ---------------------------------------------------------------------------
  # What is still refused
  # ---------------------------------------------------------------------------

  describe "functions eta_net does not implement" do
    test "gen_statem's asynchronous interface raises while a network is running" do
      :ok = @net.start(%{seed: 1})
      {:ok, _} = :eta_statem_echo.start_link(:async_statem)
      on_exit(fn -> stop_echo(:async_statem) end)

      result =
        try do
          {:ok, :eta_statem_echo.async(:async_statem, {:echo, 1})}
        rescue
          e in ErlangError -> {:error, e.original}
        end

      assert {:error, {:eta_net, {:unsupported, {:gen_statem, :send_request, 2}, _}}} = result
    end

    test "and passes straight through with no network" do
      {:ok, _} = :eta_statem_echo.start_link(:async_inert_statem)
      on_exit(fn -> stop_echo(:async_inert_statem) end)

      req = :eta_statem_echo.async(:async_inert_statem, {:echo, 1})
      assert {:reply, {:echoed, 1}} = :gen_statem.receive_response(req, 1_000)
    end
  end

  describe "at compile time" do
    test "a gen_statem module is accepted" do
      # It used to be refused outright, on the grounds that its replies come from
      # inside OTP. They come from inside the *return value*, which is a seam the
      # transform can reach.
      assert {:ok, _, _} =
               compile("""
               -behaviour(gen_statem).
               -export([callback_mode/0, init/1, handle_event/4]).
               callback_mode() -> handle_event_function.
               init([]) -> {ok, ready, []}.
               handle_event({call, From}, ping, ready, D) ->
                   {keep_state, D, [{reply, From, pong}]}.
               """)
    end

    test "a gen_event module is still refused" do
      # A handler's reply is produced inside the event manager, which is neither
      # the module the transform rewrote nor a process it can reach. Refused once
      # at compile time rather than per call site at run time.
      assert :error ==
               elem(
                 compile("""
                 -behaviour(gen_event).
                 -export([init/1]).
                 init([]) -> {ok, []}.
                 """),
                 0
               )
    end

    test "-eta_net(false) opts a gen_event module out of the pass" do
      assert {:ok, _, _} =
               compile("""
               -behaviour(gen_event).
               -eta_net(false).
               -export([init/1]).
               init([]) -> {ok, []}.
               """)
    end
  end

  # ---------------------------------------------------------------------------
  # Observability
  # ---------------------------------------------------------------------------

  describe "the observability pass" do
    test "publishes the data, not the state name" do
      {:ok, pid} = :eta_statem_observe_sut.start_link()

      assert :eta_observe.read(pid) == %{epoch: 0, leader: :undefined},
             "init/1's {ok, State, Data} has to publish the data"

      :eta_statem_observe_sut.bump(pid)
      :eta_statem_observe_sut.get(pid)

      assert :eta_observe.read(pid) == %{epoch: 1, leader: {:member, 1}},
             "`scratch` is declared on the record but not observed"

      :eta_statem_observe_sut.stop(pid)
    end

    test "republishes on every callback return" do
      {:ok, pid} = :eta_statem_observe_sut.start_link()

      for n <- 1..5 do
        :eta_statem_observe_sut.bump(pid)
        :eta_statem_observe_sut.get(pid)
        assert %{epoch: ^n} = :eta_observe.read(pid)
      end

      :eta_statem_observe_sut.stop(pid)
    end

    test "reads while the process is suspended" do
      {:ok, pid} = :eta_statem_observe_sut.start_link()
      :eta_statem_observe_sut.bump(pid)
      :eta_statem_observe_sut.get(pid)

      true = :erlang.suspend_process(pid)

      try do
        assert %{epoch: 1} = :eta_observe.read(pid)
      after
        :erlang.resume_process(pid)
      end

      :eta_statem_observe_sut.stop(pid)
    end
  end

  # Compiles a throwaway module through the transform, so a refusal can be
  # asserted on rather than described.
  defp compile(body) do
    name = :"eta_statem_probe_#{System.unique_integer([:positive])}"

    source = """
    -module(#{name}).
    #{body}
    """

    forms =
      source
      |> String.to_charlist()
      |> :erl_scan.string()
      |> then(fn {:ok, tokens, _} -> tokens end)
      |> Enum.chunk_while([], &chunk_form/2, fn acc -> {:cont, Enum.reverse(acc), []} end)
      |> Enum.reject(&(&1 == []))
      |> Enum.map(fn f ->
        {:ok, form} = :erl_parse.parse_form(f)
        form
      end)

    try do
      :compile.forms(forms, [:return_errors, {:parse_transform, :eta_transform}])
    catch
      _, _ -> {:error, :rejected}
    end
  end

  defp chunk_form({:dot, _} = tok, acc), do: {:cont, Enum.reverse([tok | acc]), []}
  defp chunk_form(tok, acc), do: {:cont, [tok | acc]}
end
