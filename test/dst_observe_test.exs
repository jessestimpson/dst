defmodule DstObserveTest do
  @moduledoc """
  `dst_observe` — reading a process's state without asking it.

  An invariant runs against a frozen system, so `check/1` cannot call into a
  `gen_server`. The transform's answer is to have the process publish on every
  callback return, into its own process dictionary, where `process_info/2` reads
  it from outside. These tests cover the 2 declaration forms and the property
  that matters most: that it works on a **suspended** process.
  """
  use ExUnit.Case, async: false

  describe "{Record, Fields}" do
    test "publishes the named fields, and only those" do
      {:ok, pid} = :dst_observe_sut.start_link()

      assert :dst_observe.read(pid) == %{epoch: 0, leader: :undefined}

      :dst_observe_sut.bump(pid)
      :dst_observe_sut.get(pid)

      assert :dst_observe.read(pid) == %{epoch: 1, leader: {:member, 1}},
             "`scratch` is declared on the record but not observed, so it must not appear"

      GenServer.stop(pid)
    end

    test "republishes on every callback return" do
      {:ok, pid} = :dst_observe_sut.start_link()

      for n <- 1..5 do
        :dst_observe_sut.bump(pid)
        :dst_observe_sut.get(pid)
        assert %{epoch: ^n} = :dst_observe.read(pid)
      end

      GenServer.stop(pid)
    end

    test "a bare field list is refused, rather than assuming #state{}" do
      # The form this replaced. It used to mean "fields of `#state{}`", so a
      # module whose record was named anything else failed with a complaint
      # about a record it had never declared.
      assert {:error, _, _} = compile("-dst_observe([epoch]).")
    end

    test "an unknown field is a compile error, not a wrong element/2 offset" do
      assert {:error, _, _} = compile("-dst_observe({st, [nope]}).")
    end
  end

  describe "all" do
    test "publishes the whole state, even when it is not a record" do
      {:ok, pid} = :dst_observe_all_sut.start_link()

      assert :dst_observe.read(pid) == %{epoch: 0}

      :dst_observe_all_sut.bump(pid)
      :dst_observe_all_sut.get(pid)
      assert :dst_observe.read(pid) == %{epoch: 1}

      GenServer.stop(pid)
    end
  end

  describe "reading" do
    test "works while the process is suspended" do
      # The property the whole module exists for. A suspended process cannot
      # answer a call, and `read/1` never asks it one.
      {:ok, pid} = :dst_observe_sut.start_link()
      :dst_observe_sut.bump(pid)
      :dst_observe_sut.get(pid)

      true = :erlang.suspend_process(pid)

      try do
        assert %{epoch: 1} = :dst_observe.read(pid)
        assert catch_exit(GenServer.call(pid, :get, 50)) != nil, "the call must not be answerable"
      after
        :erlang.resume_process(pid)
      end

      GenServer.stop(pid)
    end

    test "a process that never published, and a dead one, both read undefined" do
      assert :dst_observe.read(self()) == :undefined
      assert :dst_observe.read(:no_such_registered_name) == :undefined

      pid = spawn(fn -> :ok end)
      Process.sleep(10)
      assert :dst_observe.read(pid) == :undefined
    end
  end

  # Compiles a throwaway module carrying `attr`, so a rejected `-dst_observe`
  # declaration can be asserted on rather than described.
  defp compile(attr) do
    name = :"dst_observe_probe_#{System.unique_integer([:positive])}"

    source = """
    -module(#{name}).
    -record(st, {epoch = 0}).
    #{attr}
    -export([init/1]).
    init([]) -> {ok, #st{}}.
    """

    {:ok, tokens} =
      source
      |> String.to_charlist()
      |> :erl_scan.string()
      |> then(fn {:ok, ts, _} -> {:ok, ts} end)

    forms =
      tokens
      |> Enum.chunk_while([], &chunk_form/2, fn acc -> {:cont, Enum.reverse(acc), []} end)
      |> Enum.reject(&(&1 == []))
      |> Enum.map(fn f -> {:ok, form} = :erl_parse.parse_form(f); form end)

    try do
      :compile.forms(forms, [:return_errors, {:parse_transform, :dst_transform}])
    catch
      _, _ -> {:error, :rejected}
    end
  end

  defp chunk_form({:dot, _} = tok, acc), do: {:cont, Enum.reverse([tok | acc]), []}
  defp chunk_form(tok, acc), do: {:cont, [tok | acc]}
end
