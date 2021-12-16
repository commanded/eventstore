defmodule EventStore.Config.Store do
  @moduledoc false

  # Store and read runtime configuration associated with an event store.

  use GenServer

  def start_link(init_arg) do
    GenServer.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  def associate(name, pid, event_store, config)
      when is_atom(name) and is_pid(pid) and is_atom(event_store) do
    GenServer.call(__MODULE__, {:associate, name, pid, event_store, config})
  end

  @doc """
  Get the configuration associated with all running event store instances.
  """
  def all do
    :ets.tab2list(__MODULE__)
    |> Enum.map(fn {name, _pid, _ref, event_store, _config} -> {event_store, name} end)
  end

  @doc """
  Get the configuration associated with the given event store.
  """
  def get(name) when is_atom(name), do: lookup(name)

  @doc """
  Get the value of the config setting for the given event store.
  """
  def get(name, setting) when is_atom(name) and is_atom(setting) do
    lookup(name) |> Keyword.get(setting)
  end

  @impl GenServer
  def init(_init_arg) do
    table = :ets.new(__MODULE__, [:named_table, read_concurrency: true])

    {:ok, table}
  end

  @impl GenServer
  def handle_call({:associate, name, pid, event_store, config}, _from, table) do
    ref = Process.monitor(pid)
    true = :ets.insert(table, {name, pid, ref, event_store, config})

    {:reply, :ok, table}
  end

  @impl GenServer
  def handle_info({:DOWN, ref, _type, pid, _reason}, table) do
    [[name]] = :ets.match(table, {:"$1", pid, ref, :_, :_})

    :ets.delete(table, name)

    {:noreply, table}
  end

  defp lookup(name) do
    :ets.lookup_element(__MODULE__, name, 5)
  rescue
    ArgumentError ->
      raise "could not lookup #{inspect(name)} because it was not started or it does not exist"
  end
end
