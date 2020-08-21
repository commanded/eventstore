defmodule EventStore.Config.Store do
  @moduledoc false

  # Store and read runtime configuration associated with an event store.

  use GenServer

  def start_link(init_arg) do
    GenServer.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  def associate(event_store, pid, config) when is_atom(event_store) and is_pid(pid) do
    GenServer.call(__MODULE__, {:associate, event_store, pid, config})
  end

  @doc """
  Get the configuration associated with the given event store.
  """
  def get(event_store) when is_atom(event_store), do: lookup(event_store)

  @doc """
  Get the value of the config setting for the given event store.
  """
  def get(event_store, setting) when is_atom(event_store) and is_atom(setting) do
    event_store |> lookup() |> Keyword.get(setting)
  end

  @impl GenServer
  def init(_init_arg) do
    table = :ets.new(__MODULE__, [:named_table, read_concurrency: true])

    {:ok, table}
  end

  @impl GenServer
  def handle_call({:associate, event_store, pid, config}, _from, table) do
    ref = Process.monitor(pid)
    true = :ets.insert(table, {event_store, pid, ref, config})

    {:reply, :ok, table}
  end

  @impl GenServer
  def handle_info({:DOWN, ref, _type, pid, _reason}, table) do
    [[event_store]] = :ets.match(table, {:"$1", pid, ref, :_})

    :ets.delete(table, event_store)

    {:noreply, table}
  end

  defp lookup(event_store) do
    :ets.lookup_element(__MODULE__, event_store, 4)
  rescue
    ArgumentError ->
      raise "could not lookup #{inspect(event_store)} because it was not started or it does not exist"
  end
end
