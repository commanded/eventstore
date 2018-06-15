defmodule EventStore.AdvisoryLocks do
  @moduledoc false

  # PostgreSQL provides a means for creating locks that have application-defined
  # meanings. Advisory locks are faster, avoid table bloat, and are
  # automatically cleaned up by the server at the end of the session.

  use GenServer

  defmodule State do
    @moduledoc false
    defstruct conn: nil, state: :connected, locks: %{}
  end

  defmodule Lock do
    @moduledoc false
    defstruct key: nil, opts: nil
  end

  alias EventStore.AdvisoryLocks.{Lock, State}
  alias EventStore.Storage

  def start_link(conn) do
    GenServer.start_link(__MODULE__, %State{conn: conn}, name: __MODULE__)
  end

  def init(%State{} = state) do
    {:ok, state}
  end

  @doc """
  Attempt to obtain an advisory lock.

     - `key` - an application specific integer to acquire a lock on.
     - `opts` an optional keyword list:
        - `lock_released` - a 0-arity function called when the lock is released
          (usually due to a lost database connection).
        - `lock_reacquired` - a 0-arity function called when the lock has been
          successfully reacquired.

  Returns `:ok` when lock successfully acquired, or
  `{:error, :lock_already_taken}` if the lock cannot be acquired immediately.

  """
  @spec try_advisory_lock(key :: non_neg_integer(), opts :: list) ::
          :ok | {:error, :lock_already_taken} | {:error, term}
  def try_advisory_lock(key, opts \\ []) when is_integer(key) do
    GenServer.call(__MODULE__, {:try_advisory_lock, key, self(), opts})
  end

  def disconnect do
    GenServer.cast(__MODULE__, :disconnect)
  end

  def reconnect do
    GenServer.cast(__MODULE__, :reconnect)
  end

  def handle_call({:try_advisory_lock, key, pid, opts}, _from, %State{} = state) do
    %State{conn: conn} = state

    case Storage.Lock.try_acquire_exclusive_lock(conn, key) do
      :ok ->
        {:reply, :ok, monitor_lock(key, pid, opts, state)}

      reply ->
        {:reply, reply, state}
    end
  end

  def handle_cast(:disconnect, %State{locks: locks} = state) do
    for {_ref, %Lock{opts: opts}} <- locks do
      :ok = notify(:lock_released, opts)
    end

    {:noreply, %State{state | state: :disconnected}}
  end

  def handle_cast(:reconnect, %State{} = state) do
    %State{conn: conn, locks: locks} = state

    for {_ref, %Lock{key: key, opts: opts}} <- locks do
      with :ok <- Storage.Lock.try_acquire_exclusive_lock(conn, key),
           :ok <- notify(:lock_reacquired, opts) do
        :ok
      else
        {:error, :lock_already_taken} -> :ok
      end
    end

    {:noreply, %State{state | state: :connected}}
  end

  defp notify(notification, opts) do
    case Keyword.get(opts, notification) do
      fun when is_function(fun, 0) ->
        apply(fun, [])

      _ ->
        :ok
    end
  end

  defp monitor_lock(key, pid, opts, %State{locks: locks} = state) do
    ref = Process.monitor(pid)

    %State{state | locks: Map.put(locks, ref, %Lock{key: key, opts: opts})}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %State{locks: locks} = state) do
    state =
      case Map.get(locks, ref) do
        nil ->
          state

        %Lock{key: key} ->
          :ok = release_lock(key, state)

          %State{state | locks: Map.delete(locks, ref)}
      end

    {:noreply, state}
  end

  defp release_lock(key, %State{conn: conn, state: state}) do
    case state do
      :connected -> Storage.Lock.unlock(conn, key)
      _ -> :ok
    end
  end
end
