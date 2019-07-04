defmodule EventStore.AdvisoryLocks do
  @moduledoc false

  # PostgreSQL provides a means for creating locks that have application-defined
  # meanings. Advisory locks are faster, avoid table bloat, and are
  # automatically cleaned up by the server at the end of the session.

  use GenServer

  alias EventStore.MonitoredServer

  defmodule State do
    @moduledoc false
    defstruct [:conn, state: :connected, locks: %{}]
  end

  defmodule Lock do
    @moduledoc false

    @type t :: %Lock{
            key: non_neg_integer(),
            owner: pid(),
            ref: reference()
          }
    defstruct [:key, :owner, :ref]
  end

  alias EventStore.AdvisoryLocks.{Lock, State}
  alias EventStore.Storage

  def start_link(opts) do
    conn = Keyword.fetch!(opts, :conn)
    name = Keyword.get(opts, :name, __MODULE__)

    state = %State{conn: conn}

    GenServer.start_link(__MODULE__, state, name: name)
  end

  def init(%State{} = state) do
    %State{conn: conn} = state

    :ok = MonitoredServer.monitor(conn)

    {:ok, state}
  end

  @doc """
  Attempt to obtain an advisory lock.

     - `key` - an application specific integer to acquire a lock on.

  Returns `{:ok, lock}` when lock successfully acquired, or
  `{:error, :lock_already_taken}` if the lock cannot be acquired immediately.

  ## Lock released

  A `{EventStore.AdvisoryLocks, :lock_released, lock, reason}` message will be
  sent to the lock owner when the lock is released, usually due to the database
  connection terminating. It is up to the lock owner to attempt to reacquire the
  lost lock.

  """
  @spec try_advisory_lock(server :: GenServer.server(), key :: non_neg_integer()) ::
          {:ok, reference()} | {:error, :lock_already_taken} | {:error, term}
  def try_advisory_lock(server, key) when is_integer(key) do
    GenServer.call(server, {:try_advisory_lock, key, self()})
  end

  def handle_call({:try_advisory_lock, key, owner}, _from, %State{} = state) do
    %State{conn: conn} = state

    case try_acquire_exclusive_lock(conn, key, owner) do
      {:ok, %Lock{ref: lock_ref} = lock} ->
        state = monitor_acquired_lock(lock, state)

        {:reply, {:ok, lock_ref}, state}

      reply ->
        {:reply, reply, state}
    end
  end

  defp try_acquire_exclusive_lock(conn, key, owner) do
    case Storage.Lock.try_acquire_exclusive_lock(conn, key) do
      :ok ->
        lock = %Lock{key: key, owner: owner, ref: make_ref()}

        {:ok, lock}

      {:error, error} ->
        {:error, error}
    end
  end

  defp monitor_acquired_lock(%Lock{} = lock, %State{locks: locks} = state) do
    %Lock{owner: owner} = lock

    owner_ref = Process.monitor(owner)

    %State{state | locks: Map.put(locks, owner_ref, lock)}
  end

  # Database connection has come up.
  def handle_info({:UP, conn, _pid}, %State{conn: conn} = state) do
    {:noreply, %State{state | state: :connected}}
  end

  # Lost locks when database connection goes down.
  def handle_info({:DOWN, conn, _pid, reason}, %State{conn: conn} = state) do
    %State{locks: locks} = state

    notify_lost_locks(locks, reason)

    {:noreply, %State{state | locks: %{}, state: :disconnected}}
  end

  # Release lock when the lock owner process terminates.
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %State{} = state) do
    %State{locks: locks} = state

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

  defp notify_lost_locks(locks, reason) do
    for {_ref, %Lock{} = lock} <- locks do
      %Lock{owner: owner, ref: ref} = lock

      :ok = Process.send(owner, {__MODULE__, :lock_released, ref, reason}, [])
    end

    :ok
  end

  defp release_lock(key, %State{state: :connected} = state) do
    %State{conn: conn} = state

    Storage.Lock.unlock(conn, key)
  end

  defp release_lock(_key, %State{}), do: :ok
end
