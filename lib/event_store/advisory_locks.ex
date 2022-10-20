defmodule EventStore.AdvisoryLocks do
  @moduledoc false

  # PostgreSQL provides a means for creating locks that have application-defined
  # meanings. Advisory locks are faster, avoid table bloat, and are
  # automatically cleaned up by the server at the end of the session.

  use GenServer

  alias EventStore.MonitoredServer

  defmodule State do
    @moduledoc false
    defstruct [:conn, :query_timeout, :ref, :schema, locks: %{}]

    def new(opts) do
      conn = Keyword.fetch!(opts, :conn)
      query_timeout = Keyword.fetch!(opts, :query_timeout)
      schema = Keyword.fetch!(opts, :schema)

      %State{conn: conn, query_timeout: query_timeout, schema: schema}
    end
  end

  defmodule Lock do
    @moduledoc false

    @type t :: %Lock{
            key: non_neg_integer(),
            owner: pid(),
            ref: reference()
          }
    defstruct [:key, :owner, :ref]

    def new(key, owner) do
      %Lock{key: key, owner: owner, ref: make_ref()}
    end

    def ref(%Lock{ref: ref}), do: ref
  end

  alias EventStore.AdvisoryLocks.{Lock, State}
  alias EventStore.Storage

  def start_link(opts) do
    {start_opts, advisory_locks_opts} =
      Keyword.split(opts, [:name, :timeout, :debug, :spawn_opt, :hibernate_after])

    state = State.new(advisory_locks_opts)

    GenServer.start_link(__MODULE__, state, start_opts)
  end

  def init(%State{} = state) do
    %State{conn: conn} = state

    {:ok, ref} = MonitoredServer.monitor(conn)

    {:ok, %State{state | ref: ref}}
  end

  @doc """
  Attempt to obtain an advisory lock.

     - `key` - an application specific integer to acquire a lock on.
     - `timeout` - an integer greater than zero which specifies how many
        milliseconds to wait for a reply, or the atom `:infinity` to wait
        indefinitely.

  Returns `{:ok, lock}` when lock successfully acquired, or
  `{:error, :lock_already_taken}` if the lock cannot be acquired immediately.

  ## Lock released

  An `{EventStore.AdvisoryLocks, :lock_released, lock, reason}` message will be
  sent to the lock owner when the lock is released, usually due to the database
  connection terminating. It is up to the lock owner to attempt to reacquire the
  lost lock.

  """
  @spec try_advisory_lock(server :: GenServer.server(), key :: non_neg_integer(), timeout()) ::
          {:ok, reference()} | {:error, :lock_already_taken} | {:error, term}
  def try_advisory_lock(server, key, timeout \\ 5_000) when is_integer(key) do
    GenServer.call(server, {:try_advisory_lock, key, self(), timeout}, timeout)
  end

  def handle_call({:try_advisory_lock, key, owner, timeout}, _from, %State{} = state) do
    case try_acquire_exclusive_lock(key, owner, timeout, state) do
      {:ok, %Lock{} = lock} ->
        state = monitor_acquired_lock(lock, state)

        {:reply, {:ok, Lock.ref(lock)}, state}

      reply ->
        {:reply, reply, state}
    end
  end

  defp try_acquire_exclusive_lock(key, owner, timeout, %State{} = state) do
    %State{conn: conn, schema: schema} = state

    with :ok <-
           Storage.Lock.try_acquire_exclusive_lock(conn, key, schema: schema, timeout: timeout) do
      {:ok, Lock.new(key, owner)}
    end
  end

  defp monitor_acquired_lock(%Lock{} = lock, %State{locks: locks} = state) do
    %Lock{owner: owner} = lock

    owner_ref = Process.monitor(owner)

    %State{state | locks: Map.put(locks, owner_ref, lock)}
  end

  # Lost locks when database connection goes down.
  def handle_info({:DOWN, ref, :process, _pid, reason}, %State{ref: ref} = state) do
    %State{locks: locks} = state

    notify_lost_locks(locks, reason)

    {:noreply, %State{state | locks: %{}}}
  end

  # Release lock when the lock owner process terminates.
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %State{} = state) do
    %State{locks: locks} = state

    state =
      case Map.get(locks, ref) do
        nil ->
          state

        %Lock{key: key} ->
          release_lock(key, state)

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

  defp release_lock(key, %State{} = state) do
    %State{conn: conn, query_timeout: query_timeout, schema: schema} = state

    Storage.Lock.unlock(conn, key, schema: schema, timeout: query_timeout)
  end
end
