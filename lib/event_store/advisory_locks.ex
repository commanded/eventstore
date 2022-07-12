defmodule EventStore.AdvisoryLocks do
  @moduledoc false

  # PostgreSQL provides a means for creating locks that have application-defined
  # meanings. Advisory locks are faster, avoid table bloat, and are
  # automatically cleaned up by the server at the end of the session.

  use GenServer

  alias EventStore.MonitoredServer

  defmodule State do
    @moduledoc false
    defstruct [:conn, :conn_ref, :retry_interval, :schema, locks: %{}]
  end

  defmodule Lock do
    @moduledoc false

    @type t :: %Lock{
            acquired?: boolean(),
            key: non_neg_integer(),
            owner: pid(),
            owner_ref: reference(),
            lock_ref: reference()
          }
    defstruct [:acquired?, :key, :lock_ref, :owner, :owner_ref]

    def new(key, owner, acquired?) do
      %Lock{
        acquired?: acquired?,
        key: key,
        owner: owner,
        owner_ref: Process.monitor(owner),
        lock_ref: make_ref()
      }
    end

    def lock_ref(%Lock{lock_ref: lock_ref}), do: lock_ref
  end

  alias EventStore.AdvisoryLocks.{Lock, State}
  alias EventStore.Storage

  @doc """
  Attempt to obtain an advisory lock.

     - `key` - an application specific integer to acquire a lock on.

  Returns `{:ok, lock_ref}` when lock successfully acquired, or
  `{:eventually, lock_ref}` if the lock cannot be immediately acquired. It may
  eventually be acquired and the owner process will be notified.

  ## Lock acquired

  An `{EventStore.AdvisoryLocks, :lock_acquired, lock_ref}` message will be
  sent to the lock owner when the lock is eventually acquired.

  ## Lock released

  An `{EventStore.AdvisoryLocks, :lock_released, lock_ref, reason}` message will
  be sent to the lock owner when the lock is released, usually due to the
  database connection terminating. The advisory locks will attempt to reaquire
  the lost lock.
  """
  @spec try_advisory_lock(server :: GenServer.server(), key :: non_neg_integer()) ::
          {:ok, reference()} | {:eventually, reference()}
  def try_advisory_lock(server, key) when is_integer(key) do
    GenServer.call(server, {:try_advisory_lock, key, self()})
  end

  def start_link(opts) do
    {start_opts, advisory_locks_opts} =
      Keyword.split(opts, [:name, :timeout, :debug, :spawn_opt, :hibernate_after])

    conn = Keyword.fetch!(advisory_locks_opts, :conn)
    schema = Keyword.fetch!(advisory_locks_opts, :schema)
    retry_interval = Keyword.fetch!(advisory_locks_opts, :retry_interval)

    state = %State{conn: conn, retry_interval: retry_interval, schema: schema}

    GenServer.start_link(__MODULE__, state, start_opts)
  end

  @impl GenServer
  def init(%State{} = state) do
    %State{conn: conn} = state

    {:ok, conn_ref} = MonitoredServer.monitor(conn)

    {:ok, %State{state | conn_ref: conn_ref}}
  end

  @impl GenServer
  def handle_call({:try_advisory_lock, key, owner}, _from, %State{} = state) do
    %State{locks: locks, retry_interval: retry_interval} = state

    case try_acquire_exclusive_lock(key, state) do
      :ok ->
        lock = Lock.new(key, owner, true)
        lock_ref = Lock.lock_ref(lock)

        state = %State{state | locks: Map.put(locks, lock_ref, lock)}

        {:reply, {:ok, lock_ref}, state}

      {:error, :lock_already_taken} ->
        lock = Lock.new(key, owner, false)
        lock_ref = Lock.lock_ref(lock)

        state = %State{state | locks: Map.put(locks, lock_ref, lock)}

        Process.send_after(self(), {:try_acquire_lock, lock_ref}, retry_interval)

        {:reply, {:eventually, lock_ref}, state}

      {:error, _error} = reply ->
        {:reply, reply, state}
    end
  end

  # Attempt to acquire a requested advisory lock which was unavailable.
  @impl GenServer
  def handle_info({:try_acquire_lock, lock_ref}, %State{} = state) do
    %State{locks: locks, retry_interval: retry_interval} = state

    case Map.get(locks, lock_ref) do
      %Lock{} = lock ->
        %Lock{key: key, owner: owner} = lock

        case try_acquire_exclusive_lock(key, state) do
          :ok ->
            :ok = Process.send(owner, {__MODULE__, :lock_acquired, lock_ref}, [])

            lock = %Lock{lock | acquired?: true}
            state = %State{state | locks: Map.put(locks, lock_ref, lock)}

            {:noreply, state}

          {:error, _error} ->
            Process.send_after(self(), {:try_acquire_lock, lock_ref}, retry_interval)

            {:noreply, state}
        end

      nil ->
        {:noreply, state}
    end
  end

  # Notify locks lost when database connection goes down and attempt to
  # reacquire any acquired locks after retry interval.
  @impl GenServer
  def handle_info({:DOWN, ref, :process, _pid, reason}, %State{conn_ref: ref} = state) do
    %State{locks: locks, retry_interval: retry_interval} = state

    notify_locks_lost(reason, state)

    locks =
      Enum.into(locks, %{}, fn
        {lock_ref, %Lock{acquired?: true} = lock} ->
          Process.send_after(self(), {:try_acquire_lock, lock_ref}, retry_interval)

          {lock_ref, %Lock{lock | acquired?: false}}

        {lock_ref, %Lock{} = lock} ->
          {lock_ref, lock}
      end)

    state = %State{state | locks: locks}

    {:noreply, state}
  end

  # Release lock when the lock owner process terminates.
  @impl GenServer
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %State{} = state) do
    %State{locks: locks} = state

    state =
      for {lock_ref, %Lock{owner_ref: ^ref} = lock} <- locks do
        release_lock(lock, state)

        %State{state | locks: Map.delete(locks, lock_ref)}
      end

    {:noreply, state}
  end

  # Notify locks lost when advisory locks process terminates.
  @impl GenServer
  def terminate(reason, %State{} = state) do
    notify_locks_lost(reason, state)

    state
  end

  defp try_acquire_exclusive_lock(key, %State{} = state) do
    %State{conn: conn, schema: schema} = state

    Storage.Lock.try_acquire_exclusive_lock(conn, key, schema: schema)
  end

  defp notify_locks_lost(reason, %State{} = state) do
    %State{locks: locks} = state

    for {_lock_ref, %Lock{acquired?: true} = lock} <- locks do
      %Lock{owner: owner, lock_ref: lock_ref} = lock

      :ok = Process.send(owner, {__MODULE__, :lock_released, lock_ref, reason}, [])
    end

    :ok
  end

  defp release_lock(%Lock{} = lock, %State{} = state) do
    %Lock{acquired?: acquired?, key: key} = lock
    %State{conn: conn, schema: schema} = state

    if acquired? do
      Storage.Lock.unlock(conn, key, schema: schema)
    else
      :ok
    end
  end
end
