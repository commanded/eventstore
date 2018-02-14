defmodule EventStore.AdvisoryLocks do
  @moduledoc false

  # PostgreSQL provides a means for creating locks that have application-defined
  # meanings. Advisory locks are faster, avoid table bloat, and are
  # automatically cleaned up by the server at the end of the session.

  use GenServer

  defmodule State do
    defstruct locks: %{}
  end

  alias EventStore.AdvisoryLocks.State
  alias EventStore.Storage.Lock

  def start_link(_args) do
    GenServer.start_link(__MODULE__, %State{}, name: __MODULE__)
  end

  def init(%State{} = state) do
    {:ok, state}
  end

  @doc """
  Attempt to obtain an advisory lock.

  Returns `:ok` on success, or `{:error, :lock_already_taken}` if the lock
  cannot be acquired immediately.
  """
  @spec try_advisory_lock(conn :: pid, key :: non_neg_integer()) ::
          :ok | {:error, :lock_already_taken} | {:error, term}
  def try_advisory_lock(conn, key) when is_integer(key) do
    case Lock.try_acquire_exclusive_lock(conn, key) do
      :ok ->
        GenServer.cast(__MODULE__, {:monitor_lock, conn, key, self()})
        :ok

      reply ->
        reply
    end
  end

  @doc false
  def locks, do: GenServer.call(__MODULE__, :locks)

  @doc false
  # Monitor the connection process and the locking process to release any
  # acquired advisory locks when the process terminates
  def handle_cast({:monitor_lock, conn, key, pid}, %State{locks: locks} = state) do
    locks =
      case Map.get(locks, conn, []) do
        [] ->
          Process.monitor(conn)
          
          Map.put(locks, conn, [{pid, key, Process.monitor(pid)}])

        keys ->
          ref =
            case Enum.find(keys, fn {lock_pid, _key, _ref} -> lock_pid == pid end) do
              nil -> Process.monitor(pid)
              {_pid, _key, ref} -> ref
            end

          Map.put(locks, conn, [{pid, key, ref} | keys])
      end

    {:noreply, %State{state | locks: locks}}
  end

  def handle_call(:locks, _from, %State{locks: locks} = state), do: {:reply, locks, state}

  def handle_info({:DOWN, _ref, :process, pid, _reason}, %State{locks: locks} = state) do
    locks =
      case Map.has_key?(locks, pid) do
        true ->
          # connection has terminated, any locks will be released by Postgres

          for {_pid, _key, ref} <- Map.get(locks, pid) do
            Process.demonitor(ref)
          end

          Map.delete(locks, pid)

        false ->
          Enum.reduce(locks, %{}, fn {conn, keys}, locks ->
            keys =
              Enum.reduce(keys, [], fn
                {^pid, key, _ref}, keys ->
                  # release lock when locking process terminates
                  :ok = Lock.unlock(conn, key)

                  keys

                pair, keys ->
                  [pair | keys]
              end)

            Map.put(locks, conn, keys)
          end)
      end

    {:noreply, %State{state | locks: locks}}
  end
end
