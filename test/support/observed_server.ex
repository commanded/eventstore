defmodule EventStore.ObservedServer do
  @moduledoc false

  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, Keyword.take(opts, [:reply_to]), Keyword.take(opts, [:name]))
  end

  def init(state) do
    {:ok, state}
  end

  def handle_call(:ping, _from, state) do
    {:reply, {:ok, :pong}, state}
  end

  def handle_cast(:ping, [reply_to: reply_to] = state) do
    send(reply_to, :pong)

    {:noreply, state}
  end

  def handle_info(:ping, [reply_to: reply_to] = state) do
    send(reply_to, :pong)

    {:noreply, state}
  end
end
