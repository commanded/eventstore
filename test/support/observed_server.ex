defmodule EventStore.ObservedServer do
  @moduledoc false

  use GenServer

  def start_link(opts) do
    {start_opts, observer_opts} =
      Keyword.split(opts, [:name, :timeout, :debug, :spawn_opt, :hibernate_after])

    GenServer.start_link(__MODULE__, observer_opts, start_opts)
  end

  def init(opts) do
    reply_to = Keyword.fetch!(opts, :reply_to)

    send(reply_to, {:init, self()})

    if Keyword.get(opts, :start_successfully, true) do
      {:ok, reply_to}
    else
      {:error, :failed}
    end
  end

  def handle_call(:ping, _from, reply_to) do
    {:reply, {:ok, :pong}, reply_to}
  end

  def handle_cast(:ping, reply_to) do
    send(reply_to, :pong)

    {:noreply, reply_to}
  end

  def handle_info(:ping, reply_to) do
    send(reply_to, :pong)

    {:noreply, reply_to}
  end
end
