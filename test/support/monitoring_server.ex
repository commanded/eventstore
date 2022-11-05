defmodule EventStore.MonitoringServer do
  @moduledoc false

  use GenServer

  alias alias EventStore.MonitoredServer

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

  def handle_call({:monitor, name}, _from, reply_to) do
    {:reply, MonitoredServer.monitor(name), reply_to}
  end
end
