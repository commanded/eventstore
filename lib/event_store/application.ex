defmodule EventStore.Application do
  @moduledoc false
  use Application

  alias EventStore.Config

  def start(_, _) do
    EventStore.Supervisor.start_link(Config.parsed())
  end
end
