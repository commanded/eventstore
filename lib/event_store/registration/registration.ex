defmodule EventStore.Registration do
  @moduledoc """
  Registration specification for EventStore pub/sub.
  """

  alias EventStore.Registration.{DistributedRegistry, LocalRegistry}

  @doc """
  Return an optional supervisor spec for the registry.
  """
  @callback child_spec() :: [:supervisor.child_spec()]

  @doc """
  Subscribes the caller to the given topic.
  """
  @callback subscribe(binary, mapper: (RecordedEvent.t() -> any())) :: :ok | {:error, term}

  @doc """
  Broadcasts message on given topic.
  """
  @callback broadcast(binary, term) :: :ok | {:error, term}

  @doc """
  Return an optional supervisor spec for the registry.
  """
  @spec child_spec() :: [:supervisor.child_spec()]
  def child_spec, do: registry_provider().child_spec()

  @doc """
  Subscribes the caller to the given topic.
  """
  @spec subscribe(binary, mapper: (RecordedEvent.t() -> any())) :: :ok | {:error, term}
  def subscribe(topic, opts \\ []), do: registry_provider().subscribe(topic, opts)

  @doc """
  Broadcasts message on given topic.
  """
  @spec broadcast(binary, term) :: :ok | {:error, term}
  def broadcast(topic, message), do: registry_provider().broadcast(topic, message)

  @doc """
  Get the configured registry provider, defaults to `:local` if not configured.
  """
  def registry_provider do
    case Application.get_env(:eventstore, :registry, :local) do
      :local ->
        LocalRegistry

      :distributed ->
        DistributedRegistry

      unknown ->
        raise ArgumentError, message: "Unknown `:registry` setting in config: #{inspect(unknown)}"
    end
  end
end
