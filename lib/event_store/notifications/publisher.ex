defmodule EventStore.Notifications.Publisher do
  @moduledoc false

  # Reads events from storage by each event number range received and publishes
  # them.

  use GenStage

  require Logger

  alias EventStore.{PubSub, RecordedEvent, Storage}
  alias EventStore.Notifications.Notification

  defmodule State do
    defstruct [:conn, :event_store, :schema, :serializer, :subscribe_to]

    def new(opts) do
      %State{
        conn: Keyword.fetch!(opts, :conn),
        event_store: Keyword.fetch!(opts, :event_store),
        schema: Keyword.fetch!(opts, :schema),
        serializer: Keyword.fetch!(opts, :serializer),
        subscribe_to: Keyword.fetch!(opts, :subscribe_to)
      }
    end
  end

  def start_link(opts) do
    {start_opts, reader_opts} =
      Keyword.split(opts, [:name, :timeout, :debug, :spawn_opt, :hibernate_after])

    state = State.new(reader_opts)

    GenStage.start_link(__MODULE__, state, start_opts)
  end

  # Starts a permanent subscription to the listener producer stage which will
  # automatically start requesting items.
  def init(%State{} = state) do
    %State{subscribe_to: subscribe_to} = state

    {:consumer, state, [subscribe_to: [{subscribe_to, max_demand: 1}]]}
  end

  # Fetch events from storage and pass onwards to subscibers
  def handle_events(events, _from, state) do
    %State{event_store: event_store} = state

    events
    |> Stream.map(&read_events(&1, state))
    |> Stream.reject(&is_nil/1)
    |> Enum.each(fn {stream_uuid, batch} -> broadcast(event_store, stream_uuid, batch) end)

    {:noreply, [], state}
  end

  defp read_events(%Notification{} = notification, %State{} = state) do
    %Notification{
      stream_uuid: stream_uuid,
      stream_id: stream_id,
      from_stream_version: from_stream_version,
      to_stream_version: to_stream_version
    } = notification

    %State{conn: conn, schema: schema, serializer: serializer} = state

    count = to_stream_version - from_stream_version + 1

    try do
      case Storage.read_stream_forward(conn, stream_id, from_stream_version, count, schema: schema) do
        {:ok, events} ->
          deserialized_events = deserialize_recorded_events(events, serializer)

          {stream_uuid, deserialized_events}

        {:error, error} ->
          Logger.error(
            "EventStore notifications failed to read events due to: " <> inspect(error)
          )

          nil
      end
    catch
      :exit, ex ->
        Logger.error("EventStore notifications failed to read events due to: " <> inspect(ex))
        nil
    end
  end

  defp deserialize_recorded_events(recorded_events, serializer) do
    Enum.map(recorded_events, &RecordedEvent.deserialize(&1, serializer))
  end

  defp broadcast(event_store, stream_uuid, events) do
    PubSub.broadcast(event_store, stream_uuid, {:events, events})
  end
end
