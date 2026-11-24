defmodule EventStore.Notifications.Publisher do
  @moduledoc """
  A GenStage consumer which subscribes to a `EventStore.Notifications.Listener` and receives
  `EventStore.Notifications.Notification` events, reading the corresponding range of events from
  storage, deserializing them, and broadcasting them to PubSub subscribers for the relevant storage
  stream.

  Each append produces two notifications: one for its stream and one for `$all`. Rather than
  reading each notification's range with its own query, the publisher reads the `$all` stream
  ahead and serves notifications from the events it has already read:

    * A `$all` notification which is not covered by the most recent reads causes the `$all`
      stream to be read, in one query, from where the previous read ended through the end of the
      notification and on to any events committed since (at least 1,000 events). The
      notifications which follow are then served without querying the database. `$all` stream
      versions are allocated while holding a lock on the `$all` stream row until the transaction
      commits, so the committed events contain no gaps.

    * A stream's notification is only read when the stream has subscribers. Its events are taken
      from the same `$all` reads and numbered by their stream version. When they are not covered,
      the `$all` stream is read ahead once more.

    * When the reads cannot serve a notification (e.g. after the publisher starts, or when a read
      fails) its range is read from its own stream, as before.

  Each topic is still sent exactly one broadcast per notification, containing only the
  notification's range, in the order the notifications are received. A persistent subscription
  which has caught up from storage ignores a broadcast starting with an event it has already
  seen, so a broadcast must never extend beyond its notification's range.
  """

  use GenStage

  require Logger

  alias EventStore.{PubSub, RecordedEvent, Storage}
  alias EventStore.Notifications.Notification

  @all_stream "$all"
  @read_ahead 1_000

  defmodule State do
    defstruct [
      :conn,
      :event_store,
      :query_timeout,
      :schema,
      :serializer,
      :subscribe_to,
      :last_all_stream_version,
      correlation_id_type: "uuid",
      causation_id_type: "uuid",
      # Events from the two most recent `$all` stream reads, grouped by stream (`$all` included)
      recent_reads: {%{}, %{}}
    ]

    def new(opts) do
      %State{
        conn: Keyword.fetch!(opts, :conn),
        event_store: Keyword.fetch!(opts, :event_store),
        query_timeout: Keyword.fetch!(opts, :query_timeout),
        schema: Keyword.fetch!(opts, :schema),
        serializer: Keyword.fetch!(opts, :serializer),
        subscribe_to: Keyword.fetch!(opts, :subscribe_to),
        correlation_id_type: Keyword.get(opts, :correlation_id_type, "uuid"),
        causation_id_type: Keyword.get(opts, :causation_id_type, "uuid")
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

  # Fetch events from storage and pass onwards to subscribers
  def handle_events(notifications, _from, state) do
    state = Enum.reduce(notifications, state, &publish/2)

    {:noreply, [], state}
  end

  # Always publish to the `$all` stream, but only publish to a single stream when it has subscribers.
  defp publish(%Notification{stream_uuid: stream_uuid} = notification, %State{} = state) do
    %State{event_store: event_store} = state

    if stream_uuid == @all_stream or PubSub.subscribed?(event_store, stream_uuid) do
      publish_notification(notification, state)
    else
      state
    end
  end

  # Publish the notification's range from the recent reads, reading the `$all` stream ahead when
  # they don't cover it, otherwise read the range from its own stream.
  defp publish_notification(%Notification{} = notification, %State{} = state) do
    %Notification{stream_uuid: stream_uuid} = notification
    %State{event_store: event_store} = state

    case recent_events(notification, state) do
      {:ok, events} ->
        broadcast(event_store, stream_uuid, events)

        state

      :error ->
        state = read_ahead(notification, state)

        case recent_events(notification, state) do
          {:ok, events} ->
            broadcast(event_store, stream_uuid, events)

          :error ->
            with {stream_uuid, events} <- read_events(notification, state) do
              broadcast(event_store, stream_uuid, events)
            end
        end

        state
    end
  end

  # Read the `$all` stream from where the previous read ended (or from the notification's start
  # when there is a gap or nothing has been read yet). A single stream's notification does not
  # include the position of its events in the `$all` stream, so it can only read ahead from an
  # earlier read.
  defp read_ahead(%Notification{stream_uuid: @all_stream} = notification, %State{} = state) do
    %Notification{from_stream_version: from, to_stream_version: to} = notification
    %State{last_all_stream_version: last} = state

    cond do
      is_integer(last) and to <= last -> state
      is_integer(last) and from <= last + 1 -> read_all_stream(state, last + 1, to)
      true -> read_all_stream(state, from, to)
    end
  end

  defp read_ahead(%Notification{}, %State{last_all_stream_version: last} = state)
       when is_integer(last),
       do: read_all_stream(state, last + 1, last + 1)

  defp read_ahead(%Notification{}, %State{} = state), do: state

  # Read the `$all` stream from `from` through `to`, and on to any events committed since, in a
  # single query reading at least `@read_ahead` events.
  defp read_all_stream(%State{} = state, from, to) do
    notification = %Notification{
      stream_uuid: @all_stream,
      stream_id: 0,
      from_stream_version: from,
      to_stream_version: max(to, from + @read_ahead - 1)
    }

    case read_events(notification, state) do
      {@all_stream, [_ | _] = events} ->
        %State{recent_reads: {latest, _previous}} = state
        %RecordedEvent{event_number: last} = List.last(events)

        events_by_stream =
          events
          |> Enum.group_by(& &1.stream_uuid)
          |> Map.put(@all_stream, events)

        %State{state | last_all_stream_version: last, recent_reads: {events_by_stream, latest}}

      _no_events_or_failed ->
        state
    end
  end

  # The notification's range taken from the two most recent reads, numbered as a read of its
  # stream would number them (by position for the `$all` stream, otherwise by stream version).
  defp recent_events(%Notification{} = notification, %State{} = state) do
    %Notification{stream_uuid: stream_uuid, from_stream_version: from, to_stream_version: to} =
      notification

    %State{recent_reads: {latest, previous}} = state

    version =
      if stream_uuid == @all_stream,
        do: & &1.event_number,
        else: & &1.stream_version

    events =
      (Map.get(previous, stream_uuid, []) ++ Map.get(latest, stream_uuid, []))
      |> Enum.filter(&(version.(&1) >= from and version.(&1) <= to))
      |> Enum.uniq_by(version)
      |> Enum.sort_by(version)

    cond do
      length(events) != to - from + 1 ->
        :error

      stream_uuid == @all_stream ->
        {:ok, events}

      true ->
        {:ok, Enum.map(events, &%{&1 | event_number: &1.stream_version})}
    end
  end

  defp read_events(%Notification{} = notification, %State{} = state) do
    %Notification{
      stream_uuid: stream_uuid,
      stream_id: stream_id,
      from_stream_version: from_stream_version,
      to_stream_version: to_stream_version
    } = notification

    %State{
      conn: conn,
      query_timeout: query_timeout,
      schema: schema,
      serializer: serializer,
      correlation_id_type: correlation_id_type,
      causation_id_type: causation_id_type
    } = state

    count = to_stream_version - from_stream_version + 1

    try do
      case Storage.read_stream_forward(conn, stream_id, from_stream_version, count,
             schema: schema,
             timeout: query_timeout,
             correlation_id_type: correlation_id_type,
             causation_id_type: causation_id_type
           ) do
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
