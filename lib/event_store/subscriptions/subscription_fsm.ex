defmodule EventStore.Subscriptions.SubscriptionFsm do
  @moduledoc false

  alias EventStore.{AdvisoryLocks, RecordedEvent, Storage}
  alias EventStore.Subscriptions.{SubscriptionState, Subscription}

  use Fsm, initial_state: :initial, initial_data: %SubscriptionState{}

  require Logger

  @max_buffer_size 1_000

  # The main flow between states in this finite state machine is:
  #
  #   initial -> subscribe_to_events -> request_catch_up -> catching_up -> subscribed
  #

  defstate initial do
    defevent subscribe(conn, stream_uuid, subscription_name, subscriber, opts) do
      data = %SubscriptionState{
        conn: conn,
        stream_uuid: stream_uuid,
        subscription_name: subscription_name,
        subscriber: subscriber,
        start_from: Keyword.get(opts, :start_from),
        mapper: opts[:mapper],
        selector: opts[:selector],
        max_size: opts[:max_size] || @max_buffer_size
      }

      with {:ok, subscription} <- create_subscription(data),
           :ok <- try_acquire_exclusive_lock(subscription) do
        %Storage.Subscription{subscription_id: subscription_id, last_seen: last_seen} =
          subscription

        last_ack = last_seen || 0

        data = %SubscriptionState{
          data
          | subscription_id: subscription_id,
            last_sent: last_ack,
            last_ack: last_ack
        }

        next_state(:subscribe_to_events, data)
      else
        _ ->
          # Failed to subscribe to stream, retry after delay
          next_state(:initial, data)
      end
    end

    # ignore ack's before subscribed
    defevent ack(_ack), data: %SubscriptionState{} = data do
      next_state(:initial, data)
    end
  end

  defstate subscribe_to_events do
    defevent subscribed, data: %SubscriptionState{} = data do
      next_state(:request_catch_up, data)
    end

    defevent ack(ack), data: %SubscriptionState{} = data do
      data =
        data
        |> ack_events(ack)
        |> notify_pending_events()

      next_state(:subscribe_to_events, data)
    end
  end

  defstate request_catch_up do
    defevent catch_up, data: %SubscriptionState{} = data do
      catch_up_from_stream(data)
    end

    defevent ack(ack), data: %SubscriptionState{} = data do
      data
      |> ack_events(ack)
      |> notify_pending_events()
      |> catch_up_from_stream()
    end
  end

  defstate catching_up do
    defevent ack(ack), data: %SubscriptionState{} = data do
      data
      |> ack_events(ack)
      |> notify_pending_events()
      |> catch_up_from_stream()
    end
  end

  defstate subscribed do
    # Notify events when subscribed
    defevent notify_events(events),
      data:
        %SubscriptionState{
          last_sent: last_sent,
          last_ack: last_ack,
          pending_events: pending_events,
          max_size: max_size
        } = data do
      expected_event = last_sent + 1
      next_ack = last_ack + 1
      first_event_number = first_event_number(events)
      last_event_number = last_event_number(events)

      case first_event_number do
        past when past < expected_event ->
          Logger.debug(fn -> describe(data) <> " received past event(s), ignoring" end)

          # Ignore already seen events
          next_state(:subscribed, data)

        future when future > expected_event ->
          Logger.debug(fn ->
            describe(data) <> " received unexpected event(s), requesting catch up"
          end)

          # Missed events, go back and catch-up with unseen
          next_state(:request_catch_up, data)

        ^next_ack ->
          Logger.debug(fn ->
            describe(data) <> " is notifying subscriber with #{length(events)} event(s)"
          end)

          # Subscriber is up-to-date, so send events
          data = notify_subscriber(events, data)

          data = %SubscriptionState{
            data
            | last_sent: last_event_number,
              last_received: last_event_number
          }

          next_state(:subscribed, data)

        ^expected_event ->
          Logger.debug(fn ->
            describe(data) <>
              " received event(s) but still waiting for subscriber to ack, queueing event(s)"
          end)

          # Subscriber has not yet ack'd last seen event so store pending events
          # until subscriber ready to receive (back pressure)
          data = %SubscriptionState{
            data
            | last_sent: last_event_number,
              last_received: last_event_number,
              pending_events: pending_events ++ [events]
          }

          if length(pending_events) + length(events) >= max_size do
            # Subscriber is too far behind, must wait for it to catch up
            next_state(:max_capacity, data)
          else
            # Remain subscribed, waiting for subscriber to ack already sent events
            next_state(:subscribed, data)
          end
      end
    end

    defevent ack(ack), data: %SubscriptionState{} = data do
      data =
        data
        |> ack_events(ack)
        |> notify_pending_events()

      next_state(:subscribed, data)
    end

    defevent catch_up, data: %SubscriptionState{} = data do
      next_state(:request_catch_up, data)
    end
  end

  defstate max_capacity do
    defevent ack(ack), data: %SubscriptionState{} = data do
      data =
        data
        |> ack_events(ack)
        |> notify_pending_events()

      case data.pending_events do
        [] ->
          # no further pending events so catch up with any unseen
          next_state(:request_catch_up, data)

        _ ->
          # pending events remain, wait until subscriber ack's
          next_state(:max_capacity, data)
      end
    end
  end

  defstate disconnected do
    # reconnect to subscription after lock reacquired
    defevent reconnect, data: %SubscriptionState{} = data do
      with {:ok, subscription} <- create_subscription(data) do
        %Storage.Subscription{
          subscription_id: subscription_id,
          last_seen: last_seen
        } = subscription

        last_ack = last_seen || 0

        data = %SubscriptionState{
          data
          | subscription_id: subscription_id,
            last_sent: last_ack,
            last_ack: last_ack
        }

        next_state(:request_catch_up, data)
      else
        _ ->
          next_state(:disconnected, data)
      end
    end
  end

  defstate unsubscribed do
    defevent ack(_ack), data: %SubscriptionState{} = data do
      next_state(:unsubscribed, data)
    end

    defevent unsubscribe, data: %SubscriptionState{} = data do
      next_state(:unsubscribed, data)
    end
  end

  # Catch-all event handlers

  defevent subscribe(_conn, _stream_uuid, _subscription_name, _subscriber, _opts),
    data: %SubscriptionState{} = data,
    state: state do
    next_state(state, data)
  end

  defevent subscribed, data: %SubscriptionState{} = data, state: state do
    next_state(state, data)
  end

  # Ignore notify events unless subscribed
  defevent notify_events(events), data: %SubscriptionState{} = data, state: state do
    next_state(state, track_last_received(events, data))
  end

  defevent catch_up, data: %SubscriptionState{} = data, state: state do
    next_state(state, data)
  end

  defevent disconnect, data: %SubscriptionState{} = data do
    next_state(:disconnected, data)
  end

  defevent unsubscribe, data: %SubscriptionState{} = data do
    next_state(:unsubscribed, unsubscribe_from_stream(data))
  end

  defp create_subscription(%SubscriptionState{} = data) do
    %SubscriptionState{
      conn: conn,
      start_from: start_from,
      stream_uuid: stream_uuid,
      subscription_name: subscription_name
    } = data

    Storage.Subscription.subscribe_to_stream(
      conn,
      stream_uuid,
      subscription_name,
      start_from,
      pool: DBConnection.Poolboy
    )
  end

  defp try_acquire_exclusive_lock(%Storage.Subscription{subscription_id: subscription_id}) do
    subscription = self()

    AdvisoryLocks.try_advisory_lock(
      subscription_id,
      lock_released: fn ->
        # disconnect subscription when lock is released (e.g. database connection down)
        Subscription.disconnect(subscription)
      end,
      lock_reacquired: fn ->
        # reconnect subscription when lock reacquired
        Subscription.reconnect(subscription)
      end
    )
  end

  defp unsubscribe_from_stream(%SubscriptionState{} = data) do
    %SubscriptionState{
      conn: conn,
      stream_uuid: stream_uuid,
      subscription_name: subscription_name
    } = data

    Storage.Subscription.unsubscribe_from_stream(
      conn,
      stream_uuid,
      subscription_name,
      pool: DBConnection.Poolboy
    )

    data
  end

  defp track_last_received(events, %SubscriptionState{} = data) do
    %SubscriptionState{data | last_received: last_event_number(events)}
  end

  defp first_event_number([%RecordedEvent{event_number: event_number} | _]), do: event_number

  defp last_event_number([%RecordedEvent{event_number: event_number}]), do: event_number
  defp last_event_number([_event | events]), do: last_event_number(events)

  defp map_to_event_number(events) do
    Enum.map(events, fn %RecordedEvent{event_number: event_number} -> event_number end)
  end

  def catch_up_from_stream(%SubscriptionState{pending_events: []} = data) do
    %SubscriptionState{
      last_sent: last_sent,
      last_received: last_received
    } = data

    case read_stream_forward(data) do
      {:ok, []} ->
        if is_nil(last_received) || last_sent == last_received do
          # Subscriber is up-to-date with latest published events
          next_state(:subscribed, data)
        else
          # Need to catch-up with events published while catching up
          next_state(:request_catch_up, data)
        end

      {:ok, events} ->
        [initial | pending] = Enum.chunk_by(events, &chunk_by/1)

        data = %SubscriptionState{data | pending_events: pending}

        data = notify_subscriber(initial, data)

        data = %SubscriptionState{data | last_sent: last_event_number(events)}

        next_state(:catching_up, data)

      {:error, :stream_not_found} ->
        next_state(:subscribed, data)
    end
  end

  def catch_up_from_stream(%SubscriptionState{} = data) do
    next_state(:catching_up, data)
  end

  defp read_stream_forward(%SubscriptionState{} = data) do
    %SubscriptionState{
      conn: conn,
      stream_uuid: stream_uuid,
      last_sent: last_sent
    } = data

    EventStore.Streams.Stream.read_stream_forward(
      conn,
      stream_uuid,
      last_sent + 1,
      @max_buffer_size,
      pool: DBConnection.Poolboy
    )
  end

  # Send next chunk of pending events to subscriber if ready to receive them
  defp notify_pending_events(%SubscriptionState{pending_events: []} = data), do: data

  defp notify_pending_events(%SubscriptionState{} = data) do
    %SubscriptionState{pending_events: pending_events, last_ack: last_ack} = data

    [[%RecordedEvent{event_number: event_number} | _events] | _chunks] = pending_events

    next_ack = last_ack + 1

    case event_number do
      ^next_ack ->
        # Subscriber has ack'd last received event, so send pending.
        [initial | pending] = pending_events

        data = %SubscriptionState{data | pending_events: pending}

        notify_subscriber(initial, data)

      _ ->
        # Subscriber has not yet ack'd last received event, don't send any more.
        data
    end
  end

  defp chunk_by(%RecordedEvent{stream_uuid: stream_uuid, correlation_id: correlation_id}),
    do: {stream_uuid, correlation_id}

  defp notify_subscriber([], %SubscriptionState{} = data), do: data

  defp notify_subscriber(events, %SubscriptionState{} = data) do
    %SubscriptionState{subscriber: subscriber, filtered_event_numbers: filtered_event_numbers} =
      data

    case filter(events, data) do
      [] ->
        # All events filtered, so just ack last event and continue notifying pending events.
        data
        |> ack_events(last_event_number(events))
        |> notify_pending_events()

      events_to_send ->
        # Send filtered & mapped events to subscriber
        send_to_subscriber(subscriber, map(events_to_send, data))

        filtered_event_numbers =
          filtered_event_numbers ++
            (map_to_event_number(events) -- map_to_event_number(events_to_send))

        %SubscriptionState{
          data
          | filtered_event_numbers: filtered_event_numbers
        }
    end
  end

  defp filter(events, %SubscriptionState{selector: selector}) when is_function(selector, 1),
    do: Enum.filter(events, selector)

  defp filter(events, _selector), do: events

  defp map(events, %SubscriptionState{mapper: mapper}) when is_function(mapper, 1),
    do: Enum.map(events, mapper)

  defp map(events, _mapper), do: events

  defp send_to_subscriber(subscriber, events), do: send(subscriber, {:events, events})

  defp ack_events(%SubscriptionState{} = data, ack) do
    %SubscriptionState{
      conn: conn,
      stream_uuid: stream_uuid,
      subscription_name: subscription_name,
      filtered_event_numbers: filtered_event_numbers
    } = data

    case Enum.member?(filtered_event_numbers, ack + 1) do
      true ->
        # Next event was filtered, attempt to acknowledge the one after next
        ack_events(data, ack + 1)

      false ->
        :ok =
          Storage.Subscription.ack_last_seen_event(
            conn,
            stream_uuid,
            subscription_name,
            ack,
            pool: DBConnection.Poolboy
          )

        %SubscriptionState{
          data
          | last_ack: ack,
            filtered_event_numbers:
              Enum.filter(filtered_event_numbers, fn event_number -> event_number <= ack end)
        }
    end
  end

  defp describe(%SubscriptionState{stream_uuid: stream_uuid, subscription_name: name}),
    do: "Subscription #{inspect(name)}@#{inspect(stream_uuid)}"
end
