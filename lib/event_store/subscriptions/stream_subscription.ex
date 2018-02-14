defmodule EventStore.Subscriptions.StreamSubscription do
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
    defevent subscribe(conn, stream_uuid, subscription_name, subscriber, opts), data: %SubscriptionState{} = data do
      data = %SubscriptionState{data |
        conn: conn,
        stream_uuid: stream_uuid,
        subscription_name: subscription_name,
        subscriber: subscriber,
        mapper: opts[:mapper],
        max_size: opts[:max_size] || @max_buffer_size,
      }

      with {:ok, subscription} <- create_subscription(data, opts),
            :ok <- try_acquire_exclusive_lock(conn, subscription) do

        last_ack = subscription.last_seen || 0

        data = %SubscriptionState{data |
          subscription_id: subscription.subscription_id,
          last_seen: last_ack,
          last_ack: last_ack,
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
      next_state(:catching_up, catch_up_from_stream(data))
    end

    defevent ack(ack), data: %SubscriptionState{} = data do
      data =
        data
        |> ack_events(ack)
        |> notify_pending_events()

      next_state(:request_catch_up, data)
    end

    # ignore event notifications while catching up
    defevent notify_events(events), data: %SubscriptionState{} = data do
      next_state(:request_catch_up, track_last_received(events, data))
    end
  end

  defstate catching_up do
    defevent catch_up, data: %SubscriptionState{} = data do
      next_state(:catching_up, data)
    end

    defevent ack(ack), data: %SubscriptionState{} = data do
      data =
        data
        |> ack_events(ack)
        |> ack_catch_up(ack)
        |> notify_pending_events()

      next_state(:catching_up, data)
    end

    defevent caught_up(last_seen), data: %SubscriptionState{last_received: last_received} = data do
      data = %SubscriptionState{data |
        last_seen: last_seen,
        catch_up_pid: nil,
      }

      if is_nil(last_received) || last_seen == last_received do
        # subscriber is up-to-date with latest published events
        next_state(:subscribed, data)
      else
        # need to catch-up with events published while catching up
        next_state(:request_catch_up, data)
      end
    end

    # ignore event notifications while catching up
    defevent notify_events(events), data: %SubscriptionState{} = data do
      next_state(:catching_up, track_last_received(events, data))
    end
  end

  defstate subscribed do
    # notify events when subscribed
    defevent notify_events(events), data: %SubscriptionState{last_seen: last_seen, last_ack: last_ack, pending_events: pending_events, max_size: max_size} = data do
      expected_event = last_seen + 1
      next_ack = last_ack + 1
      first_event_number = first_event_number(events)
      last_event_number = last_event_number(events)

      case first_event_number do
        past when past < expected_event ->
          Logger.debug(fn -> describe(data) <> " received past event(s), ignoring" end)

          # ignore already seen events
          next_state(:subscribed, data)

        future when future > expected_event ->
          Logger.debug(fn -> describe(data) <> " received unexpected event(s), requesting catch up" end)

          # missed events, go back and catch-up with unseen
          next_state(:request_catch_up, data)

        ^next_ack ->
          Logger.debug(fn -> describe(data) <> " is notifying subscriber with #{length(events)} event(s)" end)

          # subscriber is up-to-date, so send events
          notify_subscriber(data, events)

          data = %SubscriptionState{data |
            last_seen: last_event_number,
            last_received: last_event_number,
          }

          next_state(:subscribed, data)

        ^expected_event ->
          Logger.debug(fn -> describe(data) <> " received event(s) but still waiting for subscriber to ack, queueing event(s)" end)

          # subscriber has not yet ack'd last seen event so store pending events
          # until subscriber ready to receive (back pressure)
          data = %SubscriptionState{data |
            last_seen: last_event_number,
            last_received: last_event_number,
            pending_events: pending_events ++ events
          }

          if length(pending_events) + length(events) >= max_size do
            # subscriber is too far behind, must wait for it to catch up
            next_state(:max_capacity, data)
          else
            # remain subscribed, waiting for subscriber to ack already sent events
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
    # ignore event notifications while over capacity
    defevent notify_events(events), data: %SubscriptionState{} = data do
      next_state(:max_capacity, track_last_received(events, data))
    end

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

    defevent catch_up, data: %SubscriptionState{} = data do
      next_state(:max_capacity, data)
    end
  end

  defstate unsubscribed do
    defevent notify_events(events), data: %SubscriptionState{} = data do
      next_state(:unsubscribed, track_last_received(events, data))
    end

    defevent ack(_ack), data: %SubscriptionState{} = data do
      next_state(:unsubscribed, data)
    end

    defevent catch_up, data: %SubscriptionState{} = data do
      next_state(:unsubscribed, data)
    end

    defevent caught_up(_last_seen), data: %SubscriptionState{} = data do
      next_state(:unsubscribed, data)
    end
  end

  # Catch-all event handlers

  defevent subscribe(_conn, _stream_uuid, _subscription_name, _subscriber, _opts),
    data: %SubscriptionState{} = data,
    state: state do
    next_state(state, data)
  end

  defevent disconnect, data: %SubscriptionState{} = data do
    next_state(:initial, data)
  end

  defevent unsubscribe, data: %SubscriptionState{} = data do
    unsubscribe_from_stream(data)
    next_state(:unsubscribed, data)
  end

  defp create_subscription(%SubscriptionState{} = data, opts) do
    %SubscriptionState{
      conn: conn,
      stream_uuid: stream_uuid,
      subscription_name: subscription_name
    } = data

    start_from = Keyword.get(opts, :start_from)

    Storage.Subscription.subscribe_to_stream(conn, stream_uuid, subscription_name, start_from)
  end

  defp try_acquire_exclusive_lock(conn, %Storage.Subscription{subscription_id: subscription_id}) do
    AdvisoryLocks.try_advisory_lock(conn, subscription_id)
  end

  defp unsubscribe_from_stream(%SubscriptionState{} = data) do
    %SubscriptionState{
      conn: conn,
      stream_uuid: stream_uuid,
      subscription_name: subscription_name
    } = data

    Storage.Subscription.unsubscribe_from_stream(conn, stream_uuid, subscription_name)
  end

  defp track_last_received(events, %SubscriptionState{} = data) do
    %SubscriptionState{data |
      last_received: last_event_number(events),
    }
  end

  defp first_event_number([%RecordedEvent{event_number: event_number} | _]), do: event_number

  defp last_event_number([%RecordedEvent{event_number: event_number}]), do: event_number
  defp last_event_number([_event | events]), do: last_event_number(events)

  # Fetch unseen events from the stream, transition to `subscribed` state when
  # stream ends
  defp catch_up_from_stream(%SubscriptionState{} = data) do
    %SubscriptionState{
      conn: conn,
      stream_uuid: stream_uuid,
      last_seen: last_seen
    } = data

    reply_to = self()

    case unseen_event_stream(conn, stream_uuid, last_seen, @max_buffer_size) do
      {:error, :stream_not_found} ->
        Subscription.caught_up(reply_to, last_seen)
        data

      unseen_event_stream ->
        # stream unseen events to subscriber in a separate process so the
        # subscription process is not blocked
        catch_up_pid = spawn_link(fn ->
          last_event =
            unseen_event_stream
            |> Stream.chunk_by(&chunk_by(&1))
            |> Stream.each(fn events ->
              notify_subscriber(data, events)
              wait_for_ack(events)
            end)
            |> Stream.map(&Enum.at(&1, -1))
            |> Enum.at(-1)

          last_seen = case last_event do
            nil -> last_seen
            %RecordedEvent{event_number: event_number} -> event_number
          end

          # notify subscription caught up to given last seen event
          Subscription.caught_up(reply_to, last_seen)
        end)

      %SubscriptionState{data | catch_up_pid: catch_up_pid}
    end
  end

  defp unseen_event_stream(conn, stream_uuid, last_seen, read_batch_size) do
    EventStore.Streams.Stream.stream_forward(conn, stream_uuid, last_seen + 1, read_batch_size)
  end

  # wait until the subscriber ack's the last sent event
  defp wait_for_ack(events) when is_list(events) do
    events
    |> last_event_number()
    |> wait_for_ack()
  end

  # wait until the subscriber ack's the `event_number`
  defp wait_for_ack(ack) do
    receive do
      {:ack, ^ack} ->
        :ok

      {:ack, received_ack} when received_ack < ack ->
        # loop until expected ack received
        wait_for_ack(ack)

      message ->
        raise RuntimeError, message: "Unexpected ack received: #{inspect message}"
    end
  end

  # send the catch-up process an acknowledgement of receipt, allowing it to continue stream events to subscriber
  defp ack_catch_up(%SubscriptionState{catch_up_pid: catch_up_pid} = data, ack) do
    send(catch_up_pid, {:ack, ack})

    data
  end

  # send pending events to subscriber if ready to receive them
  defp notify_pending_events(%SubscriptionState{pending_events: []} = data), do: data
  defp notify_pending_events(%SubscriptionState{pending_events: pending_events, last_ack: last_ack} = data) do
    [%RecordedEvent{event_number: event_number} | _events] = pending_events

    next_ack = last_ack + 1

    case event_number do
      ^next_ack ->
        # subscriber has ack'd last received event, so send pending
        pending_events
        |> Enum.chunk_by(&chunk_by/1)
        |> Enum.each(&notify_subscriber(data, &1))

        %SubscriptionState{data|
          pending_events: []
        }

      _ ->
        # subscriber has not yet ack'd last received event, don't send any more
        data
    end
  end

  defp chunk_by(%RecordedEvent{stream_uuid: stream_uuid, correlation_id: correlation_id}),
    do: {stream_uuid, correlation_id}

  defp notify_subscriber(%SubscriptionState{}, []), do: nil
  defp notify_subscriber(%SubscriptionState{subscriber: subscriber, mapper: mapper}, events)
    when is_function(mapper)
  do
    send_to_subscriber(subscriber, Enum.map(events, mapper))
  end
  defp notify_subscriber(%SubscriptionState{subscriber: subscriber}, events),
    do: send_to_subscriber(subscriber, events)

  defp send_to_subscriber(subscriber, events),
    do: send(subscriber, {:events, events})

  defp ack_events(%SubscriptionState{} = data, ack) do
    %SubscriptionState{
      conn: conn,
      stream_uuid: stream_uuid,
      subscription_name: subscription_name
    } = data

    :ok = Storage.Subscription.ack_last_seen_event(conn, stream_uuid, subscription_name, ack)

    %SubscriptionState{data| last_ack: ack}
  end

  defp describe(%SubscriptionState{stream_uuid: stream_uuid, subscription_name: name}),
    do: "Subscription #{inspect name}@#{inspect stream_uuid}"
end
