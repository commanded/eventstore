defmodule EventStore.Subscriptions.SubscriptionFsm do
  @moduledoc false

  alias EventStore.{AdvisoryLocks, RecordedEvent, Storage}
  alias EventStore.Subscriptions.{SubscriptionState, Subscription, Subscriber}

  use Fsm, initial_state: :initial, initial_data: %SubscriptionState{}

  require Logger

  @max_buffer_size 1_000
  @empty_queue :queue.new()

  # The main flow between states in this finite state machine is:
  #
  #   initial -> subscribe_to_events -> request_catch_up -> catching_up -> subscribed
  #

  defstate initial do
    defevent subscribe(conn, stream_uuid, subscription_name, subscriber, opts) do
      data =
        %SubscriptionState{
          conn: conn,
          stream_uuid: stream_uuid,
          subscription_name: subscription_name,
          start_from: opts[:start_from],
          mapper: opts[:mapper],
          selector: opts[:selector],
          max_size: opts[:max_size] || @max_buffer_size
        }
        |> monitor_subscriber(subscriber, opts)

      with {:ok, subscription} <- create_subscription(data),
           :ok <- try_acquire_exclusive_lock(subscription) do
        %Storage.Subscription{subscription_id: subscription_id, last_seen: last_seen} =
          subscription

        last_seen = last_seen || 0

        data = %SubscriptionState{
          data
          | subscription_id: subscription_id,
            last_received: last_seen,
            last_sent: last_seen,
            last_ack: last_seen
        }

        next_state(:subscribe_to_events, data)
      else
        _ ->
          # Failed to subscribe to stream, retry after delay
          next_state(:initial, data)
      end
    end

    # ignore ack's before subscribed
    defevent ack(_ack, _subscriber), data: %SubscriptionState{} = data do
      next_state(:initial, data)
    end
  end

  defstate subscribe_to_events do
    defevent subscribed, data: %SubscriptionState{} = data do
      next_state(:request_catch_up, data)
    end

    defevent ack(ack, subscriber), data: %SubscriptionState{} = data do
      data =
        data
        |> ack_events(ack, subscriber)
        |> notify_subscribers()

      next_state(:subscribe_to_events, data)
    end
  end

  defstate request_catch_up do
    defevent catch_up, data: %SubscriptionState{} = data do
      catch_up_from_stream(data)
    end

    defevent ack(ack, subscriber), data: %SubscriptionState{} = data do
      data
      |> ack_events(ack, subscriber)
      |> notify_subscribers()
      |> catch_up_from_stream()
    end
  end

  defstate catching_up do
    defevent ack(ack, subscriber), data: %SubscriptionState{} = data do
      data
      |> ack_events(ack, subscriber)
      |> notify_subscribers()
      |> catch_up_from_stream()
    end
  end

  defstate subscribed do
    # Notify events when subscribed
    defevent notify_events(events), data: %SubscriptionState{last_sent: last_sent} = data do
      expected_event = last_sent + 1

      case first_event_number(events) do
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

        _ ->
          Logger.debug(fn ->
            describe(data) <> " is enqueueing #{length(events)} event(s)"
          end)

          # Subscriber is up-to-date, so enqueue events to send
          data = data |> enqueue_events(events) |> notify_subscribers()

          if over_capacity?(data) do
            # Too many pending events, must wait for these to be processed.
            next_state(:max_capacity, data)
          else
            # Remain subscribed, waiting for subscriber to ack already sent events.
            next_state(:subscribed, data)
          end
      end
    end

    defevent ack(ack, subscriber), data: %SubscriptionState{} = data do
      data =
        data
        |> ack_events(ack, subscriber)
        |> notify_subscribers()

      next_state(:subscribed, data)
    end

    defevent catch_up, data: %SubscriptionState{} = data do
      next_state(:request_catch_up, data)
    end
  end

  defstate max_capacity do
    defevent ack(ack, subscriber), data: %SubscriptionState{} = data do
      data
      |> ack_events(ack, subscriber)
      |> notify_subscribers()
      |> case do
        %SubscriptionState{pending_events: @empty_queue} = data ->
          # No further pending events so catch up with any unseen.
          next_state(:request_catch_up, data)

        data ->
          # Pending events remain, wait until subscriber ack's.
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
    defevent ack(_ack, _subscriber), data: %SubscriptionState{} = data do
      next_state(:unsubscribed, data)
    end

    defevent unsubscribe(_subscriber), data: %SubscriptionState{} = data do
      next_state(:unsubscribed, data)
    end
  end

  # Catch-all event handlers

  defevent connect_subscriber(subscriber, opts),
    data: %SubscriptionState{} = data,
    state: state do
    data = data |> monitor_subscriber(subscriber, opts) |> notify_subscribers()

    next_state(state, data)
  end

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

  defevent unsubscribe(pid), data: %SubscriptionState{} = data, state: state do
    data = data |> remove_subscriber(pid) |> notify_subscribers()

    case has_subscribers?(data) do
      true ->
        next_state(state, data)

      false ->
        next_state(:unsubscribed, data)
    end
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
        # Disconnect subscription when lock is released (e.g. database connection down).
        Subscription.disconnect(subscription)
      end,
      lock_reacquired: fn ->
        # Reconnect subscription when lock reacquired.
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

  defp monitor_subscriber(%SubscriptionState{subscribers: subscribers} = data, pid, opts)
       when is_pid(pid) do
    subscriber = %Subscriber{
      pid: pid,
      ref: Process.monitor(pid),
      buffer_size: Keyword.get(opts, :buffer_size, 1)
    }

    %SubscriptionState{data | subscribers: Map.put(subscribers, pid, subscriber)}
  end

  defp remove_subscriber(%SubscriptionState{} = data, pid) when is_pid(pid) do
    %SubscriptionState{subscribers: subscribers, pending_events: pending_events} = data

    case Map.get(subscribers, pid) do
      nil ->
        data

      %Subscriber{in_flight: in_flight} ->
        # Prepend in-flight events for the down subscriber to the pending
        # event queue so they will be resent to another available subscriber.
        pending_events = Enum.reduce(in_flight, pending_events, &:queue.in_r/2)

        %SubscriptionState{
          data
          | subscribers: Map.delete(subscribers, pid),
            pending_events: pending_events
        }
    end
  end

  defp has_subscribers?(%SubscriptionState{subscribers: subscribers}), do: subscribers != %{}

  defp track_last_received(events, %SubscriptionState{} = data) do
    %SubscriptionState{data | last_received: last_event_number(events)}
  end

  defp first_event_number([%RecordedEvent{event_number: event_number} | _]), do: event_number

  defp last_event_number([%RecordedEvent{event_number: event_number}]), do: event_number
  defp last_event_number([_event | events]), do: last_event_number(events)

  def catch_up_from_stream(%SubscriptionState{pending_events: @empty_queue} = data) do
    %SubscriptionState{
      last_sent: last_sent,
      last_received: last_received
    } = data

    case read_stream_forward(data) do
      {:ok, []} ->
        if last_sent == last_received do
          # Subscriber is up-to-date with latest published events
          next_state(:subscribed, data)
        else
          # Need to catch-up with events published while catching up
          next_state(:request_catch_up, data)
        end

      {:ok, events} ->
        data = data |> enqueue_events(events) |> notify_subscribers()

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

  defp enqueue_events(%SubscriptionState{} = data, events) do
    Enum.reduce(events, data, fn event, data ->
      %RecordedEvent{event_number: event_number} = event
      %SubscriptionState{pending_events: pending_events, last_received: last_received} = data

      %SubscriptionState{
        data
        | pending_events: :queue.in(event, pending_events),
          last_received: max(last_received, event_number)
      }
    end)
  end

  defp notify_subscribers(%SubscriptionState{} = data, events_to_send \\ []) do
    %SubscriptionState{
      pending_events: pending_events,
      processed_event_ids: processed_event_ids,
      subscribers: subscribers,
      last_sent: last_sent
    } = data

    case :queue.out(pending_events) do
      {{:value, event}, pending_events} ->
        %RecordedEvent{event_number: event_number} = event

        case selected?(event, data) do
          true ->
            case next_available_subscriber(subscribers, event) do
              {:ok, subscriber} ->
                %Subscriber{pid: subscriber_pid} = subscriber

                events_to_send = [{subscriber_pid, event} | events_to_send]

                subscriber = Subscriber.track_in_flight(subscriber, event)
                subscribers = Map.put(subscribers, subscriber_pid, subscriber)

                data = %SubscriptionState{
                  data
                  | pending_events: pending_events,
                    last_sent: max(last_sent, event_number),
                    subscribers: subscribers
                }

                notify_subscribers(data, events_to_send)

              _ ->
                # No available subscriber, stop notifying.
                send_queued_events(events_to_send, data)

                data
            end

          false ->
            # Filtered event, don't send to subscriber but track it as processed
            # and remove from pending event queue.
            %SubscriptionState{
              data
              | pending_events: pending_events,
                processed_event_ids: MapSet.put(processed_event_ids, event_number)
            }
            |> checkpoint_last_seen()
            |> notify_subscribers(events_to_send)
        end

      {:empty, _pending_events} ->
        # No pending events.
        send_queued_events(events_to_send, data)

        data
    end
  end

  # Send events to the subscriber
  defp send_queued_events(events_to_send, data) do
    events_to_send
    |> Enum.group_by(fn {pid, _event} -> pid end, fn {_pid, event} -> event end)
    |> Enum.each(fn {pid, events} ->
      mapped_events = events |> Enum.reverse() |> map(data)
      
      send_to_subscriber(pid, mapped_events)
    end)
  end

  # Select the next available subscriber based upon their buffer size and
  # in-flight events. Use a round robbin strategy for balancing of events
  # between subscribers.
  #
  # TODO: Partition event to subscriber
  def next_available_subscriber(subscribers, _event) do
    subscribers
    |> Enum.sort_by(fn {_pid, %Subscriber{last_sent: last_sent}} -> last_sent end)
    |> Enum.find(fn {_pid, subscriber} -> Subscriber.available?(subscriber) end)
    |> case do
      nil -> {:error, :not_available}
      {_pid, subscriber} -> {:ok, subscriber}
    end
  end

  defp filter(events, %SubscriptionState{selector: selector}) when is_function(selector, 1),
    do: Enum.filter(events, selector)

  defp filter(events, _selector), do: events

  defp selected?(event, %SubscriptionState{selector: selector}) when is_function(selector, 1),
    do: selector.(event)

  defp selected?(_event, %SubscriptionState{}), do: true

  defp map(events, %SubscriptionState{mapper: mapper}) when is_function(mapper, 1),
    do: Enum.map(events, mapper)

  defp map(events, _mapper), do: events

  defp send_to_subscriber(subscriber, events) when is_pid(subscriber),
    do: send(subscriber, {:events, events})

  defp ack_events(%SubscriptionState{} = data, ack, subscriber_pid) do
    %SubscriptionState{subscribers: subscribers, processed_event_ids: processed_event_ids} = data

    case subscribers |> Map.get(subscriber_pid) |> Subscriber.acknowledge(ack) do
      {_subscriber, []} ->
        # Not an in-flight event, ignore ack.
        data

      {subscriber, acknowledged_events} ->
        subscribers = Map.put(subscribers, subscriber_pid, subscriber)

        processed_event_ids =
          acknowledged_events
          |> Enum.map(& &1.event_number)
          |> Enum.reduce(processed_event_ids, fn event_id, acc ->
            MapSet.put(acc, event_id)
          end)

        %SubscriptionState{
          data
          | subscribers: subscribers,
            processed_event_ids: processed_event_ids
        }
        |> checkpoint_last_seen()
    end
  end

  defp checkpoint_last_seen(%SubscriptionState{} = data, persist \\ false) do
    %SubscriptionState{
      conn: conn,
      stream_uuid: stream_uuid,
      subscription_name: subscription_name,
      processed_event_ids: processed_event_ids,
      last_ack: last_ack
    } = data

    ack = last_ack + 1

    cond do
      MapSet.member?(processed_event_ids, ack) ->
        %SubscriptionState{
          data
          | processed_event_ids: MapSet.delete(processed_event_ids, ack),
            last_ack: ack
        }
        |> checkpoint_last_seen(true)

      persist ->
        Storage.Subscription.ack_last_seen_event(
          conn,
          stream_uuid,
          subscription_name,
          last_ack,
          pool: DBConnection.Poolboy
        )

        data

      true ->
        data
    end
  end

  defp over_capacity?(%SubscriptionState{} = data) do
    %SubscriptionState{pending_events: pending_events, max_size: max_size} = data

    :queue.len(pending_events) >= max_size
  end

  defp describe(%SubscriptionState{stream_uuid: stream_uuid, subscription_name: name}),
    do: "Subscription #{inspect(name)}@#{inspect(stream_uuid)}"
end
