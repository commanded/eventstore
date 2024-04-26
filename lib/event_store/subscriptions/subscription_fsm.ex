defmodule EventStore.Subscriptions.SubscriptionFsm do
  @moduledoc false

  alias EventStore.{AdvisoryLocks, PubSub, RecordedEvent, Storage}
  alias EventStore.Streams.Stream
  alias EventStore.Subscriptions.{SubscriptionState, Subscriber}

  use Fsm, initial_state: :initial, initial_data: %SubscriptionState{}

  require Logger

  def new(stream_uuid, subscription_name, opts) do
    new(
      data: %SubscriptionState{
        conn: Keyword.fetch!(opts, :conn),
        event_store: Keyword.fetch!(opts, :event_store),
        stream_uuid: stream_uuid,
        subscription_name: subscription_name,
        serializer: Keyword.fetch!(opts, :serializer),
        metadata_serializer: Keyword.fetch!(opts, :metadata_serializer),
        schema: Keyword.fetch!(opts, :schema),
        start_from: opts[:start_from] || 0,
        mapper: opts[:mapper],
        selector: opts[:selector],
        partition_by: opts[:partition_by],
        buffer_size: opts[:buffer_size] || 1,
        checkpoint_after: opts[:checkpoint_after] || 0,
        checkpoint_threshold: opts[:checkpoint_threshold] || 1,
        query_timeout: opts[:query_timeout] || 15_000,
        max_size: opts[:max_size] || 1_000,
        transient: Keyword.get(opts, :transient, false)
      }
    )
  end

  # The main flow between states in this finite state machine is:
  #
  #   initial -> request_catch_up -> catching_up -> subscribed
  #

  defstate initial do
    defevent subscribe, data: %SubscriptionState{transient: true} = data do
      data = SubscriptionState.reset_event_tracking(data)

      with :ok <- subscribe_to_events(data) do
        last_seen = data.start_from

        data = %SubscriptionState{
          data
          | last_received: last_seen,
            last_sent: last_seen,
            last_ack: last_seen
        }

        notify_subscribed(data)

        next_state(:request_catch_up, data)
      else
        _ ->
          # Failed to subscribe to stream, retry after delay
          next_state(:initial, data)
      end
    end

    defevent subscribe,
      data: %SubscriptionState{} = data do
      data = SubscriptionState.reset_event_tracking(data)

      with {:ok, subscription} <- create_subscription(data),
           {:ok, lock_ref} <- try_acquire_exclusive_lock(data, subscription),
           :ok <- subscribe_to_events(data) do
        %Storage.Subscription{subscription_id: subscription_id, last_seen: last_seen} =
          subscription

        last_seen = last_seen || 0

        data = %SubscriptionState{
          data
          | subscription_id: subscription_id,
            lock_ref: lock_ref,
            last_received: last_seen,
            last_sent: last_seen,
            last_ack: last_seen
        }

        notify_subscribed(data)

        next_state(:request_catch_up, data)
      else
        _ ->
          # Failed to subscribe to stream, retry after delay
          next_state(:initial, data)
      end
    end
  end

  defstate request_catch_up do
    defevent catch_up, data: %SubscriptionState{} = data do
      catch_up_from_stream(data)
    end

    defevent ack(ack, subscriber), data: %SubscriptionState{} = data do
      with {:ok, data} <- ack_events(data, ack, subscriber) do
        catch_up_from_stream(data)
      else
        reply -> respond(reply)
      end
    end

    defevent checkpoint(), data: %SubscriptionState{} = data do
      next_state(:subscribed, persist_checkpoint(data))
    end
  end

  defstate catching_up do
    defevent ack(ack, subscriber), data: %SubscriptionState{} = data do
      with {:ok, data} <- ack_events(data, ack, subscriber) do
        catch_up_from_stream(data)
      else
        reply -> respond(reply)
      end
    end

    defevent checkpoint(), data: %SubscriptionState{} = data do
      next_state(:subscribed, persist_checkpoint(data))
    end
  end

  defstate subscribed do
    # Notify events when subscribed
    defevent notify_events(events), data: %SubscriptionState{} = data do
      %SubscriptionState{last_received: last_received} = data

      expected_event = last_received + 1

      case first_event_number(events) do
        past when past < expected_event ->
          Logger.debug(describe(data) <> " received past event(s), ignoring")

          # Ignore already seen events
          next_state(:subscribed, data)

        future when future > expected_event ->
          Logger.debug(describe(data) <> " received unexpected event(s), requesting catch up")

          # Missed event(s), request catch-up with any unseen events from storage
          next_state(:request_catch_up, data)

        ^expected_event ->
          Logger.debug(describe(data) <> " is enqueueing #{length(events)} event(s)")

          # Subscriber is up-to-date, so enqueue events to send
          data =
            data
            |> enqueue_events(events)
            |> notify_subscribers()

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
      with {:ok, data} <- ack_events(data, ack, subscriber) do
        next_state(:subscribed, data)
      else
        reply -> respond(reply)
      end
    end

    defevent catch_up, data: %SubscriptionState{} = data do
      next_state(:request_catch_up, data)
    end

    defevent checkpoint(), data: %SubscriptionState{} = data do
      next_state(:subscribed, persist_checkpoint(data))
    end
  end

  defstate max_capacity do
    defevent ack(ack, subscriber), data: %SubscriptionState{} = data do
      with {:ok, data} <- ack_events(data, ack, subscriber) do
        if empty_queue?(data) do
          # No further pending events so catch up with any unseen.
          next_state(:request_catch_up, data)
        else
          # Pending events remain, wait until subscriber ack's.
          next_state(:max_capacity, data)
        end
      else
        reply -> respond(reply)
      end
    end

    defevent checkpoint(), data: %SubscriptionState{} = data do
      next_state(:subscribed, persist_checkpoint(data))
    end
  end

  defstate disconnected do
    # Attempt to subscribe
    defevent subscribe, data: %SubscriptionState{} = data do
      with {:ok, subscription} <- create_subscription(data),
           {:ok, lock_ref} <- try_acquire_exclusive_lock(data, subscription) do
        %Storage.Subscription{
          subscription_id: subscription_id,
          last_seen: last_seen
        } = subscription

        last_ack = last_seen || 0

        data = %SubscriptionState{
          data
          | subscription_id: subscription_id,
            lock_ref: lock_ref,
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
    defevent unsubscribe(_subscriber), data: %SubscriptionState{} = data do
      next_state(:unsubscribed, data)
    end
  end

  # Catch-all event handlers

  defevent ack(_ack, _subscriber), data: %SubscriptionState{} = data, state: state do
    next_state(state, data)
  end

  defevent checkpoint(), data: %SubscriptionState{} = data, state: state do
    next_state(state, data)
  end

  defevent connect_subscriber(subscriber, opts),
    data: %SubscriptionState{} = data,
    state: state do
    data =
      data
      |> monitor_subscriber(subscriber, opts)
      |> notify_subscribers()

    unless state == :initial do
      notify_subscribed(subscriber)
    end

    next_state(state, data)
  end

  defevent subscribe, data: %SubscriptionState{} = data, state: state do
    next_state(state, data)
  end

  # Ignore notify events unless subscribed
  defevent notify_events(events), data: %SubscriptionState{} = data, state: state do
    next_state(state, track_last_received(data, events))
  end

  defevent catch_up, data: %SubscriptionState{} = data, state: state do
    next_state(state, data)
  end

  defevent disconnect(lock_ref), data: %SubscriptionState{lock_ref: lock_ref} = data do
    data =
      %SubscriptionState{data | lock_ref: nil}
      |> SubscriptionState.reset_event_tracking()
      |> purge_in_flight_events()

    next_state(:disconnected, data)
  end

  defevent unsubscribe(pid), data: %SubscriptionState{} = data, state: state do
    data =
      data
      |> remove_subscriber(pid)
      |> notify_subscribers()

    case has_subscribers?(data) do
      true ->
        next_state(state, data)

      false ->
        data = persist_checkpoint(data)

        next_state(:unsubscribed, data)
    end
  end

  defevent terminate, data: %SubscriptionState{} = data, state: state do
    next_state(state, data)
  end

  defp create_subscription(%SubscriptionState{} = data) do
    %SubscriptionState{
      conn: conn,
      query_timeout: query_timeout,
      schema: schema,
      start_from: start_from,
      stream_uuid: stream_uuid,
      subscription_name: subscription_name
    } = data

    Storage.Subscription.subscribe_to_stream(
      conn,
      stream_uuid,
      subscription_name,
      start_from,
      schema: schema,
      timeout: query_timeout
    )
  end

  defp try_acquire_exclusive_lock(
         %SubscriptionState{} = data,
         %Storage.Subscription{} = subscription
       ) do
    %Storage.Subscription{subscription_id: subscription_id} = subscription
    %SubscriptionState{event_store: event_store, query_timeout: timeout} = data

    server = Module.concat(event_store, AdvisoryLocks)

    AdvisoryLocks.try_advisory_lock(server, subscription_id, timeout)
  end

  defp subscribe_to_events(%SubscriptionState{} = data) do
    %SubscriptionState{event_store: event_store, stream_uuid: stream_uuid} = data

    PubSub.subscribe(event_store, stream_uuid)
  end

  defp monitor_subscriber(%SubscriptionState{} = data, pid, opts) when is_pid(pid) do
    %SubscriptionState{subscribers: subscribers, buffer_size: buffer_size} = data

    subscriber = %Subscriber{
      pid: pid,
      ref: Process.monitor(pid),
      buffer_size: Keyword.get(opts, :buffer_size, buffer_size)
    }

    %SubscriptionState{data | subscribers: Map.put(subscribers, pid, subscriber)}
  end

  defp remove_subscriber(%SubscriptionState{subscribers: subscribers} = data, subscriber_pid)
       when is_pid(subscriber_pid) do
    case subscriber_by_pid(subscribers, subscriber_pid) do
      {:ok, %Subscriber{} = subscriber} ->
        %Subscriber{in_flight: in_flight} = subscriber

        # Prepend in-flight events sent to the removed subscriber back into the
        # pending queue so they can be sent to another available subscriber.

        data =
          in_flight
          |> Enum.sort_by(fn %RecordedEvent{event_number: event_number} -> -event_number end)
          |> Enum.reduce(data, fn event, data -> enqueue_event(data, event, &:queue.in_r/2) end)
          |> SubscriptionState.track_in_flight(in_flight)

        %SubscriptionState{data | subscribers: Map.delete(subscribers, subscriber_pid)}

      {:error, :unknown_subscriber} ->
        data
    end
  end

  defp has_subscribers?(%SubscriptionState{subscribers: subscribers}), do: subscribers != %{}

  # Notify all connected subscribers that this subscription has successfully subscribed.
  defp notify_subscribed(%SubscriptionState{subscribers: subscribers}) do
    for {pid, _subscriber} <- subscribers do
      notify_subscribed(pid)
    end

    :ok
  end

  defp notify_subscribed(subscriber) when is_pid(subscriber) do
    send(subscriber, {:subscribed, self()})
  end

  defp track_last_received(%SubscriptionState{} = data, events) when is_list(events) do
    track_last_received(data, last_event_number(events))
  end

  defp track_last_received(%SubscriptionState{} = data, event_number)
       when is_number(event_number) do
    %SubscriptionState{last_received: last_received} = data

    %SubscriptionState{data | last_received: max(last_received, event_number)}
  end

  defp track_sent(%SubscriptionState{} = data, event_number) do
    data
    |> SubscriptionState.track_last_sent(event_number)
    |> SubscriptionState.track_in_flight(event_number)
  end

  defp first_event_number([%RecordedEvent{event_number: event_number} | _]), do: event_number

  defp last_event_number([%RecordedEvent{event_number: event_number}]), do: event_number
  defp last_event_number([_event | events]), do: last_event_number(events)

  def catch_up_from_stream(%SubscriptionState{queue_size: 0} = data) do
    %SubscriptionState{last_sent: last_sent, last_received: last_received} = data

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

        if empty_queue?(data) do
          # Request next batch of events
          next_state(:request_catch_up, data)
        else
          # Wait until subscribers have ack'd in-flight events
          next_state(:catching_up, data)
        end

      {:error, :stream_deleted} ->
        # Don't allow subscriptions to deleted streams to receive any events
        next_state(:unsubscribed, data)

      {:error, :stream_not_found} ->
        # Allow subscriptions to streams which don't yet exist, but might be created later
        next_state(:subscribed, data)
    end
  end

  def catch_up_from_stream(%SubscriptionState{} = data) do
    next_state(:catching_up, data)
  end

  defp read_stream_forward(%SubscriptionState{} = data) do
    %SubscriptionState{
      conn: conn,
      schema: schema,
      serializer: serializer,
      metadata_serializer: metadata_serializer,
      stream_uuid: stream_uuid,
      last_sent: last_sent,
      max_size: max_size,
      query_timeout: query_timeout
    } = data

    Stream.read_stream_forward(conn, stream_uuid, last_sent + 1, max_size,
      schema: schema,
      serializer: serializer,
      metadata_serializer: metadata_serializer,
      timeout: query_timeout
    )
  end

  defp enqueue_events(%SubscriptionState{} = data, []), do: data

  defp enqueue_events(%SubscriptionState{} = data, [event | events]) do
    %SubscriptionState{acknowledged_event_numbers: acknowledged_event_numbers} = data
    %RecordedEvent{event_number: event_number} = event

    data =
      if selected?(event, data) do
        # Unfiltered event, enqueue to send to a subscriber
        enqueue_event(data, event)
      else
        # Filtered event, don't send to subscriber, but track it as ack'd.
        %SubscriptionState{
          data
          | acknowledged_event_numbers: MapSet.put(acknowledged_event_numbers, event_number)
        }
        |> track_sent(event_number)
      end

    data
    |> track_last_received(event_number)
    |> enqueue_events(events)
  end

  defp enqueue_event(%SubscriptionState{} = data, event, enqueue \\ &:queue.in/2) do
    %SubscriptionState{partitions: partitions, queue_size: queue_size} = data

    partition_key = partition_key(data, event)

    partitions =
      partitions
      |> Map.put_new(partition_key, :queue.new())
      |> Map.update!(partition_key, fn pending_events -> enqueue.(event, pending_events) end)

    %SubscriptionState{data | partitions: partitions, queue_size: queue_size + 1}
  end

  def partition_key(%SubscriptionState{partition_by: nil}, %RecordedEvent{}), do: nil

  def partition_key(%SubscriptionState{partition_by: partition_by}, %RecordedEvent{} = event)
      when is_function(partition_by, 1),
      do: partition_by.(event)

  # Attempt to notify subscribers with any pending events. Partitions are
  # selected by peeking at the event number of their queue to ensure earlier
  # events are sent first.
  defp notify_subscribers(%SubscriptionState{partitions: partitions} = data) do
    partitions
    |> Enum.sort_by(fn {_partition_key, pending_events} -> peek_event_number(pending_events) end)
    |> Enum.reduce(data, fn {partition_key, _pending_events}, data ->
      notify_partition_subscriber(data, partition_key)
    end)
    |> checkpoint_acknowledged()
  end

  defp peek_event_number(pending_events) do
    case :queue.peek(pending_events) do
      {:value, %RecordedEvent{event_number: event_number}} -> event_number
      :empty -> nil
    end
  end

  defp notify_partition_subscriber(data, partition_key, events_to_send \\ []) do
    %SubscriptionState{
      partitions: partitions,
      subscribers: subscribers,
      queue_size: queue_size
    } = data

    with pending_events when not is_nil(pending_events) <- Map.get(partitions, partition_key),
         {{:value, event}, pending_events} <- :queue.out(pending_events),
         {:ok, subscriber} <- next_available_subscriber(data, partition_key) do
      %RecordedEvent{event_number: event_number} = event
      %Subscriber{pid: subscriber_pid} = subscriber

      subscriber = Subscriber.track_in_flight(subscriber, event, partition_key)

      partitions =
        case :queue.is_empty(pending_events) do
          true -> Map.delete(partitions, partition_key)
          false -> Map.put(partitions, partition_key, pending_events)
        end

      %SubscriptionState{
        data
        | partitions: partitions,
          subscribers: Map.put(subscribers, subscriber_pid, subscriber),
          queue_size: max(queue_size - 1, 0)
      }
      |> track_sent(event_number)
      |> notify_partition_subscriber(partition_key, [{subscriber_pid, event} | events_to_send])
    else
      _ ->
        # No further queued event or available subscriber, send ready events to
        # subscribers then stop notifying.
        send_queued_events(events_to_send, data)
    end
  end

  # Send events to the subscriber
  defp send_queued_events([], data), do: data

  defp send_queued_events(events_to_send, data) do
    events_to_send
    |> Enum.group_by(fn {pid, _event} -> pid end, fn {_pid, event} -> event end)
    |> Enum.each(fn {pid, events} ->
      mapped_events = events |> Enum.reverse() |> map(data)

      send(pid, {:events, mapped_events})
    end)

    data
  end

  # Select the next available subscriber based upon their partition key, buffer
  # size and number of currently in-flight events.
  #
  # Uses a round robin strategy for balancing events between subscribers.
  #
  # Events will be distributed to subscribers based upon their partition key
  # when a `partition_by/1` function is provided. This is used to guarantee
  # ordering of events for each partition.
  defp next_available_subscriber(%SubscriptionState{} = data, partition_key) do
    %SubscriptionState{subscribers: subscribers} = data

    partition_subscriber =
      Enum.find(subscribers, fn {_pid, subscriber} ->
        Subscriber.in_partition?(subscriber, partition_key)
      end)

    subscribers =
      case partition_subscriber do
        nil -> subscribers
        subscriber -> [subscriber]
      end

    subscribers
    |> Enum.sort_by(fn {_pid, %Subscriber{last_sent: last_sent}} -> last_sent end)
    |> Enum.find(fn {_pid, subscriber} -> Subscriber.available?(subscriber) end)
    |> case do
      nil -> {:error, :no_available_subscriber}
      {_pid, subscriber} -> {:ok, subscriber}
    end
  end

  defp selected?(event, %SubscriptionState{selector: selector}) when is_function(selector, 1),
    do: selector.(event)

  defp selected?(_event, %SubscriptionState{}), do: true

  defp map(events, %SubscriptionState{mapper: mapper}) when is_function(mapper, 1),
    do: Enum.map(events, mapper)

  defp map(events, _mapper), do: events

  defp ack_events(%SubscriptionState{} = data, ack, subscriber_pid) do
    %SubscriptionState{
      subscribers: subscribers,
      acknowledged_event_numbers: acknowledged_event_numbers
    } = data

    with {:ok, subscriber} <- subscriber_by_pid(subscribers, subscriber_pid),
         {:ok, subscriber, acknowledged_events} <- Subscriber.acknowledge(subscriber, ack) do
      acknowledged_event_numbers =
        acknowledged_events
        |> Enum.map(& &1.event_number)
        |> Enum.reduce(acknowledged_event_numbers, &MapSet.put(&2, &1))

      data =
        %SubscriptionState{
          data
          | subscribers: Map.put(subscribers, subscriber_pid, subscriber),
            acknowledged_event_numbers: acknowledged_event_numbers
        }
        |> notify_subscribers()

      {:ok, data}
    end
  end

  defp subscriber_by_pid(subscribers, subscriber_pid) do
    case Map.get(subscribers, subscriber_pid) do
      %Subscriber{} = subscriber -> {:ok, subscriber}
      nil -> {:error, :unknown_subscriber}
    end
  end

  defp checkpoint_acknowledged(data, persist \\ false)

  defp checkpoint_acknowledged(%SubscriptionState{in_flight_event_numbers: []} = data, persist) do
    if persist, do: maybe_persist_checkpoint(data), else: data
  end

  defp checkpoint_acknowledged(%SubscriptionState{} = data, persist) do
    %SubscriptionState{
      acknowledged_event_numbers: acknowledged_event_numbers,
      checkpoints_pending: checkpoints_pending,
      in_flight_event_numbers: [ack | in_flight_event_numbers]
    } = data

    if MapSet.member?(acknowledged_event_numbers, ack) do
      %SubscriptionState{
        data
        | acknowledged_event_numbers: MapSet.delete(acknowledged_event_numbers, ack),
          in_flight_event_numbers: in_flight_event_numbers,
          checkpoints_pending: checkpoints_pending + 1,
          last_ack: ack
      }
      |> checkpoint_acknowledged(true)
    else
      if persist, do: maybe_persist_checkpoint(data), else: data
    end
  end

  defp maybe_persist_checkpoint(%SubscriptionState{transient: true} = data) do
    %SubscriptionState{data | checkpoints_pending: 0}
  end

  defp maybe_persist_checkpoint(%SubscriptionState{transient: false} = data) do
    %SubscriptionState{
      checkpoint_after: checkpoint_after,
      checkpoints_pending: checkpoints_pending,
      checkpoint_threshold: checkpoint_threshold,
      checkpoint_timer_ref: checkpoint_timer_ref
    } = data

    cond do
      checkpoints_pending >= checkpoint_threshold ->
        persist_checkpoint(data)

      checkpoint_after > 0 ->
        if checkpoint_timer_ref, do: Process.cancel_timer(checkpoint_timer_ref)

        checkpoint_timer_ref = Process.send_after(self(), :checkpoint, checkpoint_after)

        %SubscriptionState{data | checkpoint_timer_ref: checkpoint_timer_ref}

      true ->
        data
    end
  end

  defp persist_checkpoint(%SubscriptionState{transient: true} = data) do
    %SubscriptionState{data | checkpoints_pending: 0}
  end

  defp persist_checkpoint(%SubscriptionState{transient: false} = data) do
    %SubscriptionState{
      conn: conn,
      schema: schema,
      stream_uuid: stream_uuid,
      subscription_name: subscription_name,
      last_ack: last_ack,
      query_timeout: query_timeout,
      checkpoints_pending: checkpoints_pending
    } = data

    if checkpoints_pending > 0 do
      Storage.Subscription.ack_last_seen_event(conn, stream_uuid, subscription_name, last_ack,
        schema: schema,
        timeout: query_timeout
      )
    end

    %SubscriptionState{data | checkpoints_pending: 0}
  end

  # Purge all subscriber in-flight events and subscription event queue.
  defp purge_in_flight_events(%SubscriptionState{} = data) do
    %SubscriptionState{subscribers: subscribers} = data

    subscribers =
      Enum.reduce(subscribers, %{}, fn {pid, subscriber}, acc ->
        Map.put(acc, pid, Subscriber.reset_in_flight(subscriber))
      end)

    %SubscriptionState{data | subscribers: subscribers}
  end

  defp empty_queue?(%SubscriptionState{queue_size: 0}), do: true
  defp empty_queue?(%SubscriptionState{}), do: false

  defp over_capacity?(%SubscriptionState{queue_size: queue_size, max_size: max_size}),
    do: queue_size >= max_size

  defp describe(%SubscriptionState{stream_uuid: stream_uuid, subscription_name: name}),
    do: "Subscription #{inspect(name)}@#{inspect(stream_uuid)}"
end
