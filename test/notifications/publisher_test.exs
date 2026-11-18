defmodule EventStore.Notifications.PublisherTest do
  use EventStore.StorageCase

  alias EventStore.{EventData, EventFactory, RecordedEvent, UUID, Wait}
  alias EventStore.Notifications.Publisher
  alias TestEventStore, as: EventStore

  @publisher Module.concat([TestEventStore, Publisher])

  setup do
    wait_until_publishing()
  end

  describe "publish events" do
    test "appended concurrently to many streams" do
      subscribed_streams = for _i <- 1..6, do: UUID.uuid4()
      unsubscribed_stream = UUID.uuid4()
      streams = [unsubscribed_stream | subscribed_streams]

      all_collector = start_collector("$all")
      stream_collectors = Map.new(subscribed_streams, &{&1, start_collector(&1)})

      # Append 25 batches of one, two, or three events to each stream concurrently.
      appends =
        Map.new(streams, fn stream_uuid ->
          batches =
            for batch <- 1..25 do
              for n <- 1..(rem(batch, 3) + 1), do: "#{batch}.#{n}"
            end

          {stream_uuid, batches ++ [["last"]]}
        end)

      appends
      |> Task.async_stream(
        fn {stream_uuid, batches} -> Enum.each(batches, &append(stream_uuid, &1)) end,
        max_concurrency: map_size(appends),
        timeout: 60_000
      )
      |> Stream.run()

      expected =
        Map.new(appends, fn {stream_uuid, batches} -> {stream_uuid, List.flatten(batches)} end)

      all_events = await_last(all_collector, streams)

      assert_ordered_by_event_number(all_events)

      for {stream_uuid, labels} <- expected do
        stream_events = Enum.filter(all_events, &(&1.stream_uuid == stream_uuid))

        assert labels(stream_events) == labels
        assert stream_versions(stream_events) == Enum.to_list(1..length(labels))
      end

      for stream_uuid <- subscribed_streams do
        labels = Map.fetch!(expected, stream_uuid)
        stream_events = await_last(stream_collectors[stream_uuid], [stream_uuid])

        assert Enum.uniq(Enum.map(stream_events, & &1.stream_uuid)) == [stream_uuid]
        assert labels(stream_events) == labels
        assert event_numbers(stream_events) == Enum.to_list(1..length(labels))
        assert stream_versions(stream_events) == Enum.to_list(1..length(labels))
      end
    end

    test "appended in a single batch larger than the read ahead" do
      stream_uuid = UUID.uuid4()

      all_collector = start_collector("$all")
      stream_collector = start_collector(stream_uuid)

      labels = Enum.map(1..2_300, &to_string/1)

      append(stream_uuid, labels)

      Wait.until(10_000, fn ->
        assert length(received_events(stream_collector)) == length(labels)
        assert length(received_events(all_collector, stream_uuid)) == length(labels)
      end)

      # Append another event to ensure no duplicate events are received
      append(stream_uuid, ["last"])

      labels = labels ++ ["last"]

      stream_events = await_last(stream_collector, [stream_uuid])

      assert labels(stream_events) == labels
      assert event_numbers(stream_events) == Enum.to_list(1..length(labels))

      all_events = await_last(all_collector, [stream_uuid])

      assert_ordered_by_event_number(all_events)
      assert labels(all_events) == labels
      assert stream_versions(all_events) == Enum.to_list(1..length(labels))
    end

    # The first notification for the subscribed stream causes the `$all` stream to be read ahead,
    # ending part way through the large batch appended to the unsubscribed stream.
    test "appended while the publisher is suspended" do
      unsubscribed_stream = UUID.uuid4()
      subscribed_stream = UUID.uuid4()

      all_collector = start_collector("$all")
      stream_collector = start_collector(subscribed_stream)

      batch = Enum.map(1..1_500, &to_string/1)

      expected = %{
        unsubscribed_stream => batch ++ ["last"],
        subscribed_stream => ["1", "2", "3", "last"]
      }

      :ok = :sys.suspend(@publisher)

      append(subscribed_stream, ["1"])
      append(unsubscribed_stream, batch)
      append(subscribed_stream, ["2", "3"])
      append(unsubscribed_stream, ["last"])
      append(subscribed_stream, ["last"])

      :ok = :sys.resume(@publisher)

      all_events = await_last(all_collector, Map.keys(expected))

      assert_ordered_by_event_number(all_events)

      for {stream_uuid, labels} <- expected do
        assert all_events |> Enum.filter(&(&1.stream_uuid == stream_uuid)) |> labels() == labels
      end

      stream_events = await_last(stream_collector, [subscribed_stream])

      assert labels(stream_events) == expected[subscribed_stream]
      assert event_numbers(stream_events) == [1, 2, 3, 4]
    end

    test "to a stream subscribed after events have been appended" do
      stream_uuid = UUID.uuid4()

      all_collector = start_collector("$all")

      append(stream_uuid, ["1", "2", "3"])

      await_last(all_collector, [stream_uuid], "3")

      stream_collector = start_collector(stream_uuid)

      append(stream_uuid, ["4", "5"])
      append(stream_uuid, ["6"])

      stream_events = await_last(stream_collector, [stream_uuid], "6")

      assert Enum.map(stream_events, &{&1.event_number, &1.stream_version, label(&1)}) == [
               {4, 4, "4"},
               {5, 5, "5"},
               {6, 6, "6"}
             ]
    end

    # A persistent subscription which has caught up ignores a broadcast starting with an event it
    # has already seen, so each append must be published in its own broadcast.
    test "to a persistent subscription which caught up before the publisher published them" do
      stream_uuid = UUID.uuid4()
      subscription_name = UUID.uuid4()

      :ok = :sys.suspend(@publisher)

      append(stream_uuid, ["1", "2", "3"])

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, self(), start_from: :current)

      assert_receive {:subscribed, ^subscription}

      Wait.until(fn ->
        assert %{subscription: %{state: :subscribed}} = :sys.get_state(subscription)
      end)

      append(stream_uuid, ["4", "5", "6"])

      :ok = :sys.resume(@publisher)

      events = receive_and_ack(subscription, 3)

      refute_receive {:events, _events}

      assert labels(events) == ["4", "5", "6"]
      assert stream_versions(events) == [4, 5, 6]

      :ok = EventStore.unsubscribe_from_all_streams(subscription_name)
    end
  end

  # Postgres does not queue notifications for a `LISTEN` which has not yet been established, so
  # append events until they are published. The first notification is published before the
  # `$all` stream has been read.
  defp wait_until_publishing do
    stream_uuid = UUID.uuid4()

    all_collector = start_collector("$all")
    stream_collector = start_collector(stream_uuid)

    Wait.until(5_000, fn ->
      append(stream_uuid, ["probe"])

      Wait.until(100, fn -> assert [_ | _] = received_events(all_collector, stream_uuid) end)
    end)

    append(stream_uuid, ["last"])

    all_events = await_last(all_collector, [stream_uuid])
    stream_events = await_last(stream_collector, [stream_uuid])

    versions = stream_versions(all_events)

    assert versions == Enum.to_list(hd(versions)..List.last(versions))
    assert event_numbers(stream_events) == versions
    assert stream_versions(stream_events) == versions

    stop_collector(all_collector)
    stop_collector(stream_collector)

    :ok
  end

  # Receive and acknowledge events from the subscription until the expected number are received.
  defp receive_and_ack(subscription, count, received \\ [])

  defp receive_and_ack(_subscription, count, received) when length(received) >= count,
    do: received

  defp receive_and_ack(subscription, count, received) do
    assert_receive {:events, events}

    :ok = EventStore.ack(subscription, events)

    receive_and_ack(subscription, count, received ++ events)
  end

  defp append(stream_uuid, labels) do
    events =
      Enum.map(labels, fn label ->
        %EventData{
          event_type: "Elixir.EventStore.EventFactory.Event",
          data: %EventFactory.Event{event: label},
          metadata: %{}
        }
      end)

    :ok = EventStore.append_to_stream(stream_uuid, :any_version, events)
  end

  # Wait until the last event of each given stream has been received.
  defp await_last(collector, stream_uuids, last_label \\ "last") do
    Wait.until(10_000, fn ->
      events = received_events(collector)

      for stream_uuid <- stream_uuids do
        assert Enum.any?(events, &(&1.stream_uuid == stream_uuid and label(&1) == last_label))
      end

      Enum.filter(events, &(&1.stream_uuid in stream_uuids))
    end)
  end

  defp assert_ordered_by_event_number(events) do
    event_numbers = event_numbers(events)

    assert event_numbers == Enum.sort(event_numbers)
    assert event_numbers == Enum.uniq(event_numbers)
  end

  defp label(%RecordedEvent{data: %EventFactory.Event{event: label}}), do: label

  defp labels(events), do: Enum.map(events, &label/1)
  defp event_numbers(events), do: Enum.map(events, & &1.event_number)
  defp stream_versions(events), do: Enum.map(events, & &1.stream_version)

  # Start a process subscribed to the topic which collects the events it receives.
  defp start_collector(topic) do
    reply_to = self()

    collector =
      spawn_link(fn ->
        :ok = EventStore.subscribe(topic)

        send(reply_to, {:subscribed, self()})

        collect([])
      end)

    assert_receive {:subscribed, ^collector}

    collector
  end

  defp collect(received) do
    receive do
      {:events, events} ->
        collect([events | received])

      {:received_events, reply_to} ->
        send(reply_to, {:received_events, self(), received |> Enum.reverse() |> Enum.concat()})

        collect(received)
    end
  end

  defp received_events(collector) do
    send(collector, {:received_events, self()})

    assert_receive {:received_events, ^collector, events}

    events
  end

  defp received_events(collector, stream_uuid) do
    collector |> received_events() |> Enum.filter(&(&1.stream_uuid == stream_uuid))
  end

  defp stop_collector(collector) do
    Process.unlink(collector)
    Process.exit(collector, :kill)
  end
end
