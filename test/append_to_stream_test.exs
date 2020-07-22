defmodule EventStore.AppendToStreamTest do
  use EventStore.StorageCase

  alias EventStore.EventFactory
  alias TestEventStore, as: EventStore

  describe "append to new stream using `:any_version`" do
    test "should persist events" do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(3)

      assert :ok = EventStore.append_to_stream(stream_uuid, :any_version, events)

      assert {:ok, events} = EventStore.read_stream_forward(stream_uuid)
      assert length(events) == 3
    end

    test "concurrently should succeed" do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(1)

      results =
        1..4
        |> Enum.map(fn _ ->
          Task.async(fn -> EventStore.append_to_stream(stream_uuid, :any_version, events) end)
        end)
        |> Enum.map(&Task.await/1)

      # All concurrent writes should succeed
      assert Enum.all?(results, fn result -> result == :ok end)

      assert {:ok, events} = EventStore.read_stream_forward(stream_uuid)
      assert length(events) == 4
    end
  end

  describe "append to existing stream using `:any_version`" do
    setup [:append_events_to_stream]

    test "should persist events", %{stream_uuid: stream_uuid} do
      events = EventFactory.create_events(1)

      assert :ok = EventStore.append_to_stream(stream_uuid, :any_version, events)

      assert {:ok, events} = EventStore.read_stream_forward(stream_uuid)
      assert length(events) == 4
    end

    test "concurrently should succeed", %{stream_uuid: stream_uuid} do
      events = EventFactory.create_events(1)

      results =
        1..4
        |> Enum.map(fn _ ->
          Task.async(fn -> EventStore.append_to_stream(stream_uuid, :any_version, events) end)
        end)
        |> Enum.map(&Task.await/1)

      # All concurrent writes should succeed
      assert Enum.all?(results, fn result -> result == :ok end)

      assert {:ok, events} = EventStore.read_stream_forward(stream_uuid)
      assert length(events) == 7
    end
  end

  describe "append to new stream using `:no_stream`" do
    test "should persist events" do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(3)

      assert :ok = EventStore.append_to_stream(stream_uuid, :no_stream, events)

      assert {:ok, events} = EventStore.read_stream_forward(stream_uuid)
      assert length(events) == 3
    end
  end

  describe "append to existing stream using `:no_stream`" do
    setup [:append_events_to_stream]

    test "should return `{:error, :stream_exists}`", %{stream_uuid: stream_uuid} do
      events = EventFactory.create_events(1)

      assert {:error, :stream_exists} =
               EventStore.append_to_stream(stream_uuid, :no_stream, events)
    end
  end

  describe "append to new stream using `:stream_exists`" do
    test "should return `{:error, :stream_not_found}`" do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(3)

      assert {:error, :stream_not_found} =
               EventStore.append_to_stream(stream_uuid, :stream_exists, events)
    end
  end

  describe "append to existing stream using `:stream_exists`" do
    setup [:append_events_to_stream]

    test "should persist events`", %{stream_uuid: stream_uuid} do
      events = EventFactory.create_events(1)

      assert :ok = EventStore.append_to_stream(stream_uuid, :stream_exists, events)

      assert {:ok, events} = EventStore.read_stream_forward(stream_uuid)
      assert length(events) == 4
    end
  end

  describe "append to existing stream using an expected stream version" do
    setup [:append_events_to_stream]

    test "should persist events with correct stream version", %{stream_uuid: stream_uuid} do
      events = EventFactory.create_events(1)

      assert :ok = EventStore.append_to_stream(stream_uuid, 3, events)

      assert {:ok, events} = EventStore.read_stream_forward(stream_uuid)
      assert length(events) == 4
    end

    test "should return `{:error, :wrong_expected_version}` with incorrect stream version",
         %{stream_uuid: stream_uuid} do
      events = EventFactory.create_events(3)

      assert {:error, :wrong_expected_version} =
               EventStore.append_to_stream(stream_uuid, 1, events)

      assert {:error, :wrong_expected_version} =
               EventStore.append_to_stream(stream_uuid, 2, events)

      assert {:error, :wrong_expected_version} =
               EventStore.append_to_stream(stream_uuid, 4, events)
    end

    test "should fail to persist events with same expected version concurrently",
         %{stream_uuid: stream_uuid} do
      events = EventFactory.create_events(1)

      results =
        1..4
        |> Enum.map(fn _ ->
          Task.async(fn -> EventStore.append_to_stream(stream_uuid, 3, events) end)
        end)
        |> Enum.map(&Task.await/1)
        |> Enum.sort()

      # One write should succeed and three should fail due to wrong expected version
      assert results == [
               :ok,
               {:error, :wrong_expected_version},
               {:error, :wrong_expected_version},
               {:error, :wrong_expected_version}
             ]

      assert {:ok, events} = EventStore.read_stream_forward(stream_uuid)
      assert length(events) == 4
    end
  end

  @tag :slow
  @tag timeout: 180_000
  test "should append many events to a stream" do
    stream_uuid = UUID.uuid4()
    events = EventFactory.create_events(8_001)

    assert :ok = EventStore.append_to_stream(stream_uuid, 0, events, timeout: :infinity)

    assert EventStore.stream_all_forward() |> Stream.map(& &1.event_number) |> Enum.to_list() ==
             Enum.to_list(1..8_001)
  end

  defp append_events_to_stream(_context) do
    stream_uuid = UUID.uuid4()
    events = EventFactory.create_events(3)

    :ok = EventStore.append_to_stream(stream_uuid, 0, events)

    [stream_uuid: stream_uuid, events: events]
  end
end
