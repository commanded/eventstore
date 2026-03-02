defmodule EventStore.Notifications.Notification do
  @moduledoc """
  Represents a notification received from PostgreSQL's `LISTEN/NOTIFY` mechanism.

  Parsed from a comma-separated payload containing the stream UUID, stream ID, and the first and
  last stream versions of newly appended events.

  ## Examples:

      iex> EventStore.Notifications.Notification.new("stream-12345,1,2,5")
      %EventStore.Notifications.Notification{
        stream_uuid: "stream-12345",
        stream_id: 1,
        from_stream_version: 2,
        to_stream_version: 5
      }
  """

  alias EventStore.Notifications.Notification

  defstruct [:stream_uuid, :stream_id, :from_stream_version, :to_stream_version]

  @doc """
  Parses the PostgreSQL `NOTIFY` payload and builds a new struct.

  ## Example

      iex> EventStore.Notifications.Notification.new("stream-12345,1,1,5")
      %EventStore.Notifications.Notification{
        stream_uuid: "stream-12345",
        stream_id: 1,
        from_stream_version: 1,
        to_stream_version: 5
      }
  """
  def new(payload) do
    [last, first, stream_id, stream_uuid] =
      payload
      |> String.reverse()
      |> String.split(",", parts: 4)
      |> Enum.map(&String.reverse/1)

    {stream_id, ""} = Integer.parse(stream_id)
    {from_stream_version, ""} = Integer.parse(first)
    {to_stream_version, ""} = Integer.parse(last)

    %Notification{
      stream_uuid: stream_uuid,
      stream_id: stream_id,
      from_stream_version: from_stream_version,
      to_stream_version: to_stream_version
    }
  end
end
