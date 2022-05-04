defmodule EventStore.Notifications.Notification do
  @moduledoc false

  alias EventStore.Notifications.Notification

  defstruct [:stream_uuid, :stream_id, :from_stream_version, :to_stream_version]

  @doc """
  Build a new notification struct from the `NOTIFY` payload which contains the
  stream uuid, stream id, first and last stream versions.

  ## Example

    Notification.new("stream-12345,1,1,5")

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
