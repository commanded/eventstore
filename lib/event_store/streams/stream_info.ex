defmodule EventStore.Streams.StreamInfo do
  alias EventStore.Storage
  alias EventStore.Streams.StreamInfo

  defstruct [:stream_uuid, :stream_id, :deleted_at, stream_version: 0]

  def new(stream_uuid, stream_id, stream_version, deleted_at) do
    %StreamInfo{
      stream_uuid: stream_uuid,
      stream_id: stream_id,
      stream_version: stream_version,
      deleted_at: deleted_at
    }
  end

  def read(conn, stream_uuid, expected_version, opts) do
    with {:ok, stream_id, stream_version, deleted_at} <-
           Storage.stream_info(conn, stream_uuid, opts),
         stream_info = new(stream_uuid, stream_id, stream_version, deleted_at),
         :ok <- validate_expected_version(stream_info, expected_version) do
      {:ok, stream_info}
    end
  end

  defp validate_expected_version(%StreamInfo{} = stream, expected_version) do
    %StreamInfo{stream_id: stream_id, stream_version: stream_version, deleted_at: deleted_at} =
      stream

    cond do
      not is_nil(deleted_at) ->
        {:error, :stream_deleted}

      is_nil(stream_id) and expected_version in [0, :any_version, :no_stream] ->
        :ok

      is_nil(stream_id) and expected_version == :stream_exists ->
        {:error, :stream_not_found}

      not is_nil(stream_id) and expected_version in [stream_version, :any_version, :stream_exists] ->
        :ok

      not is_nil(stream_id) and stream_version == 0 and expected_version == :no_stream ->
        :ok

      not is_nil(stream_id) and stream_version != 0 and expected_version == :no_stream ->
        {:error, :stream_exists}

      true ->
        {:error, :wrong_expected_version}
    end
  end
end
