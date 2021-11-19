defmodule EventStore.Streams.StreamInfo do
  alias EventStore.Streams.StreamInfo

  @type t :: %StreamInfo{
          stream_uuid: String.t(),
          stream_id: non_neg_integer() | nil,
          stream_version: non_neg_integer(),
          created_at: DateTime.t(),
          deleted_at: DateTime.t() | nil,
          status: :created | :deleted | nil
        }

  defstruct [:stream_uuid, :stream_id, :created_at, :deleted_at, :status, stream_version: 0]

  def new(stream_uuid) do
    %StreamInfo{stream_uuid: stream_uuid}
  end

  def validate_expected_version(%StreamInfo{} = stream, expected_version) do
    %StreamInfo{stream_id: stream_id, stream_version: stream_version, status: status} = stream

    cond do
      status == :deleted ->
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
