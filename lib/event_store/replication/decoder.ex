# Copied from https://github.com/cainophile/pgoutput_decoder
#
# Apache License 2.0
# See https://github.com/cainophile/pgoutput_decoder/blob/master/LICENSE
#
defmodule EventStore.Replication.Decoder do
  @doc """
  Decode `INSERT` messages only, ignore all others.
  """
  def decode_message(
        <<"I", _relation_id::integer-32, "N", number_of_columns::integer-16, tuple_data::binary>>
      ) do
    {<<>>, decoded_tuple_data} = decode_tuple_data(tuple_data, number_of_columns)

    {:insert, decoded_tuple_data}
  end

  def decode_message(
        <<"C", _flags::binary-1, lsn::binary-8, end_lsn::binary-8, _timestamp::integer-64>>
      ) do
    {:commit, decode_lsn(lsn), decode_lsn(end_lsn)}
  end

  defp decode_lsn(<<xlog_file::integer-32, xlog_offset::integer-32>>),
    do: {xlog_file, xlog_offset}

  def decode_message(_message), do: :ignore

  defp decode_tuple_data(binary, columns_remaining, accumulator \\ [])

  defp decode_tuple_data(remaining_binary, 0, accumulator) when is_binary(remaining_binary),
    do: {remaining_binary, accumulator |> Enum.reverse() |> List.to_tuple()}

  defp decode_tuple_data(<<"n", rest::binary>>, columns_remaining, accumulator),
    do: decode_tuple_data(rest, columns_remaining - 1, [nil | accumulator])

  defp decode_tuple_data(<<"u", rest::binary>>, columns_remaining, accumulator),
    do: decode_tuple_data(rest, columns_remaining - 1, [:unchanged_toast | accumulator])

  defp decode_tuple_data(
         <<"t", column_length::integer-32, rest::binary>>,
         columns_remaining,
         accumulator
       ) do
    decode_tuple_data(
      :erlang.binary_part(rest, {byte_size(rest), -(byte_size(rest) - column_length)}),
      columns_remaining - 1,
      [
        :erlang.binary_part(rest, {0, column_length}) | accumulator
      ]
    )
  end
end
