defmodule EventStore.Storage.Stream do
  @moduledoc false

  alias EventStore.Page
  alias EventStore.Sql.Statements
  alias EventStore.Streams.StreamInfo

  def stream_info(conn, stream_uuid, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)

    query = Statements.query_stream_info(schema)

    case Postgrex.query(conn, query, [stream_uuid], opts) do
      {:ok, %Postgrex.Result{num_rows: 0}} ->
        {:ok, StreamInfo.new(stream_uuid)}

      {:ok, %Postgrex.Result{rows: [row]}} ->
        {:ok, to_stream_info(row)}

      {:error, _error} = reply ->
        reply
    end
  end

  def paginate_streams(conn, opts) do
    {page_number, opts} = Keyword.pop(opts, :page_number, 1)
    {page_size, opts} = Keyword.pop(opts, :page_size, 50)
    {schema, opts} = Keyword.pop(opts, :schema)

    offset = page_size * (page_number - 1)

    with {:ok, total_streams} <- count_streams(conn, schema, opts),
         {:ok, streams} <- query_streams(conn, schema, page_size, offset, opts) do
      page = %Page{
        entries: streams,
        page_number: page_number,
        page_size: page_size,
        total_entries: total_streams,
        total_pages: Page.total_pages(total_streams, page_size)
      }

      {:ok, page}
    end
  end

  defp count_streams(conn, schema, opts) do
    query = Statements.count_streams(schema)

    search = search_term(opts)

    case Postgrex.query(conn, query, [search], opts) do
      {:ok, %Postgrex.Result{rows: [[count]]}} ->
        {:ok, count}

      {:error, _error} = reply ->
        reply
    end
  end

  defp query_streams(conn, schema, limit, offset, opts) do
    search = search_term(opts)

    sort_by =
      case Keyword.get(opts, :sort_by, :stream_id) do
        :stream_uuid -> "stream_uuid"
        :stream_id -> "stream_id"
        :stream_version -> "stream_version"
        :created_at -> "created_at"
        :deleted_at -> "deleted_at"
        :status -> "status"
      end

    sort_dir =
      case Keyword.get(opts, :sort_dir, :asc) do
        :asc -> "ASC"
        :desc -> "DESC"
      end

    query = Statements.query_streams(schema, sort_by, sort_dir)

    case Postgrex.query(conn, query, [search, limit, offset], opts) do
      {:ok, %Postgrex.Result{num_rows: 0}} ->
        {:ok, []}

      {:ok, %Postgrex.Result{rows: rows}} ->
        {:ok, Enum.map(rows, &to_stream_info/1)}

      {:error, _error} = reply ->
        reply
    end
  end

  defp search_term(opts) do
    case Keyword.get(opts, :search) do
      nil -> nil
      "" -> nil
      search when is_binary(search) -> "%" <> search <> "%"
    end
  end

  defp to_stream_info(row) do
    [stream_id, stream_uuid, stream_version, created_at, deleted_at] = row

    stream_version = if is_nil(stream_version), do: 0, else: stream_version
    status = if is_nil(deleted_at), do: :created, else: :deleted

    %StreamInfo{
      stream_uuid: stream_uuid,
      stream_id: stream_id,
      stream_version: stream_version,
      created_at: created_at,
      deleted_at: deleted_at,
      status: status
    }
  end
end
