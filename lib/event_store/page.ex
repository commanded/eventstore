defmodule EventStore.Page do
  @moduledoc """
  A page of results from a paginated query.
  """
  alias EventStore.Page

  defstruct [:page_number, :page_size, :total_entries, :total_pages, entries: []]

  @type t :: %Page{
          entries: list(),
          page_number: pos_integer(),
          page_size: integer(),
          total_entries: integer(),
          total_pages: pos_integer()
        }

  @type t(entry) :: %Page{
          entries: list(entry),
          page_number: pos_integer(),
          page_size: integer(),
          total_entries: integer(),
          total_pages: pos_integer()
        }

  def total_pages(0, _page_size), do: 1

  def total_pages(total_entries, page_size) do
    (total_entries / page_size) |> Float.ceil() |> round()
  end
end
