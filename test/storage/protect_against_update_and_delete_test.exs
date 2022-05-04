defmodule EventStore.Storage.ProtectAgainstUpdateAndDeleteTest do
  use EventStore.StorageCase

  alias EventStore.Config

  describe "event store table protection" do
    test "cannot delete from `events`, `streams`, and `stream_events` tables", %{conn: conn} do
      for table <- ["events", "streams", "stream_events"] do
        assert_cannot_delete(conn, table)
      end
    end

    test "cannot update `events` table", %{conn: conn} do
      assert {:error, %Postgrex.Error{postgres: %{message: "EventStore: Cannot update events"}}} =
               Postgrex.query(conn, "UPDATE events SET created_at = NOW();", [])
    end

    test "cannot update `stream_events` table", %{conn: conn} do
      assert {:error,
              %Postgrex.Error{postgres: %{message: "EventStore: Cannot update stream events"}}} =
               Postgrex.query(conn, "UPDATE stream_events SET stream_id = 1;", [])
    end
  end

  describe "event store enable hard deletes" do
    test "can delete from `events`, `streams`, and `stream_events` tables" do
      config =
        TestEventStore.config(enable_hard_deletes: true)
        |> Config.default_postgrex_opts()

      conn = start_supervised!({Postgrex, config})

      Postgrex.transaction(conn, fn transaction ->
        {:ok, %Postgrex.Result{}} =
          Postgrex.query(transaction, "SET SESSION eventstore.enable_hard_deletes TO 'on';", [])

        for table <- ["events", "streams", "stream_events"] do
          assert {:ok, %Postgrex.Result{}} =
                   Postgrex.query(transaction, "DELETE FROM #{table};", [])
        end
      end)
    end
  end

  defp assert_cannot_delete(conn, table) do
    expected_message = "EventStore: Cannot delete #{String.replace(table, "_", " ")}"

    assert {:error, %Postgrex.Error{postgres: %{message: ^expected_message}}} =
             Postgrex.query(conn, "DELETE FROM #{table};", [])
  end
end
