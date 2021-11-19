defmodule EventStore.Sql.Statements do
  @moduledoc false

  require EEx

  alias EventStore.Sql.{Init, Reset}

  defdelegate initializers(config), to: Init, as: :statements
  defdelegate reset(config), to: Reset, as: :statements

  for {fun, args} <- [
        {:count_streams, [:schema]},
        {:create_stream, [:schema]},
        {:insert_events, [:schema, :stream_id, :number_of_events]},
        {:insert_events_any_version, [:schema, :stream_id, :number_of_events]},
        {:insert_link_events, [:schema, :number_of_events]},
        {:soft_delete_stream, [:schema]},
        {:hard_delete_stream, [:schema]},
        {:insert_subscription, [:schema]},
        {:delete_subscription, [:schema]},
        {:try_advisory_lock, [:schema]},
        {:advisory_unlock, [:schema]},
        {:subscription_ack, [:schema]},
        {:insert_snapshot, [:schema]},
        {:delete_snapshot, [:schema]},
        {:query_all_subscriptions, [:schema]},
        {:query_snapshot, [:schema]},
        {:query_stream_info, [:schema]},
        {:query_stream_events_backward, [:schema]},
        {:query_stream_events_forward, [:schema]},
        {:query_streams, [:schema, :sort_by, :sort_dir]},
        {:query_subscription, [:schema]}
      ] do
    file = Path.expand("statements/#{fun}.sql.eex", __DIR__)

    @external_resource file

    EEx.function_from_file(:def, fun, file, args, engine: EventStore.EExIOListEngine)
  end
end
