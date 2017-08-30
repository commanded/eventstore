defmodule EventStore.StorageAdapters.Ecto.Repo.Migrations.CreateEventsTable do
  use Ecto.Migration

  def change do
    create table(:events, primary_key: false) do
      add :event_id, :bigint, primary_key: true, null: false
      add :stream_id, references(:streams, type: :bigint, column: :stream_id), null: false
      add :stream_version, :bigint, null: false
      add :event_type, :text, null: false
      add :correlation_id, :text
      add :causation_id, :text
      add :data, :bytea, null: false
      add :metadata, :bytea, null: true
      add :created_at, :naive_datetime, default: fragment("(now() at time zone 'utc')"), null: false
    end

    create index(:events, [:stream_id], name: :ix_events_stream_id)
    create unique_index(:events, [:stream_id, "stream_version DESC"], name: :ix_events_stream_id_stream_version)
  end
end
