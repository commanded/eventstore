defmodule EventStore.StorageAdapters.Ecto.Repo.Migrations.CreateStreamsTable do
  use Ecto.Migration

  def change do
    create table(:streams, primary_key: false) do
      add :stream_id, :bigserial, primary_key: true, null: false
      add :stream_uuid, :text, null: false
      add :created_at, :naive_datetime, default: fragment("(now() at time zone 'utc')"), null: false
    end

    create unique_index(:streams, [:stream_uuid], name: :ix_streams_stream_uuid)
  end
end
