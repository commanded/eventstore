defmodule EventStore.StorageAdapters.Ecto.Repo.Migrations.CreateSnapshotsTable do
  use Ecto.Migration

  def change do
    create table(:snapshots, primary_key: false) do
      add :source_uuid, :text, primary_key: true, null: false
      add :source_version, :bigint, null: false
      add :source_type, :text, null: false
      add :data, :bytea, null: false
      add :metadata, :bytea, null: true
      add :created_at, :naive_datetime, default: fragment("(now() at time zone 'utc')"), null: false
    end
  end
end
