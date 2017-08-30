defmodule EventStore.StorageAdapters.Ecto.Repo.Migrations.CreateSubscriptionsTable do
  use Ecto.Migration

  def change do
    create table(:subscriptions, primary_key: false) do
      add :subscription_id, :bigserial, primary_key: true, null: false
      add :stream_uuid, :text, null: false
      add :subscription_name, :text, null: false
      add :last_seen_event_id, :bigint, null: true
      add :last_seen_stream_version, :bigint, null: true
      add :created_at, :naive_datetime, default: fragment("(now() at time zone 'utc')"), null: false
    end

    create unique_index(:subscriptions, [:stream_uuid, :subscription_name], name: :ix_subscriptions_stream_uuid_subscription_name)
  end
end
