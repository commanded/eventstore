defmodule EventStore.StorageAdapters.Ecto.Repo.Migrations.CreateEventCounterTable do
  use Ecto.Migration

  def change do
    create table(:event_counter, primary_key: false) do
      add :event_id, :bigint, primary_key: true, null: false
    end 
  end
end
