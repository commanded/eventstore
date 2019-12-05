defmodule EventStore.Storage.Schema do
  @moduledoc false

  alias EventStore.Storage.Database

  def create(config) do
    schema = Keyword.fetch!(config, :schema)

    case Database.execute("CREATE SCHEMA #{schema}", config) do
      :ok ->
        :ok

      {:error, %{postgres: %{code: :duplicate_schema}}} ->
        {:error, :already_up}

      {:error, error} ->
        {:error, Exception.message(error)}
    end
  end

  def drop(config) do
    schema = Keyword.fetch!(config, :schema)

    case Database.execute("DROP SCHEMA #{schema} CASCADE;", config) do
      :ok ->
        :ok

      {:error, %{postgres: %{code: :invalid_schema_name}}} ->
        {:error, :already_down}

      {:error, error} ->
        {:error, Exception.message(error)}
    end
  end
end
