defmodule EventStore.ConfigTest do
  use ExUnit.Case

  alias EventStore.Config

  test "parse keys" do
    config = [
      migration_source: "my_new_migration_source",
      username: "postgres",
      hostname: "localhost",
      database: "eventstore_test",
      password: "postgres",
      schema: "example",
      timeout: 120_000,
      pool: EventStore.Config.get_pool()
    ]

    assert Config.parse(config) ==
             [
               timeout: 120_000,
               schema: "example",
               password: "postgres",
               database: "eventstore_test",
               hostname: "localhost",
               username: "postgres",
               migration_source: "my_new_migration_source",
               pool: EventStore.Config.get_pool()
             ]
  end

  test "parse socket" do
    config = [
      username: "postgres",
      socket: "/path/to/socket",
      database: "eventstore_test",
      password: "postgres",
      timeout: 120_000,
      pool: EventStore.Config.get_pool()
    ]

    assert Config.parse(config) ==
             [
               schema: "public",
               timeout: 120_000,
               password: "postgres",
               database: "eventstore_test",
               socket: "/path/to/socket",
               username: "postgres",
               pool: EventStore.Config.get_pool()
             ]
  end

  test "parse socket_dir" do
    config = [
      username: "postgres",
      socket_dir: "/path/to/socket_dir",
      database: "eventstore_test",
      password: "postgres",
      timeout: 120_000,
      pool: EventStore.Config.get_pool()
    ]

    assert Config.parse(config) ==
             [
               schema: "public",
               timeout: 120_000,
               password: "postgres",
               database: "eventstore_test",
               socket_dir: "/path/to/socket_dir",
               username: "postgres",
               pool: EventStore.Config.get_pool()
             ]
  end

  test "parse url" do
    config = [url: "postgres://username:password@localhost/database"]

    assert Config.parse(config) ==
             [
               schema: "public",
               username: "username",
               password: "password",
               database: "database",
               hostname: "localhost",
               pool: EventStore.Config.get_pool()
             ]
  end

  test "parse url with query parameters" do
    config = [
      url: "postgres://username:password@localhost/database?ssl=true&pool_size=5&timeout=120000"
    ]

    assert Config.parse(config) ==
             [
               schema: "public",
               username: "username",
               password: "password",
               database: "database",
               hostname: "localhost",
               timeout: 120_000,
               pool_size: 5,
               ssl: true,
               pool: EventStore.Config.get_pool()
             ]
  end

  test "parse database url from environment variable" do
    set_envs(%{"ES_URL" => "postgres://username:password@localhost/database"})

    config = [url: {:system, "ES_URL"}]

    assert Config.parse(config) ==
             [
               schema: "public",
               username: "username",
               password: "password",
               database: "database",
               hostname: "localhost",
               pool: EventStore.Config.get_pool()
             ]
  end

  test "fetch database settings from environment variables" do
    set_envs(%{
      "ES_USERNAME" => "username",
      "ES_PASSWORD" => "password",
      "ES_DATABASE" => "database",
      "ES_HOSTNAME" => "hostname",
      "ES_PORT" => "5432",
      "ES_TIMEOUT" => "120000"
    })

    config = [
      username: {:system, "ES_USERNAME"},
      password: {:system, "ES_PASSWORD"},
      database: {:system, "ES_DATABASE"},
      hostname: {:system, "ES_HOSTNAME"},
      port: {:system, "ES_PORT"},
      timeout: {:system, "ES_TIMEOUT"}
    ]

    assert Config.parse(config) ==
             [
               schema: "public",
               timeout: 120_000,
               port: 5432,
               hostname: "hostname",
               database: "database",
               password: "password",
               username: "username",
               pool: EventStore.Config.get_pool()
             ]
  end

  test "support default value when env variable not set" do
    config = [username: {:system, "ES_USERNAME", "default_username"}]

    assert Config.parse(config) ==
             [
               schema: "public",
               username: "default_username",
               pool: EventStore.Config.get_pool()
             ]
  end

  defp set_envs(envs) do
    for {key, value} <- envs do
      System.put_env(key, value)
    end

    on_exit(fn ->
      for {key, _value} <- envs do
        System.delete_env(key)
      end
    end)
  end
end
