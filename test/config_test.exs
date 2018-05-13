defmodule EventStore.ConfigTest do
  use ExUnit.Case

  alias EventStore.Config

  test "parse keys" do
    original = [
      username: "postgres",
      hostname: "localhost",
      database: "eventstore_test",
      password: "postgres",
      pool: DBConnection.Poolboy
    ]

    assert Config.parse(original) == [
             password: "postgres",
             database: "eventstore_test",
             hostname: "localhost",
             username: "postgres",
             pool: DBConnection.Poolboy
           ]
  end

  test "parse url" do
    original = [url: "postgres://username:password@localhost/database"]

    config = Config.parse(original)

    assert config == [
             username: "username",
             password: "password",
             database: "database",
             hostname: "localhost",
             pool: DBConnection.Poolboy
           ]
  end

  test "parse database url from environment variable" do
    set_envs(%{"ES_URL" => "postgres://username:password@localhost/database"})

    config = Config.parse(url: {:system, "ES_URL"})

    assert config == [
             username: "username",
             password: "password",
             database: "database",
             hostname: "localhost",
             pool: DBConnection.Poolboy
           ]
  end

  test "fetch database settings from environment variables" do
    set_envs(%{
      "ES_USERNAME" => "username",
      "ES_PASSWORD" => "password",
      "ES_DATABASE" => "database",
      "ES_HOSTNAME" => "hostname",
      "ES_PORT" => "5432"
    })

    config =
      Config.parse(
        username: {:system, "ES_USERNAME"},
        password: {:system, "ES_PASSWORD"},
        database: {:system, "ES_DATABASE"},
        hostname: {:system, "ES_HOSTNAME"},
        port: {:system, "ES_PORT"}
      )

    assert config == [
             port: 5432,
             hostname: "hostname",
             database: "database",
             password: "password",
             username: "username",
             pool: DBConnection.Poolboy
           ]
  end

  test "support default value when env variable not set" do
    config = Config.parse(username: {:system, "ES_USERNAME", "default_username"})

    assert config == [
             username: "default_username",
             pool: DBConnection.Poolboy
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
