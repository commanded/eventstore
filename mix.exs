defmodule EventStore.Mixfile do
  use Mix.Project

  @version "0.15.1"

  def project do
    [
      app: :eventstore,
      version: @version,
      elixir: "~> 1.5",
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      description: description(),
      package: package(),
      docs: docs(),
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      consolidate_protocols: Mix.env() == :prod,
      aliases: aliases(),
      preferred_cli_env: preferred_cli_env(),
      dialyzer: dialyzer(),
      name: "EventStore",
      source_url: "https://github.com/commanded/eventstore"
    ]
  end

  def application do
    [
      extra_applications: [
        :logger,
        :poolboy
      ],
      mod: {EventStore.Application, []}
    ]
  end

  defp elixirc_paths(:bench), do: ["lib", "test/support", "test/subscriptions/support"]
  defp elixirc_paths(:jsonb), do: ["lib", "test/support", "test/subscriptions/support"]
  defp elixirc_paths(:distributed), do: ["lib", "test/support", "test/subscriptions/support"]
  defp elixirc_paths(:local), do: ["lib", "test/support", "test/subscriptions/support"]
  defp elixirc_paths(:test), do: ["lib", "test/support", "test/subscriptions/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:fsm, "~> 0.3"},
      {:gen_stage, "~> 0.14"},
      {:poolboy, "~> 1.5"},
      {:postgrex, "~> 0.14 or ~> 0.13"},
      {:elixir_uuid, "~> 1.2"},

      # Test & release tooling
      {:benchfella, "~> 0.3", only: :bench},
      {:credo, "~> 0.10", only: [:dev, :test]},
      {:dialyxir, "~> 0.5", only: [:dev, :test]},
      {:ex_doc, "~> 0.19", only: :dev},
      {:markdown, github: "devinus/markdown", only: :dev},
      {:mix_test_watch, "~> 0.9", only: :dev},
      {:poison, "~> 2.2 or ~> 3.0 or ~> 4.0", optional: true}
    ]
  end

  defp description do
    """
    EventStore using PostgreSQL for persistence.
    """
  end

  defp docs do
    [
      main: "EventStore",
      canonical: "http://hexdocs.pm/eventstore",
      source_ref: "v#{@version}",
      extra_section: "GUIDES",
      extras: [
        "guides/Getting Started.md",
        "guides/Usage.md",
        "guides/Subscriptions.md",
        "guides/Cluster.md",
        "guides/Event Serialization.md",
        "guides/Upgrades.md",
        "CHANGELOG.md"
      ]
    ]
  end

  defp package do
    [
      files: ["lib", "priv", "guides", "mix.exs", "README*", "LICENSE*"],
      maintainers: ["Ben Smith"],
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/commanded/eventstore",
        "Docs" => "https://hexdocs.pm/eventstore/"
      }
    ]
  end

  defp aliases do
    [
      "event_store.setup": ["event_store.create", "event_store.init"],
      "event_store.reset": ["event_store.drop", "event_store.setup"],
      "es.setup": ["event_store.setup"],
      "es.reset": ["event_store.reset"],
      benchmark: ["es.reset", "app.start", "bench"],
      "test.all": ["test.registries", "test.jsonb", "test --only slow"],
      "test.jsonb": &test_jsonb/1,
      "test.registries": &test_registries/1,
      "test.distributed": &test_distributed/1,
      "test.local": &test_local/1
    ]
  end

  defp preferred_cli_env do
    [
      "test.all": :test,
      "test.jsonb": :test,
      "test.registries": :test,
      "test.distributed": :test,
      "test.local": :test
    ]
  end

  defp dialyzer do
    [
      plt_add_apps: [:poison, :ex_unit],
      plt_add_deps: :app_tree,
      plt_file: {:no_warn, "priv/plts/eventstore.plt"}
    ]
  end

  defp test_jsonb(args), do: test_env(:jsonb, args)
  defp test_registries(args), do: Enum.map([:local, :distributed], &test_env(&1, args))
  defp test_distributed(args), do: test_env(:distributed, args)
  defp test_local(args), do: test_env(:local, args)

  defp test_env(env, args) do
    test_args = if IO.ANSI.enabled?(), do: ["--color" | args], else: ["--no-color" | args]

    IO.puts("==> Running tests for MIX_ENV=#{env} mix test #{Enum.join(args, " ")}")

    {_, res} =
      System.cmd(
        "mix",
        ["test" | test_args],
        into: IO.binstream(:stdio, :line),
        env: [{"MIX_ENV", to_string(env)}]
      )

    if res > 0 do
      System.at_exit(fn _ -> exit({:shutdown, 1}) end)
    end
  end
end
