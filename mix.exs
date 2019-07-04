defmodule EventStore.Mixfile do
  use Mix.Project

  @version "0.17.0"

  def project do
    [
      app: :eventstore,
      version: @version,
      elixir: "~> 1.6",
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
      extra_applications: [:logger, :ssl]
    ]
  end

  defp elixirc_paths(env) when env in [:bench, :distributed, :jsonb, :local, :test],
    do: ["lib", "test/support", "test/subscriptions/support"]

  defp elixirc_paths(_env), do: ["lib"]

  defp deps do
    [
      {:elixir_uuid, "~> 1.2"},
      {:fsm, "~> 0.3"},
      {:gen_stage, "~> 0.14"},
      {:postgrex, "~> 0.14"},

      # Optional dependencies
      {:jason, "~> 1.1", optional: true},
      {:poolboy, "~> 1.5", optional: true},

      # Test & release tooling
      {:benchfella, "~> 0.3", only: :bench},
      {:credo, "~> 1.1", only: [:dev, :test]},
      {:dialyxir, "~> 0.5", only: [:dev, :test]},
      {:ex_doc, "~> 0.20", only: :dev},
      {:mix_test_watch, "~> 0.9", only: :dev}
    ]
  end

  defp description do
    """
    Event store using PostgreSQL for persistence.
    """
  end

  defp docs do
    [
      main: "EventStore",
      canonical: "http://hexdocs.pm/eventstore",
      source_ref: "v#{@version}",
      extra_section: "GUIDES",
      extras: [
        "CHANGELOG.md",
        "guides/Getting Started.md",
        "guides/Usage.md",
        "guides/Subscriptions.md",
        "guides/Cluster.md",
        "guides/Event Serialization.md",
        "guides/Upgrades.md",
        "guides/upgrades/0.17-1.0.md": [
          filename: "0.17-1.0",
          title: "Upgrade guide v0.17.x to 1.0"
        ]
      ],
      groups_for_extras: [
        Introduction: [
          "guides/Getting Started.md",
          "guides/Usage.md",
          "guides/Subscriptions.md"
        ],
        Serialization: [
          "guides/Event Serialization.md"
        ],
        Deployment: [
          "guides/Cluster.md"
        ],
        Upgrades: [
          "guides/Upgrades.md",
          "guides/upgrades/0.17-1.0.md"
        ]
      ],
      groups_for_modules: [
        "Data types": [
          EventStore.EventData,
          EventStore.RecordedEvent,
          EventStore.Snapshots.SnapshotData
        ],
        Serialization: [
          EventStore.JsonSerializer,
          EventStore.JsonbSerializer,
          EventStore.Serializer,
          EventStore.TermSerializer
        ],
        Registration: [
          EventStore.Registration,
          EventStore.Registration.DistributedRegistry,
          EventStore.Registration.LocalRegistry
        ],
        Tasks: [
          EventStore.Tasks.Create,
          EventStore.Tasks.Drop,
          EventStore.Tasks.Init,
          EventStore.Tasks.Migrate
        ]
      ]
    ]
  end

  defp package do
    [
      files: ["lib", "priv", "guides", "mix.exs", "README*", "LICENSE*"],
      maintainers: ["Ben Smith"],
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/commanded/eventstore"
      }
    ]
  end

  defp aliases do
    [
      benchmark: ["es.reset", "app.start", "bench"],
      "event_store.reset": ["event_store.drop", "event_store.setup"],
      "event_store.setup": ["event_store.create", "event_store.init"],
      "es.reset": ["event_store.reset"],
      "es.setup": ["event_store.setup"],
      "test.all": ["test.jsonb", "test --include slow"],
      "test.distributed": &test_distributed/1,
      "test.jsonb": &test_jsonb/1,
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
      plt_add_apps: [:jason, :ex_unit],
      plt_add_deps: :app_tree,
      plt_file: {:no_warn, "priv/plts/eventstore.plt"}
    ]
  end

  defp test_distributed(args), do: test_env(:distributed, args)
  defp test_jsonb(args), do: test_env(:jsonb, args)
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
