use Mix.Config

config :eventstore,
  registry: :local,
  column_data_type: "bytea"

import_config "#{Mix.env}.exs"
