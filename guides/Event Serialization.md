# Event serialization

The default serialization of event data and metadata uses Erlang's [external term format](http://erlang.org/doc/apps/erts/erl_ext_dist.html). This is not a recommended serialization format for production usage as backwards compatibility is only guaranteed going back at least two major releases.

You must implement the `EventStore.Serializer` behaviour to provide your preferred serialization format.

## JSON serialization using Jason

EventStore includes a JSON serializer using Jason in the `EventStore.JsonSerializer` module. To include it, add `{:jason, "~> 1.1"}` to your application's mix dependencies and configure your EventStore as below.

```elixir
config :eventstore, MyApp.EventStore, serializer: EventStore.JsonSerializer
```

## Example JSON serializer using Poison

The example serializer below serializes event data and metadata to JSON using the [Poison](https://github.com/devinus/poison) library.

```elixir
defmodule JsonSerializer do
  @moduledoc """
  A serializer that uses the JSON format.
  """

  @behaviour EventStore.Serializer

  @doc """
  Serialize given term to JSON binary data.
  """
  def serialize(term) do
    Poison.encode!(term)
  end

  @doc """
  Deserialize given JSON binary data to the expected type.
  """
  def deserialize(binary, config) do
    type = case Keyword.get(config, :type, nil) do
      nil -> []
      type -> type |> String.to_existing_atom |> struct
    end
    Poison.decode!(binary, as: type)
  end
end
```

Configure the JSON serializer for your event store by setting the `serializer` option in the mix environment configuration file (e.g. `config/dev.exs`):

```elixir
config :eventstore, MyApp.EventStore, serializer: JsonSerializer
```

You must set the `event_type` field to a string representing the type of event being persisted when using this serializer:

```elixir
%EventStore.EventData{
  event_type: "Elixir.ExampleEvent",
  data: %ExampleEvent{key: "value"},
  metadata: %{user: "someuser@example.com"},
}
```

You can use `Atom.to_string/1` to get a string representation of a given event struct compatible with the example `JsonSerializer` module:

```elixir
event = %ExampleEvent{key: "value"}
event_type = Atom.to_string(event.__struct__)  #=> "Elixir.ExampleEvent"
```
