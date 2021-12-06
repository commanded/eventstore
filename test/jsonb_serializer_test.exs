defmodule EventStore.JsonbSerializerTest do
  use ExUnit.Case
  alias EventStore.JsonbSerializer

  defmodule TestStruct do
    defstruct [:one, :two]
  end

  test "Converts first level atoms to strings" do
    data = %TestStruct{one: 1, two: %{three: 3, four: %{five: 5}}}

    serialized = %{"one" => 1, "two" => %{four: %{five: 5}, three: 3}}

    assert JsonbSerializer.serialize(data) == serialized
  end

  test "Convert all string keys to atom in struct with list and map" do
    data = %{
      "one" => 1,
      "two" => %{
        "three" => 3,
        "four" => [
          %{"five" => 5}
        ]
      }
    }

    struct_value =
      JsonbSerializer.deserialize(data,
        type: "Elixir.EventStore.JsonbSerializerTest.TestStruct"
      )

    assert struct_value == %TestStruct{
             one: 1,
             two: %{
               three: 3,
               four: [
                 %{five: 5}
               ]
             }
           }
  end

end
