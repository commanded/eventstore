defmodule EventStore.StorageAdapters.StorageAdapter do
  
  defmacro __using__(_opts) do
    quote do
      @behaviour EventStore.Storage.Subscription
    end
  end
end
