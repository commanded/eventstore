defmodule EventStore.StorageAdapters.Manager do
  def storage_adapter() do
    Application.get_env(:eventstore, :storage_adapter)
  end 
end
