defmodule EventStore.Tasks.Output do
  @moduledoc false

  def write_info(msg, opts) do
    unless opts[:quiet] do
      info(msg, opts[:is_mix])
    end
  end

  defp info(msg, true) do
    Mix.shell().info(msg)
  end

  defp info(msg, _) do
    IO.puts(msg)
  end

  def raise_msg(msg, opts) do
    unless opts[:quiet] do
      do_raise(msg, opts[:is_mix])
    end
  end

  @spec do_raise(msg :: String.t(), boolean()) :: no_return()
  defp do_raise(msg, true) do
    Mix.raise(msg)
  end

  defp do_raise(msg, _) do
    raise msg
  end
end
