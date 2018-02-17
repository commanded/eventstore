defmodule GenProxy do
  defmacro __using__(_) do
    quote location: :keep do
      use GenServer

      def handle_call(msg, from, state) do
        case proxy_call(msg, from, state) do
          {:forward, server, new_state} ->
            :erlang.send(server, {:"$gen_call", from, msg}, [:noconnect])

            {:noreply, new_state}          

          other ->
            other
        end
      end

      def handle_cast(msg, state) do
        case proxy_cast(msg, state) do
          {:forward, server, new_state} ->
            :erlang.send(server, {:"$gen_cast", msg}, [:noconnect])

            {:noreply, new_state}

          other ->
            other
        end
      end

      def handle_info(msg, state) do
        case proxy_info(msg, state) do
          {:forward, server, new_state} ->
            :erlang.send(server, msg, [:noconnect])

            {:noreply, new_state}

          other ->
            other
        end
      end

      def proxy_call(msg, _from, state) do
        {:stop, {:bad_call, msg}, state}
      end

      def proxy_cast(msg, state) do
        {:stop, {:bad_cast, msg}, state}
      end

      def proxy_info(msg, state) do
        {:stop, {:bad_info, msg}, state}
      end

      defoverridable proxy_call: 3, proxy_cast: 2, proxy_info: 2
    end
  end
end
