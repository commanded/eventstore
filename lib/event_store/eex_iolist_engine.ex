defmodule EventStore.EExIOListEngine do
  @moduledoc false
  @behaviour EEx.Engine

  def init(_opts) do
    %{iodata: [], dynamic: [], vars_count: 0}
  end

  def handle_begin(state) do
    %{state | iodata: [], dynamic: []}
  end

  def handle_end(quoted) do
    handle_body(quoted)
  end

  def handle_body(state) do
    %{iodata: iodata, dynamic: dynamic} = state
    iodata = Enum.reverse(iodata)
    {:__block__, [], Enum.reverse([iodata | dynamic])}
  end

  # Required for Elixir versions older than v1.12.0
  def handle_text(state, text) do
    handle_text(state, [], text)
  end

  def handle_text(state, _meta, text) do
    %{iodata: iodata} = state
    %{state | iodata: [text | iodata]}
  end

  def handle_expr(state, "=", ast) do
    %{iodata: iodata, dynamic: dynamic, vars_count: vars_count} = state

    var = Macro.var(:"arg#{vars_count}", __MODULE__)

    ast =
      quote do
        unquote(var) = String.Chars.to_string(unquote(ast))
      end

    %{state | dynamic: [ast | dynamic], iodata: [var | iodata], vars_count: vars_count + 1}
  end

  def handle_expr(state, "", ast) do
    %{dynamic: dynamic} = state
    %{state | dynamic: [ast | dynamic]}
  end

  def handle_expr(state, marker, ast) do
    EEx.Engine.handle_expr(state, marker, ast)
  end
end
