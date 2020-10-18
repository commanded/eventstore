defmodule EventStore.EExIOListEngine do
  @behaviour EEx.Engine

  @impl true
  def init(_opts) do
    %{iodata: [], dynamic: [], vars_count: 0}
  end

  @impl true
  def handle_begin(state) do
    %{state | iodata: [], dynamic: []}
  end

  @impl true
  def handle_end(quoted) do
    handle_body(quoted)
  end

  @impl true
  def handle_body(state) do
    %{iodata: iodata, dynamic: dynamic} = state
    iodata = Enum.reverse(iodata)
    {:__block__, [], Enum.reverse([iodata | dynamic])}
  end

  @impl true
  def handle_text(state, text) do
    %{iodata: iodata} = state
    %{state | iodata: [text | iodata]}
  end

  @impl true
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
