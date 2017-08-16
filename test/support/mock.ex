defmodule Rivulet.Mock do
  defmacro __using__(_) do
    quote do
      require Rivulet.Mock
      import Rivulet.Mock
    end
  end

  defmacro mock(module, function, behavior, opts \\ [], [do: do_block]) do
    quote do
      test = fn() -> unquote(do_block) end
      Rivulet.Mock.do_mock(unquote(module), unquote(function), unquote(behavior), test, unquote(opts))
    end
  end

  def do_mock(module, function, behavior, test, opts \\ []) do
    :meck.new(module, opts)
    :meck.expect(module, function, behavior)

    test.()

    :meck.unload(module)
  rescue 
    e -> 
    IO.inspect("Unloading module")
      :meck.unload(module)
      raise e
  end
end
