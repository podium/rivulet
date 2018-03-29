defmodule Rivulet.Config do
  def max_bytes do
    Application.get_env(:rivulet, :default_max_bytes, 100_000)
  end
end
