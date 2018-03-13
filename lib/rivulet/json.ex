defmodule Rivulet.JSON do
  def encode(data) do
    {:ok, :jiffy.encode(data)}
  end

  def decode(binary) when is_binary(binary) do
    {:ok, :jiffy.decode(binary, [:return_maps, :use_nil])}
  end
end
