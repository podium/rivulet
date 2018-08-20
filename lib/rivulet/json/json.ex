defmodule Rivulet.JSON do
  def encode(data), do: handler().encode(data)
  def decode(data), do: handler().decode(data)
  defp handler(), do: Application.get_env(:rivulet, :json_handler) || Rivulet.JSON.Jiffy
end
