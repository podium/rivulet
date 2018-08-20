defmodule Rivulet.JSON do
  @def encode(term) :: {:ok, String.t} | {:error, term}
  def encode(data), do: handler().encode(data)

  @def decode(String.t) :: {:ok, term} | {:error, term}
  def decode(data), do: handler().decode(data)

  def handler(), do: Application.get_env(:rivulet, :json_handler) || Rivulet.JSON.Jiffy
end
