defmodule Rivulet.JSON.Handler do
  @callback encode(term) :: {:ok, String.t} | {:error, term}
  @callback decode(String.t) :: {:ok, term} | {:error, term}
end
