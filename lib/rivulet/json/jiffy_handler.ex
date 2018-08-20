defmodule Rivulet.JSON.Jiffy do
  @behaviour Rivulet.JSON.Handler

  # jiffy throws an error in the stuff is invalid.
  # We handle that so other functions can branch with a case statement
  # or raise their own exception if desired.
  def encode(data) do
    try do
      {:ok, :jiffy.encode(data, [:use_nil])}
    catch
      {:error, _reason} = err -> err
    end
  end

  def decode(binary) when is_binary(binary) do
    try do
      {:ok, :jiffy.decode(binary, [:use_nil, :return_maps])}
    catch
      {:error, _reason} = err -> err
    end
  end
end
