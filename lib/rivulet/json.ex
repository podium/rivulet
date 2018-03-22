defmodule Rivulet.JSON do
  # jiffy throws an error in the stuff is invalid.
  # We handle that so other functions can branch with a case statement
  # or raise their own exception if desired.
  def encode(data) do
    try_action(&:jiffy.encode/1, data)
  end

  def decode(binary) when is_binary(binary) do
    try_action(&:jiffy.decode/1, binary)
  end

  defp try_action(func, data) do
    try do
      {:ok, func.(data)}
    catch
      {:error, _reason} = err -> err
    end
  end
end
