defmodule Rivulet.Measure.Message do
  @separator ":"

  @type msg :: binary()

  @spec latency(String.t, non_neg_integer | binary) :: binary
  def latency(key, microseconds) when is_integer(microseconds) do
    latency(key, Integer.to_string(microseconds))
  end

  def latency(key, microseconds) do
    msg(["key", key, "lat", microseconds])
  end

  def utilization(resource, utilization_class, value) when is_integer(value) do
    utilization(resource, utilization_class, Integer.to_string(value))
  end

  def utilization(resource, utilization_class, value) do
    class = Atom.to_string(utilization_class)
    msg(["key", resource, "class", class, "value", value])
  end

  @spec msg([binary]) :: binary
  defp msg(vals) do
    Enum.join(vals, @separator)
  end
end
