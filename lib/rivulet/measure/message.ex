defmodule Rivulet.Measure.Message do
  @separator ":"
  def latency(key, microseconds) do
    msg(["key", key, "lat", microseconds])
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
