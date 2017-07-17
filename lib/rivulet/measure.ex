defmodule Rivulet.Measurement do
  alias Rivulet.Kafka.Publisher
  alias Rivulet.Measure.Message

  @type key :: String.t
  @type resource :: String.t
  @type utilization_class ::
    :percent
    | :count

  @utilization_classes [
    :percent,
    :count
  ]

  @spec latency(key, non_neg_integer | function) :: :ok | term
  def latency(key, microseconds) when is_integer(microseconds) do
    topic = Application.get_env(:rivulet, :stats_log, "stats")
    message = Message.latency(key, microseconds)

    Publisher.publish(topic, :random, :avro, "key", message)

    :ok
  end

  def latency(key, function) when is_function(function, 0) do
    {microseconds, value} = :timer.tc(function)

    latency(key, microseconds)

    value
  end

  @spec latency(key, function, [term]) :: term
  def latency(key, function, args)
  when is_function(function)
  and is_list(args) do
    {microseconds, value} = :timer.tc(function)

    latency(key, microseconds)

    value
  end

  @spec latency(key, module, atom, [term]) :: term
  def latency(key, module, function, args) do
    {microseconds, value} = :timer.tc(module, function, args)

    latency(key, microseconds)

    value
  end

  @spec utilization(resource, utilization_class, non_neg_integer)
  :: :ok
  def utilization(resource, utilization_class, value)
  when utilization_class in @utilization_classes do
    message = Message.utilization(resource, utilization_class, value)
    Publisher.publish("stats", :random, :avro, "key", message)
  end
end
