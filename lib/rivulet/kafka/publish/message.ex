defmodule Rivulet.Kafka.Publish.Message do
  @enforce_keys [:topic, :partition_strategy, :encoding_strategy, :value]
  defstruct [:topic, :partition, :partition_strategy, :encoding_strategy, :key, :value]

  @type t :: %__MODULE__{
    topic: Partition.topic,
    partition: Partition.partition,
    partition_strategy: Publisher.partition_strategy,
    encoding_strategy: Publisher.encoding_strategy,
    key: Publisher.key,
    value: Publisher.value
  }
end
