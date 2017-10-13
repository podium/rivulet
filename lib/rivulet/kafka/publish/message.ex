defmodule Rivulet.Kafka.Publish.Message do
  @enforce_keys [:topic, :partition_strategy, :encoding_strategy, :value]
  defstruct [:topic, :partition, :partition_strategy, :encoding_strategy, :key, :value]

  alias Rivulet.Kafka.{Partition, Publisher}

  @type t :: %__MODULE__{
    topic: Partition.topic,
    partition: Partition.partition_number,
    partition_strategy: Publisher.partition_strategy,
    encoding_strategy: Publisher.encoding_strategy,
    key: Publisher.key,
    value: Publisher.value
  }
end
