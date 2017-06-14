defmodule Rivulet.Kafka.Publisher do
  alias Rivulet.Kafka.Partition

  @type partition_strategy :: :random | {:key, binary} | integer

  @spec publish(Partition.topic, partition_strategy, binary)
  :: :ok
  | {:error, :topic_not_found}
  def publish(topic, :random, message) do
    with {:ok, partition} <- Partition.random_partition(topic) do
       KafkaEx.produce(topic, partition, message)
    end
  end

  def publish(topic, {:key, key}, message) when is_binary(key) do
    with {:ok, partition} <- Partition.hashed_partition(topic, key) do
       KafkaEx.produce(topic, partition, message)
    end
  end
end
