defmodule Rivulet.Kafka.Publisher do
  alias Rivulet.Kafka.Partition
  alias Rivulet.Avro

  @type partition_strategy :: :random | {:key, binary} | integer
  @type encoding_strategy :: :avro | :raw | :json

  @spec publish(Partition.topic, partition_strategy, encoding_strategy, binary)
  :: :ok
  | {:error, :topic_not_found}
  def publish(topic, :random, encoding_strategy, message) do
    with {:ok, partition} <- Partition.random_partition(topic) do
      publish(topic, partition, encoding_strategy, message)
    end
  end

  def publish(topic, {:key, key}, encoding_strategy, message) when is_binary(key) do
    with {:ok, partition} <- Partition.hashed_partition(topic, key) do
      publish(topic, partition, encoding_strategy, message)
    end
  end

  def publish(topic, partition, encoding_strategy, message) when is_integer(partition) do
    msg =
      case encoding_strategy do
        :raw -> message
        :json -> Poison.encode!(message)
        :avro ->
          schema = Avro.schema_for(topic)
          Avro.encode(message, schema)
      end

    KafkaEx.produce(topic, partition, msg)
  end
end
