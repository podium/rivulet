defmodule Rivulet.Kafka.Publisher do
  alias Rivulet.Kafka.Partition
  alias Rivulet.Avro

  @type partition_strategy :: :random | {:key, binary} | integer
  @type encoding_strategy :: :avro | :raw | :json
  @type key :: bitstring | Avro.decoded_message
  @typedoc """
  If the encoding_strategy is :raw, the function takes a bitstring. If another
  encoding strategy is specified, the function accepts whatever structures the
  underlying encoding accepts.
  """
  @type value :: bitstring | term

  @type produce_return :: nil | :ok | {:ok, integer} | {:error, :closed} | {:error, :inet.posix} | {:error, any} | iodata | :leader_not_available

  @spec publish(Partition.topic, partition_strategy, encoding_strategy, key, value)
  :: produce_return
  | {:error, :schema_not_found}
  | {:error, term}
  def publish(topic, :random, encoding_strategy, key, message) do
    with {:ok, partition} <- Partition.random_partition(topic) do
      publish(topic, partition, encoding_strategy, key, message)
    end
  end

  def publish(topic, {:key, hashing_key}, encoding_strategy, key, message) when is_binary(hashing_key) do
    with {:ok, partition} <- Partition.hashed_partition(topic, hashing_key) do
      publish(topic, partition, encoding_strategy, key, message)
    end
  end

  def publish(topic, partition, :raw, key, message) when is_integer(partition) do
    KafkaEx.produce(topic, partition, message, key: key)
  end

  def publish(topic, partition, :json, key, message) when is_integer(partition) do
    with {:ok, k} <- Poison.encode(key),
         {:ok, msg} <- Poison.encode(message) do
      publish(topic, partition, :raw, k, msg)
    end
  end

  def publish(topic, partition, :avro, key, message) when is_integer(partition) do
    with %{key: key_schema, value: value_schema} <- Avro.schema_for(topic),
         {:ok, k} <- Avro.encode(key, key_schema),
         {:ok, msg} <- Avro.encode(message, value_schema) do
      publish(topic, partition, :raw, k, msg)
    else
      {:error, reason} -> {:error, reason}
    end
  end
end
