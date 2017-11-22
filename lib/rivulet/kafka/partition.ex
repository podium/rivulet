defmodule Rivulet.Kafka.Partition do
  require Logger

  @type count :: non_neg_integer
  @type partition_number :: non_neg_integer
  @type topic :: String.t
  @type offset :: non_neg_integer

  @enforce_keys [:topic, :partition]
  defstruct [:topic, :partition]
  @type t :: %__MODULE__{
    topic: topic,
    partition: partition_number
  }

  @spec partition_count(topic)
  :: {:ok, count}
   | {:error, :topic_not_found, topic}
  def partition_count(topic) when is_binary(topic) do
    hostname = System.get_env("HOSTNAME")
    case :brod.get_partitions_count(:"rivulet_brod_client-#{hostname}", topic) do
      {:ok, partition_count} ->
        {:ok, partition_count}
      _ ->
        Logger.error("Could not find topic: #{topic}")
        {:error, :topic_not_found, topic}
    end
  end

  @spec random_partition(topic)
  :: {:ok, partition_number}
  | {:error, :topic_not_found, topic}
  def random_partition(topic) when is_binary(topic) do
    with {:ok, count} <- partition_count(topic) do
      {:ok, :rand.uniform(count) - 1}
    end
  end

  @spec hashed_partition(topic, binary)
  :: {:ok, partition_number}
  | {:error, :topic_not_found, topic}
  def hashed_partition(topic, key) when is_binary(topic) and is_binary(key) do
    with {:ok, count} <- partition_count(topic) do
      {:ok, :erlang.phash2(key, count)}
    end
  end
end
