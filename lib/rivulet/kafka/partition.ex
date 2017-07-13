defmodule Rivulet.Kafka.Partition do
  @type count :: non_neg_integer
  @type partition :: non_neg_integer
  @type topic :: String.t

  @spec partition_count(topic)
  :: {:ok, count}
   | {:error, :topic_not_found}
  def partition_count(topic) when is_binary(topic) do
    with {:ok, topic_metadata} <- topic_metadata(topic) do
      {:ok, length(topic_metadata.partition_metadatas)}
    end
  end

  @spec random_partition(topic)
  :: {:ok, partition}
  | {:error, :topic_not_found}
  def random_partition(topic) when is_binary(topic) do
    with {:ok, count} <- partition_count(topic) do
      {:ok, :rand.uniform(count) - 1}
    end
  end

  @spec hashed_partition(topic, binary)
  :: {:ok, partition}
  | {:error, :topic_not_found}
  def hashed_partition(topic, key) when is_binary(topic) and is_binary(key) do
    with {:ok, count} <- partition_count(topic) do
      {:ok, :erlang.phash2(key, count)}
    end
  end

  @spec find_topic(topic)
  :: (KafkaEx.Protocol.Metadata.TopicMetadata.t -> boolean)
  defp find_topic(topic) do
    fn
      (%{topic: ^topic}) -> true
      (_) -> false
    end
  end

  @spec topic_metadata(topic)
  :: {:ok, KafkaEx.Protocol.Metadata.TopicMetadata}
  | {:error, :topic_not_found}
  def topic_metadata(topic) when is_binary(topic) do
    topic_finder = find_topic(topic)

    topic = Enum.find(KafkaEx.metadata.topic_metadatas, topic_finder)

    if topic do
      {:ok, topic}
    else
      {:error, :topic_not_found}
    end
  end
end