defmodule Rivulet.Kafka.Publisher do
  alias Rivulet.Kafka.Publish.Message
  alias Rivulet.Kafka.Partition
  alias Rivulet.Avro

  alias KafkaEx.Protocol.Produce.Request, as: ProduceRequest

  require Logger

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

  @spec publish([Message.t])
  :: produce_return
  def publish(messages) do
    messages
    |> group_messages
    |> Enum.map(fn({{topic, partition}, msgs}) ->
      request =
        %ProduceRequest{
          topic: topic,
          partition: partition,
          required_acks: 0,
          timeout: 100,
          compression: :none,
          messages: Enum.map(msgs, &to_kafka_ex/1)
        }

      KafkaEx.produce(request)
    end)
  end

  @doc false
  def group_messages(messages) do
    messages
    |> Enum.filter(&remove_nil/1)
    |> Enum.map(&partition/1)
    |> Enum.filter(&remove_nil/1)
    |> Enum.map(&encode/1)
    |> Enum.filter(&remove_nil/1)
    |> Enum.group_by(fn(message) ->
      {message.topic, message.partition}
    end)
  end

  @doc false
  defp partition(%Message{topic: topic, partition_strategy: :random, partition: nil} = message) do
    with {:ok, partition} <- Partition.random_partition(topic) do
      %Message{message | partition: partition}
    else
      err ->
        err
        |> inspect
        |> Logger.error

        nil
    end
  end

  defp partition(%Message{topic: topic, partition_strategy: {:key, hashing_key}, partition: nil} = message) do
    with {:ok, partition} <- Partition.hashed_partition(topic, hashing_key) do
      %Message{message | partition: partition}
    else
      err ->
        err
        |> inspect
        |> Logger.error

        nil
    end
  end

  defp partition(%Message{partition_strategy: partition, partition: nil} = message) when is_integer(partition) do
    %Message{message | partition: partition}
  end

  defp partition(%Message{partition: partition} = message) when is_integer(partition) do
    message
  end

  @doc false
  @spec encode(Message.t) :: Message.t | nil
  def encode(%Message{encoding_strategy: :raw} = message) do
    message
  end

  def encode(%Message{encoding_strategy: :json, key: key, value: value} = message) do
    with {:ok, k} <- Poison.encode(key),
         {:ok, v} <- Poison.encode(value) do
           %Message{message | encoding_strategy: :raw, key: k, value: v}
    else
      err ->
        err
        |> inspect
        |> Logger.error

        nil
    end
  end

  def encode(%Message{encoding_strategy: :avro, key: key, value: value, topic: topic} = message) do
    with %{key: key_schema, value: value_schema} <- Avro.schema_for(topic),
         {:ok, k} <- Avro.encode(key, key_schema),
         {:ok, v} <- Avro.encode(value, value_schema) do
           %Message{message | encoding_strategy: :raw, key: k, value: v}
    else
      err ->
        [inspect(err), inspect(key), inspect(value)]
        |> Enum.join(" -- ")
        |> Logger.error

        nil
    end
  end

  def encode(%Message{encoding_strategy: :raw} = message) do
    message
  end

  @doc false
  defp remove_nil(nil), do: false
  defp remove_nil(_), do: true

  defp to_kafka_ex(%Message{} = msg) do
    %KafkaEx.Protocol.Produce.Message{key: msg.key, value: msg.value}
  end
end
