defmodule Rivulet.Kafka.Publisher do
  alias Rivulet.Kafka.Publisher.Message
  alias Rivulet.Kafka.Partition
  alias Rivulet.Avro

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

  def publish(topic, partition, :raw, nil, message) when is_integer(partition) do
    publish(topic, partition, :raw, "", message)
  end

  def publish(topic, partition, :raw, key, message) when is_integer(partition) and is_binary(key) do
    :rivulet
    |> Application.get_env(:publish_client_name)
    |> :brod.produce(topic, partition, key, message)
  end

  def publish(topic, partition, :json, key, message) when is_integer(partition) do
    with {:ok, k} <- Poison.encode(key),
         {:ok, msg} <- Poison.encode(message) do
      publish(topic, partition, :raw, k, msg)
    end
  end

  def publish(topic, partition, :avro, key, value) when is_integer(partition) do
    with {:ok, key_schema} <- Avro.schema_for_subject(topic <> "-key"),
         {:ok, value_schema} <- Avro.schema_for_subject(topic <> "-value"),
         {:ok, k} <- Avro.encode(key, key_schema),
         {:ok, v} <- Avro.encode(value, value_schema) do
      publish(topic, partition, :raw, k, v)
    else
      {:error, reason} -> {:error, reason}
    end
  end

  # @spec publish([Message.t])
  # :: produce_return
  # def publish(messages) do
  #   messages
  #   |> group_messages
  #   |> Enum.map(fn({{topic, partition}, msgs}) ->
  #     :rivulet
  #     |> Application.get_env(:publish_client_name)
  #     |> :brod.produce(topic, partition, _key = "", Enum.map(msgs, &to_brod_message/1))
  #   end)
  # end

  # msg = %Rivulet.Kafka.Publisher.Message{topic: "abc", partition_strategy: :random, encoding_strategy: :raw, key: "def", value: "123"}
  # msg1 = %Rivulet.Kafka.Publisher.Message{topic: "abc1", partition_strategy: :random, encoding_strategy: :raw, key: "def1", value: "1234"}
  # Rivulet.Kafka.Publisher.publish([msg, msg, msg, msg])
  # Rivulet.Kafka.Publisher.publish([msg, msg1])

  @spec publish([Message.t]) :: produce_return
  def publish(messages) do
    publish(messages, 0)
  end

  def publish(_, counter) when counter > 10, do: {:error, "Too many retries"}

  @spec publish([Message.t], non_neg_integer) :: produce_return
  def publish(messages, counter)  when counter <= 10 do
    counter = counter + 1

    messages
    |> group_messages
    |> Enum.map(fn({{topic, partition}, msgs}) ->
      Task.Supervisor.async(Task.Supervisor, fn ->
        :rivulet
        |> Application.get_env(:publish_client_name)
        |> :brod.produce(topic, partition, _key = "", Enum.map(msgs, &to_brod_message/1))
      end)
    end)
    |> Task.yield_many()
    |> Enum.each(fn
      ({_, {:exit, err}}) ->
        IO.puts "Republishing due to received errors:"
        IO.inspect err
        publish(messages, counter)
      ({_, {:ok, value}}) ->
        IO.inspect value
      ({_, nil}) ->
        IO.puts "Received 'nil', republishing"
        publish(messages, counter)
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
    with {:ok, key_schema} <- Avro.schema_for_subject(topic <> "-key"),
         {:ok, value_schema} <- Avro.schema_for_subject(topic <> "-value"),
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

  @doc false
  defp remove_nil(nil), do: false
  defp remove_nil(_), do: true

  defp to_brod_message(%Message{key: key, value: value}) when is_nil(key) do
    {"", value}
  end
  defp to_brod_message(%Message{key: key, value: value}) when is_binary(key) do
    {key, value}
  end
end
