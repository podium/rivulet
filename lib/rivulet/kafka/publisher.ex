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

  @spec publish([Message.t]) :: :ok | :error
  def publish(messages) do
    tasks =
      messages
      |> group_messages
      |> Enum.map(fn({{topic, partition}, msgs}) ->
        Task.Supervisor.async(Task.Supervisor, fn ->
          :rivulet
          |> Application.get_env(:publish_client_name)
          |> :brod.produce(topic, partition, _key = "", Enum.map(msgs, &to_brod_message/1))
        end)
      end)

    lookup_map =
      tasks
      |> Enum.zip(messages)
      |> Map.new

    do_wait(tasks, lookup_map, 4)
  end

  defp do_wait(tasks, _lookup_map, 0) when is_list(tasks), do: :error
  defp do_wait([], _lookup_map, counter) when counter > 0, do: :ok
  defp do_wait(tasks, lookup_map, counter)
  when is_list(tasks) and counter > 0 do
    if :error in tasks do
      Logger.error("Bulk publish to kafka failed due to error.")
      :error
    else
      tasks
      |> Task.yield_many
      |> Enum.map(fn(res) ->
        handle_task(lookup_map, res)
      end)
      |> Enum.reject(fn
        (:ok) -> true
        (:error) -> false
        ({:still_running, %Task{}}) -> false
      end)
      |> Enum.map(fn
        (:error) -> :error
        ({:still_running, task}) -> task
      end)
      |> do_wait(lookup_map, counter - 1)
    end
  end

  @spec handle_task(%{Task.t => [Message.t]}, {Task.t, {:exit, term} | {:ok, term} | nil})
  :: {:still_running, Task.t} | :ok | :error
  defp handle_task(_lookup_map, {_task, {:ok, {:error, err}}}) do
    Logger.error("Bulk publish failed: #{inspect err}")
    :error
  end
  defp handle_task(_lookup_map, {_task, {:ok, :leader_not_available}}) do
    Logger.error("Bulk publish failed - leader not available")
    :error
  end
  defp handle_task(_lookup_map, {_task, {:ok, :ok}}), do: :ok
  defp handle_task(_lookup_map, {_task, {:ok, _}}), do: :ok
  defp handle_task(_lookup_map, {task, nil}), do: {:still_running, task}
  defp handle_task(lookup_map, {task, {:exit, err}}) do
    Logger.error("Bulk publish failed: #{inspect err}")
    :error
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

  defp to_brod_message(%Message{key: nil, value: value}) do
    {"", value}
  end
  defp to_brod_message(%Message{key: key, value: value}) when is_binary(key) do
    {key, value}
  end
end
