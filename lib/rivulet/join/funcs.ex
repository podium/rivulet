defmodule Rivulet.Kafka.Join.Funcs do
  require Logger
  alias Rivulet.Join.ElasticSearch
  def start_link(module, consumer_group, stream_topics) do
    config =
      %Rivulet.Consumer.Config{
        client_id: Application.get_env(:rivulet, :client_name),
        consumer_group_name: consumer_group,
        topics: stream_topics,
        group_config: [
          offset_commit_policy: :commit_to_kafka_v2,
          offset_commit_interval_secons: 1,
        ],
        consumer_config: [begin_offset: :earliest, max_bytes: Rivulet.Config.max_bytes()]
      } |> IO.inspect(label: "join funcs config")

    Logger.info("Configuration for #{module}: #{inspect config}")

    Rivulet.Consumer.start_link(module, config)
  end

  def to_publish(nil), do: nil
  def to_publish({k, v}) when is_binary(k) and is_binary(v) do
    %Rivulet.Kafka.Publisher.Message{
      key: k,
      value: v,
      encoding_strategy: :raw,
      topic: :unknown,
      partition_strategy: :unknown
    }
  end

  def to_publish(other) do
    Logger.error("handle_message returned #{inspect other}, which is an unsupported type.")
    :error
  end

  defp deserialize_key(module, message) do
    case module.deserialize_key(message.raw_key) do
      {:ok, key} -> {:ok, key}
      {:error, _err} ->
        Logger.error("#{module} could not deserialize key for message. Dropping message: #{inspect(message)}")
      other ->
        Logger.error("#{module}.deseralize_key/1 returned an unexpected_value: #{inspect other}. Dropping message: #{inspect(message)}")
    end
  end

  def deserialize_value(module, message) do
    case module.deserialize_value(message.raw_value) do
      {:ok, value} -> {:ok, value}
      {:error, _err} ->
        Logger.error("#{module} could not deserialize value for message. Dropping message: #{inspect(message)}")
      other ->
        Logger.error("#{module}.deseralize_value/1 returned an unexpected_value: #{inspect other}. Dropping message: #{inspect(message)}")
    end
  end

  def get_object_id(module, key, value) do
    case module.object_id(key, value) do
      {:ok, key} when is_binary(key) -> {:ok, key}
      {:ok, key} ->
        Logger.error("#{module}.object_id/2 returned #{inspect key}, but should have returned a binary. Dropping message: KEY: #{inspect key} VALUE: #{inspect value}")
      {:error, _} = err->
        Logger.error("#{module}.object_id/2 returned #{inspect err}. Dropping message: KEY: #{inspect key} VALUE: #{inspect value}")
      other ->
        Logger.error("#{module}.object_id/2 returned #{inspect other}, but should have returned a binary. Dropping message: KEY: #{inspect key} VALUE: #{inspect value}")
    end
  end

  def handle_messages(%Rivulet.Kafka.Partition{topic: topic, partition: partition}, messages, stream, join_id, batcher) do
    {_, module} =
      Enum.find(stream, fn
        ({^topic, _}) -> true
        (_) -> false
      end)

    bulk =
      messages
      |> Enum.map(fn(message) -> message end)
      |> Enum.map(fn(message) ->
        alias Rivulet.Kafka.Consumer.Message

        with {:ok, key} <- deserialize_key(module, message),
             {:ok, value} <- deserialize_value(module, message),
             {:ok, object_id} <- get_object_id(module, key, value),
             {:ok, join_key} when is_binary(join_key) <- module.join_key(key, value) do
               message = %Message{message | decoded_key: key, decoded_value: value}

              {join_key, message, object_id}
        else
          err ->
            Logger.error("#{module} did not correctly handle message #{inspect message} - got #{inspect err}. Dropping message.")
            nil
             end
      end)
      |> Enum.reject(fn
        (nil) -> true
        (_) -> false
      end)
      |> Enum.map(fn({join_key, message, object_id}) ->
        {:put, join_key, object_id, message.decoded_value}
      end)

    join_keys =
      Enum.map(bulk, fn({:put, join_key, _message, _object_id}) ->
        join_key
      end)

    offset =
      messages
      |> List.last
      |> Map.get(:offset)

    bulk_doc = ElasticSearch.bulk_put_join_doc(bulk, join_id)

    Rivulet.Join.Batcher.batch_commands(batcher, bulk_doc, join_keys, topic, partition, offset)
  end

  @type ignored :: term

  @spec transforms([[term]], {module, [{Rivulet.Kafka.Partition.topic, :key | :random}]})
  :: ignored
  def transforms(join_docs, transformers) do
    IO.inspect(join_docs, label: "join_docs")
    Enum.map(join_docs, fn(join) ->
      IO.inspect(transformers, label: "transformers")
      Enum.map(transformers, fn({module, publishes}) ->
        messages = module.handle_join(join)

        messages =
          case messages do
            list when is_list(list) -> list
            other -> [other]
          end

        publishes =
          messages
          |> List.flatten
          |> Enum.reject(fn
            (nil) -> true
            ({_key, _value}) -> false
            (other) ->
              Logger.error("#{module}.handle_join/1 returned a bad value: #{inspect other}. Dropping message.")
              true
          end)
          |> Enum.map(fn({k, v}) ->
            Enum.map(publishes, fn
              ({topic, :key}) ->
                %Rivulet.Kafka.Publisher.Message{
                  key: k,
                  value: v,
                  encoding_strategy: :raw,
                  topic: topic,
                  partition_strategy: {:key, k}
                }
              ({topic, :random}) ->
                %Rivulet.Kafka.Publisher.Message{
                  key: k,
                  value: v,
                  encoding_strategy: :raw,
                  topic: topic,
                  partition_strategy: :random
                }
            end)
          end)
          |> List.flatten

        refs = Rivulet.Kafka.Publisher.publish(publishes)
        Enum.map(refs, fn
          ({:ok, call_ref}) ->
            receive do
              {:brod_produce_reply, ^call_ref, :brod_produce_req_acked} -> :ok
              {:brod_produce_reply, ^call_ref, other} ->
                Logger.error("Pulishing message failed: #{inspect other}")
                raise "Kafka publish failed."
            after 1000 ->
              raise "Publish took more than 1 second"
            end
        end)
      end)
    end)
  end
end
