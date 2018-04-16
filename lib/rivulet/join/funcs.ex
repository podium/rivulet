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

    join_key_object_id_combo =
      Enum.map(bulk, fn({:put, join_key, object_id, _decoded_value}) ->
        {join_key, object_id}
      end)

    offset =
      messages
      |> List.last
      |> Map.get(:offset)

    bulk_doc = ElasticSearch.bulk_put_join_doc(bulk, join_id)

    Rivulet.Join.Batcher.batch_commands(batcher, bulk_doc, join_key_object_id_combo, topic, partition, offset)
  end

  @type ignored :: term

  @doc """
  join_docs: [
  [
    %{
      "created_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
      "customer_name" => "Dan",
      "last_modified_at" => #DateTime<2018-04-15 19:27:23.926410Z>,
      "location_uid" => <<60, 203, 36, 228, 44, 92, 81, 159, 184, 205, 119, 18,
        242, 43, 166, 43>>,
      "phone_number" => "+15555554",
      "sent_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
      "uid" => <<178, 249, 191, 234, 62, 214, 89, 21, 158, 151, 96, 88, 156, 11,
        42, 39>>
    },
    %{
      "comment" => "New Comment",
      "inserted_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
      "last_modified_at" => #DateTime<2018-04-15 19:28:53.457296Z>,
      "likelihood_to_recommend" => 10,
      "nps_invitation_uid" => <<178, 249, 191, 234, 62, 214, 89, 21, 158, 151,
        96, 88, 156, 11, 42, 39>>,
      "uid" => <<73, 227, 38, 147, 46, 191, 93, 97, 138, 82, 196, 167, 8, 109,
        126, 25>>
    }
  ],
  [
    %{
      "created_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
      "customer_name" => "Dan",
      "last_modified_at" => #DateTime<2018-04-15 19:27:23.926410Z>,
      "location_uid" => <<60, 203, 36, 228, 44, 92, 81, 159, 184, 205, 119, 18,
        242, 43, 166, 43>>,
      "phone_number" => "+15555558",
      "sent_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
      "uid" => <<222, 204, 177, 65, 106, 58, 81, 39, 129, 17, 29, 121, 54, 249,
        133, 223>>
    }
  ],
  [
    %{
      "created_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
      "customer_name" => "Dan",
      "last_modified_at" => #DateTime<2018-04-15 19:25:57.553577Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "phone_number" => "+15555556",
      "sent_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
      "uid" => <<192, 238, 2, 53, 80, 105, 87, 115, 177, 30, 40, 10, 188, 164,
        188, 32>>
    },
    %{
      "comment" => "Updated Comment",
      "inserted_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
      "last_modified_at" => #DateTime<2018-04-15 19:28:53.457296Z>,
      "likelihood_to_recommend" => 10,
      "nps_invitation_uid" => <<192, 238, 2, 53, 80, 105, 87, 115, 177, 30, 40,
        10, 188, 164, 188, 32>>,
      "uid" => <<240, 49, 92, 84, 178, 114, 94, 147, 134, 249, 152, 151, 194,
        216, 96, 197>>
    }
  ],
  [
    %{
      "created_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
      "customer_name" => "Dan",
      "last_modified_at" => #DateTime<2018-04-15 19:25:57.553577Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "phone_number" => "+15555558",
      "sent_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
      "uid" => <<100, 172, 15, 213, 159, 35, 93, 21, 151, 195, 93, 44, 32, 134,
        215, 99>>
    }
  ],
  [
    %{
      "comment" => "C",
      "inserted_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
      "last_modified_at" => #DateTime<2018-04-15 19:28:25.690017Z>,
      "likelihood_to_recommend" => 10,
      "nps_invitation_uid" => <<120, 150, 134, 191, 250, 77, 86, 154, 154, 159,
        15, 214, 100, 150, 250, 72>>,
      "uid" => <<99, 234, 26, 194, 40, 212, 80, 237, 131, 150, 5, 95, 147, 38,
        179, 128>>
    },
    %{
      "created_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
      "customer_name" => "Dan",
      "last_modified_at" => #DateTime<2018-04-15 19:25:57.553577Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "phone_number" => "+15555555",
      "sent_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
      "uid" => <<120, 150, 134, 191, 250, 77, 86, 154, 154, 159, 15, 214, 100,
        150, 250, 72>>
    }
  ],
  [
    %{
      "created_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
      "customer_name" => "Dan",
      "last_modified_at" => #DateTime<2018-04-15 19:25:57.553577Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "phone_number" => "+15555554",
      "sent_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
      "uid" => <<85, 71, 26, 31, 63, 154, 85, 210, 144, 195, 142, 4, 193, 119,
        54, 23>>
    }
  ],
  [
    %{
      "comment" => "C",
      "inserted_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
      "last_modified_at" => #DateTime<2018-04-15 19:28:25.690017Z>,
      "likelihood_to_recommend" => 10,
      "nps_invitation_uid" => <<120, 150, 134, 191, 250, 77, 86, 154, 154, 159,
        15, 214, 100, 150, 250, 72>>,
      "uid" => <<99, 234, 26, 194, 40, 212, 80, 237, 131, 150, 5, 95, 147, 38,
        179, 128>>
    },
    %{
      "created_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
      "customer_name" => "Dan",
      "last_modified_at" => #DateTime<2018-04-15 19:25:57.553577Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "phone_number" => "+15555555",
      "sent_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
      "uid" => <<120, 150, 134, 191, 250, 77, 86, 154, 154, 159, 15, 214, 100,
        150, 250, 72>>
    }
  ],
  [
    %{
      "created_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
      "customer_name" => "Dan",
      "last_modified_at" => #DateTime<2018-04-15 19:25:57.553577Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "phone_number" => "+15555556",
      "sent_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
      "uid" => <<192, 238, 2, 53, 80, 105, 87, 115, 177, 30, 40, 10, 188, 164,
        188, 32>>
    },
    %{
      "comment" => "Updated Comment",
      "inserted_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
      "last_modified_at" => #DateTime<2018-04-15 19:28:53.457296Z>,
      "likelihood_to_recommend" => 10,
      "nps_invitation_uid" => <<192, 238, 2, 53, 80, 105, 87, 115, 177, 30, 40,
        10, 188, 164, 188, 32>>,
      "uid" => <<240, 49, 92, 84, 178, 114, 94, 147, 134, 249, 152, 151, 194,
        216, 96, 197>>
    }
  ],
  [
    %{
      "created_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
      "customer_name" => "Dan",
      "last_modified_at" => #DateTime<2018-04-15 19:25:57.553577Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "phone_number" => "+15555556",
      "sent_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
      "uid" => <<192, 238, 2, 53, 80, 105, 87, 115, 177, 30, 40, 10, 188, 164,
        188, 32>>
    },
    %{
      "comment" => "Updated Comment",
      "inserted_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
      "last_modified_at" => #DateTime<2018-04-15 19:28:53.457296Z>,
      "likelihood_to_recommend" => 10,
      "nps_invitation_uid" => <<192, 238, 2, 53, 80, 105, 87, 115, 177, 30, 40,
        10, 188, 164, 188, 32>>,
      "uid" => <<240, 49, 92, 84, 178, 114, 94, 147, 134, 249, 152, 151, 194,
        216, 96, 197>>
    }
  ],
  [
    %{
      "created_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
      "customer_name" => "Dan",
      "last_modified_at" => #DateTime<2018-04-15 19:27:23.926410Z>,
      "location_uid" => <<60, 203, 36, 228, 44, 92, 81, 159, 184, 205, 119, 18,
        242, 43, 166, 43>>,
      "phone_number" => "+15555554",
      "sent_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
      "uid" => <<178, 249, 191, 234, 62, 214, 89, 21, 158, 151, 96, 88, 156, 11,
        42, 39>>
    },
    %{
      "comment" => "New Comment",
      "inserted_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
      "last_modified_at" => #DateTime<2018-04-15 19:28:53.457296Z>,
      "likelihood_to_recommend" => 10,
      "nps_invitation_uid" => <<178, 249, 191, 234, 62, 214, 89, 21, 158, 151,
        96, 88, 156, 11, 42, 39>>,
      "uid" => <<73, 227, 38, 147, 46, 191, 93, 97, 138, 82, 196, 167, 8, 109,
        126, 25>>
    }
  ],
  [
    %{
      "created_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
      "customer_name" => "Dan",
      "last_modified_at" => #DateTime<2018-04-15 19:27:23.926410Z>,
      "location_uid" => <<60, 203, 36, 228, 44, 92, 81, 159, 184, 205, 119, 18,
        242, 43, 166, 43>>,
      "phone_number" => "+15555554",
      "sent_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
      "uid" => <<178, 249, 191, 234, 62, 214, 89, 21, 158, 151, 96, 88, 156, 11,
        42, 39>>
    },
    %{
      "comment" => "New Comment",
      "inserted_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
      "last_modified_at" => #DateTime<2018-04-15 19:28:53.457296Z>,
      "likelihood_to_recommend" => 10,
      "nps_invitation_uid" => <<178, 249, 191, 234, 62, 214, 89, 21, 158, 151,
        96, 88, 156, 11, 42, 39>>,
      "uid" => <<73, 227, 38, 147, 46, 191, 93, 97, 138, 82, 196, 167, 8, 109,
        126, 25>>
    }
  ]
]
  """
  def transforms(join_docs, transformers, join_key_object_id_combo) do
    IO.inspect(join_key_object_id_combo, label: "join_key_object_id_combo")

    Enum.map(join_docs, fn(join) ->
      IO.inspect(join, label: "join_doc")
      Enum.map(transformers, fn({module, publishes} = thing) ->
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
