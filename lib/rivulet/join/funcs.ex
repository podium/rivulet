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

  def handle_messages(%Rivulet.Kafka.Partition{topic: topic, partition: partition} = p, messages, stream, join_id, batcher) do
    IO.inspect(p, label: "Rivulet.Kafka.Partition")
    IO.inspect(messages, label: "messages")
    IO.inspect(stream, label: "stream")
    IO.inspect(join_id, label: "join_id")
    IO.inspect(batcher, label: "batcher")

    {_, module} =
      Enum.find(stream, fn
        ({^topic, _}) -> true
        (_) -> false
      end)

    IO.inspect(module, label: "module")

    # require IEx; IEx.pry
    bulk =
      messages
      |> Enum.map(fn(message) -> message end)
      |> Enum.map(fn(message) ->
        alias Rivulet.Kafka.Consumer.Message

        with {:ok, key} <- deserialize_key(module, message) |> IO.inspect(label: "deseralize_key"),
             {:ok, value} <- deserialize_value(module, message) |> IO.inspect(label: "deseralize_value"),
             {:ok, object_id} <- get_object_id(module, key, value) |> IO.inspect(label: "object_id"),
             {:ok, join_key} when is_binary(join_key) <- module.join_key(key, value) |> IO.inspect(label: "join_key") do
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
      |> IO.inspect(label: "bulk")

    # require IEx; IEx.pry
    join_keys =
      Enum.map(bulk, fn({:put, join_key, _message, _object_id}) ->
        join_key
      end)
      |> IO.inspect(label: "join_keys after bulk")

    # require IEx; IEx.pry
    offset =
      messages
      |> List.last
      |> Map.get(:offset)
      |> IO.inspect(label: "offset")

    # require IEx; IEx.pry
    bulk_doc = ElasticSearch.bulk_put_join_doc(bulk, join_id)

    Rivulet.Join.Batcher.batch_commands(batcher, bulk_doc, join_keys, topic, partition, offset)
  end

  @type ignored :: term

  @doc """
  join_docs:

  [
    [
      %{
        "address" => "3190 Auto Center Cir, Stockton, CA 95212, USA",
        "archived" => false,
        "created_at" => "2018-02-24T22:55:42.709274Z",
        "last_modified_at" => "2018-04-11T21:55:42.709818Z",
        "name" => "Paul Blanco's Good Car Company",
        "organization_uid" => "f4ac4bcb-e271-5a92-8e43-1d676a8821fa",
        "podium_number" => "+13853360060",
        "timezone_identifier_uid" => nil,
        "uid" => "5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5",
        "updated_at" => "2018-04-11T21:55:42.736053Z"
      },
      %{
        "adjusted_score" => 100,
        "created_at" => #DateTime<2018-04-11 21:54:54.808000Z>,
        "customer_name" => "DanTwo",
        "customer_phone" => "+18505857617",
        "invitation_sent_at" => #DateTime<2018-04-11 21:54:54.808000Z>,
        "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
          132, 183, 140, 176, 197>>,
        "nps_comment" => "CommentTwo",
        "nps_invitation_uid" => <<192, 238, 2, 53, 80, 105, 87, 115, 177, 30, 40,
          10, 188, 164, 188, 32>>,
        "nps_response_uid" => <<240, 49, 92, 84, 178, 114, 94, 147, 134, 249, 152,
          151, 194, 216, 96, 197>>,
        "nps_score" => 10,
        "response_received_at" => #DateTime<2018-04-11 21:54:54.808000Z>
      },
      %{
        "adjusted_score" => 100,
        "created_at" => #DateTime<2018-04-11 21:54:54.808000Z>,
        "customer_name" => "Dan",
        "customer_phone" => "+18505857616",
        "invitation_sent_at" => #DateTime<2018-04-11 21:54:54.808000Z>,
        "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
          132, 183, 140, 176, 197>>,
        "nps_comment" => "Comment",
        "nps_invitation_uid" => <<120, 150, 134, 191, 250, 77, 86, 154, 154, 159,
          15, 214, 100, 150, 250, 72>>,
        "nps_response_uid" => <<99, 234, 26, 194, 40, 212, 80, 237, 131, 150, 5,
          95, 147, 38, 179, 128>>,
        "nps_score" => 10,
        "response_received_at" => #DateTime<2018-04-11 21:54:54.808000Z>
      }
    ],
    [
      %{
        "address" => "3190 Auto Center Cir, Stockton, CA 95212, USA",
        "archived" => false,
        "created_at" => "2018-02-24T22:55:42.709274Z",
        "last_modified_at" => "2018-04-11T21:55:42.709818Z",
        "name" => "Paul Blanco's Good Car Company",
        "organization_uid" => "f4ac4bcb-e271-5a92-8e43-1d676a8821fa",
        "podium_number" => "+13853360060",
        "timezone_identifier_uid" => nil,
        "uid" => "5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5",
        "updated_at" => "2018-04-11T21:55:42.736053Z"
      },
      %{
        "adjusted_score" => 100,
        "created_at" => #DateTime<2018-04-11 21:54:54.808000Z>,
        "customer_name" => "DanTwo",
        "customer_phone" => "+18505857617",
        "invitation_sent_at" => #DateTime<2018-04-11 21:54:54.808000Z>,
        "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
          132, 183, 140, 176, 197>>,
        "nps_comment" => "CommentTwo",
        "nps_invitation_uid" => <<192, 238, 2, 53, 80, 105, 87, 115, 177, 30, 40,
          10, 188, 164, 188, 32>>,
        "nps_response_uid" => <<240, 49, 92, 84, 178, 114, 94, 147, 134, 249, 152,
          151, 194, 216, 96, 197>>,
        "nps_score" => 10,
        "response_received_at" => #DateTime<2018-04-11 21:54:54.808000Z>
      },
      %{
        "adjusted_score" => 100,
        "created_at" => #DateTime<2018-04-11 21:54:54.808000Z>,
        "customer_name" => "Dan",
        "customer_phone" => "+18505857616",
        "invitation_sent_at" => #DateTime<2018-04-11 21:54:54.808000Z>,
        "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
          132, 183, 140, 176, 197>>,
        "nps_comment" => "Comment",
        "nps_invitation_uid" => <<120, 150, 134, 191, 250, 77, 86, 154, 154, 159,
          15, 214, 100, 150, 250, 72>>,
        "nps_response_uid" => <<99, 234, 26, 194, 40, 212, 80, 237, 131, 150, 5,
          95, 147, 38, 179, 128>>,
        "nps_score" => 10,
        "response_received_at" => #DateTime<2018-04-11 21:54:54.808000Z>
      }
    ],
    [
      %{
        "address" => "3190 Auto Center Cir, Stockton, CA 95212, USA",
        "archived" => false,
        "created_at" => "2018-02-24T22:55:42.709274Z",
        "last_modified_at" => "2018-04-11T21:55:42.709818Z",
        "name" => "Paul Blanco's Good Car Company",
        "organization_uid" => "f4ac4bcb-e271-5a92-8e43-1d676a8821fa",
        "podium_number" => "+13853360060",
        "timezone_identifier_uid" => nil,
        "uid" => "5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5",
        "updated_at" => "2018-04-11T21:55:42.736053Z"
      },
      %{
        "adjusted_score" => 100,
        "created_at" => #DateTime<2018-04-11 21:54:54.808000Z>,
        "customer_name" => "DanTwo",
        "customer_phone" => "+18505857617",
        "invitation_sent_at" => #DateTime<2018-04-11 21:54:54.808000Z>,
        "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
          132, 183, 140, 176, 197>>,
        "nps_comment" => "CommentTwo",
        "nps_invitation_uid" => <<192, 238, 2, 53, 80, 105, 87, 115, 177, 30, 40,
          10, 188, 164, 188, 32>>,
        "nps_response_uid" => <<240, 49, 92, 84, 178, 114, 94, 147, 134, 249, 152,
          151, 194, 216, 96, 197>>,
        "nps_score" => 10,
        "response_received_at" => #DateTime<2018-04-11 21:54:54.808000Z>
      },
      %{
        "adjusted_score" => 100,
        "created_at" => #DateTime<2018-04-11 21:54:54.808000Z>,
        "customer_name" => "Dan",
        "customer_phone" => "+18505857616",
        "invitation_sent_at" => #DateTime<2018-04-11 21:54:54.808000Z>,
        "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
          132, 183, 140, 176, 197>>,
        "nps_comment" => "Comment",
        "nps_invitation_uid" => <<120, 150, 134, 191, 250, 77, 86, 154, 154, 159,
          15, 214, 100, 150, 250, 72>>,
        "nps_response_uid" => <<99, 234, 26, 194, 40, 212, 80, 237, 131, 150, 5,
          95, 147, 38, 179, 128>>,
        "nps_score" => 10,
        "response_received_at" => #DateTime<2018-04-11 21:54:54.808000Z>
      }
    ],
    [
      %{
        "address" => "3190 Auto Center Cir, Stockton, CA 95212, USA",
        "archived" => false,
        "created_at" => "2018-02-24T22:55:42.709274Z",
        "last_modified_at" => "2018-04-11T21:55:42.709818Z",
        "name" => "Paul Blanco's Good Car Company",
        "organization_uid" => "f4ac4bcb-e271-5a92-8e43-1d676a8821fa",
        "podium_number" => "+13853360060",
        "timezone_identifier_uid" => nil,
        "uid" => "5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5",
        "updated_at" => "2018-04-11T21:55:42.736053Z"
      },
      %{
        "adjusted_score" => 100,
        "created_at" => #DateTime<2018-04-11 21:54:54.808000Z>,
        "customer_name" => "DanTwo",
        "customer_phone" => "+18505857617",
        "invitation_sent_at" => #DateTime<2018-04-11 21:54:54.808000Z>,
        "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
          132, 183, 140, 176, 197>>,
        "nps_comment" => "CommentTwo",
        "nps_invitation_uid" => <<192, 238, 2, 53, 80, 105, 87, 115, 177, 30, 40,
          10, 188, 164, 188, 32>>,
        "nps_response_uid" => <<240, 49, 92, 84, 178, 114, 94, 147, 134, 249, 152,
          151, 194, 216, 96, 197>>,
        "nps_score" => 10,
        "response_received_at" => #DateTime<2018-04-11 21:54:54.808000Z>
      },
      %{
        "adjusted_score" => 100,
        "created_at" => #DateTime<2018-04-11 21:54:54.808000Z>,
        "customer_name" => "Dan",
        "customer_phone" => "+18505857616",
        "invitation_sent_at" => #DateTime<2018-04-11 21:54:54.808000Z>,
        "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
          132, 183, 140, 176, 197>>,
        "nps_comment" => "Comment",
        "nps_invitation_uid" => <<120, 150, 134, 191, 250, 77, 86, 154, 154, 159,
          15, 214, 100, 150, 250, 72>>,
        "nps_response_uid" => <<99, 234, 26, 194, 40, 212, 80, 237, 131, 150, 5,
          95, 147, 38, 179, 128>>,
        "nps_score" => 10,
        "response_received_at" => #DateTime<2018-04-11 21:54:54.808000Z>
      }
    ]
  ]

  transformers:

    [
      {FireHydrant.JoinTransformer.NPSLocationJoin,
       [{"platform_nps_location_joins", :key}]}
    ]

  join:

    [
      %{
        "address" => "3190 Auto Center Cir, Stockton, CA 95212, USA",
        "archived" => false,
        "created_at" => "2018-02-24T22:55:42.709274Z",
        "last_modified_at" => "2018-04-11T21:55:42.709818Z",
        "name" => "Paul Blanco's Good Car Company",
        "organization_uid" => "f4ac4bcb-e271-5a92-8e43-1d676a8821fa",
        "podium_number" => "+13853360060",
        "timezone_identifier_uid" => nil,
        "uid" => "5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5",
        "updated_at" => "2018-04-11T21:55:42.736053Z"
      },
      %{
        "adjusted_score" => 100,
        "created_at" => #DateTime<2018-04-11 21:54:54.808000Z>,
        "customer_name" => "DanTwo",
        "customer_phone" => "+18505857617",
        "invitation_sent_at" => #DateTime<2018-04-11 21:54:54.808000Z>,
        "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144, 132,
          183, 140, 176, 197>>,
        "nps_comment" => "CommentTwo",
        "nps_invitation_uid" => <<192, 238, 2, 53, 80, 105, 87, 115, 177, 30, 40,
          10, 188, 164, 188, 32>>,
        "nps_response_uid" => <<240, 49, 92, 84, 178, 114, 94, 147, 134, 249, 152,
          151, 194, 216, 96, 197>>,
        "nps_score" => 10,
        "response_received_at" => #DateTime<2018-04-11 21:54:54.808000Z>
      },
      %{
        "adjusted_score" => 100,
        "created_at" => #DateTime<2018-04-11 21:54:54.808000Z>,
        "customer_name" => "Dan",
        "customer_phone" => "+18505857616",
        "invitation_sent_at" => #DateTime<2018-04-11 21:54:54.808000Z>,
        "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144, 132,
          183, 140, 176, 197>>,
        "nps_comment" => "Comment",
        "nps_invitation_uid" => <<120, 150, 134, 191, 250, 77, 86, 154, 154, 159,
          15, 214, 100, 150, 250, 72>>,
        "nps_response_uid" => <<99, 234, 26, 194, 40, 212, 80, 237, 131, 150, 5, 95,
          147, 38, 179, 128>>,
        "nps_score" => 10,
        "response_received_at" => #DateTime<2018-04-11 21:54:54.808000Z>
      }
    ]

  thing:
    {FireHydrant.JoinTransformer.NPSLocationJoin, [{"platform_nps_location_joins", :key}]}

  publishes:
    [{"platform_nps_location_joins", :key}]

  messages:

    [
      {"c0ee0235-5069-5773-b11e-280abca4bc20",
       "{\"response_received_at\":\"2018-04-11T21:54:54.808000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":10,\"nps_response_uid\":\"f0315c54-b272-5e93-86f9-9897c2d860c5\",\"nps_invitation_uid\":\"c0ee0235-5069-5773-b11e-280abca4bc20\",\"nps_comment\":\"CommentTwo\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-11T21:54:54.808000Z\",\"customer_phone\":\"+18505857617\",\"customer_name\":\"DanTwo\",\"created_at\":\"2018-04-11T21:54:54.808000Z\",\"adjusted_score\":100}"},
      {"789686bf-fa4d-569a-9a9f-0fd66496fa48",
       "{\"response_received_at\":\"2018-04-11T21:54:54.808000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":10,\"nps_response_uid\":\"63ea1ac2-28d4-50ed-8396-055f9326b380\",\"nps_invitation_uid\":\"789686bf-fa4d-569a-9a9f-0fd66496fa48\",\"nps_comment\":\"Comment\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-11T21:54:54.808000Z\",\"customer_phone\":\"+18505857616\",\"customer_name\":\"Dan\",\"created_at\":\"2018-04-11T21:54:54.808000Z\",\"adjusted_score\":100}"}
    ]

  publishes(part2):

    [
      %Rivulet.Kafka.Publisher.Message{
        encoding_strategy: :raw,
        key: "c0ee0235-5069-5773-b11e-280abca4bc20",
        partition: nil,
        partition_strategy: {:key, "c0ee0235-5069-5773-b11e-280abca4bc20"},
        topic: "platform_nps_location_joins",
        value: "{\"response_received_at\":\"2018-04-11T21:54:54.808000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":10,\"nps_response_uid\":\"f0315c54-b272-5e93-86f9-9897c2d860c5\",\"nps_invitation_uid\":\"c0ee0235-5069-5773-b11e-280abca4bc20\",\"nps_comment\":\"CommentTwo\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-11T21:54:54.808000Z\",\"customer_phone\":\"+18505857617\",\"customer_name\":\"DanTwo\",\"created_at\":\"2018-04-11T21:54:54.808000Z\",\"adjusted_score\":100}"
      },
      %Rivulet.Kafka.Publisher.Message{
        encoding_strategy: :raw,
        key: "789686bf-fa4d-569a-9a9f-0fd66496fa48",
        partition: nil,
        partition_strategy: {:key, "789686bf-fa4d-569a-9a9f-0fd66496fa48"},
        topic: "platform_nps_location_joins",
        value: "{\"response_received_at\":\"2018-04-11T21:54:54.808000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":10,\"nps_response_uid\":\"63ea1ac2-28d4-50ed-8396-055f9326b380\",\"nps_invitation_uid\":\"789686bf-fa4d-569a-9a9f-0fd66496fa48\",\"nps_comment\":\"Comment\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-11T21:54:54.808000Z\",\"customer_phone\":\"+18505857616\",\"customer_name\":\"Dan\",\"created_at\":\"2018-04-11T21:54:54.808000Z\",\"adjusted_score\":100}"
      }
    ]

  refs:
    [
      ok: {:brod_call_ref, #PID<0.561.0>, #PID<0.804.0>,
       #Reference<0.1270628481.3392405506.24342>},
      ok: {:brod_call_ref, #PID<0.561.0>, #PID<0.798.0>,
       #Reference<0.1270628481.3392405506.24344>}
    ]
  """
  @spec transforms([[term]], {module, [{Rivulet.Kafka.Partition.topic, :key | :random}]})
  :: ignored
  def transforms(join_docs, transformers) do
    IO.inspect(join_docs, label: "join_docs")
    IO.inspect(transformers, label: "transformers")

    # require IEx; IEx.pry

    Enum.map(join_docs, fn(join) ->
      # require IEx; IEx.pry
      IO.inspect(join, label: "join")

      Enum.map(transformers, fn({module, publishes} = thing) ->
        IO.inspect(thing, label: "thing")
        IO.inspect(publishes, label: "publishes")
        # require IEx; IEx.pry

        messages = module.handle_join(join)

        messages =
          case messages do
            list when is_list(list) -> list
            other -> [other]
          end

        IO.inspect(messages, label: "messages")

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

        IO.inspect(publishes, label: "publishes")
        # require IEx; IEx.pry

        refs = Rivulet.Kafka.Publisher.publish(publishes)

        IO.inspect(refs, label: "refs")
        # require IEx; IEx.pry

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
