defmodule Rivulet.Join.Batcher do
  @behaviour :gen_statem
  require Logger

  alias Rivulet.Join.ElasticSearch

  defmodule Data do
    defstruct [:handler, updates: [], ack_data: [], join_keys: []]
  end

  @empty_state :empty
  @filling_state :filling
  @flush_event :flush

  alias Rivulet.Kafka.Partition

  def batch_commands(batcher, cmds, join_key_object_id_combo, topic, partition, offset) do
    :gen_statem.call(batcher, {:add_batch, cmds, join_key_object_id_combo, {topic, partition, offset}})
  end

  def start_link(handler) do
    :gen_statem.start_link(__MODULE__, {handler}, [])
  end

  def callback_mode, do: [:handle_event_function, :state_enter]

  def init({handler}) do
    {:ok, @empty_state, %Data{handler: handler}}
  end

  def handle_event({:call, from}, {:add_batch, cmds, join_keys, {_topic, _partition, _offset} = ack_data}, @empty_state, %Data{} = data) do
    new_data = %Data{data | updates: [cmds | data.updates], ack_data: [ack_data | data.ack_data], join_keys: [join_keys | data.join_keys]}

    {:next_state, @filling_state, new_data, [{:reply, from, :accepted}]}
  end

  def handle_event(:enter, @empty_state, @filling_state, %Data{} = data) do
    {:next_state, @filling_state, data, [{:state_timeout, 500, @flush_event}]}
  end

  def handle_event(:enter, _any, @empty_state, %Data{}) do
    :keep_state_and_data
  end

  def handle_event({:call, from}, {:add_batch, cmds, join_keys, {_topic, _partition, _offset} = ack_data}, @filling_state, %Data{} = data) do
    new_data = %Data{data | updates: [cmds | data.updates], ack_data: [ack_data | data.ack_data], join_keys: [join_keys | data.join_keys]}

    {:keep_state, new_data, [{:reply, from, :accepted}]}
  end

  @doc """
  data when it is flushed: %Rivulet.Join.Batcher.Data{
  ack_data: [
    {"platform_nps_responses", 9,
     {"b2f9bfea-3ed6-5915-9e97-60589c0b2a27",
      %Rivulet.Kafka.Consumer.Message{
        attributes: 0,
        crc: 770482052,
        decoded_key: <<73, 227, 38, 147, 46, 191, 93, 97, 138, 82, 196, 167, 8,
          109, 126, 25>>,
        decoded_value: %{
          "comment" => "New Comment",
          "inserted_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
          "last_modified_at" => #DateTime<2018-04-15 19:28:53.457296Z>,
          "likelihood_to_recommend" => 10,
          "nps_invitation_uid" => <<178, 249, 191, 234, 62, 214, 89, 21, 158,
            151, 96, 88, 156, 11, 42, 39>>,
          "uid" => <<73, 227, 38, 147, 46, 191, 93, 97, 138, 82, 196, 167, 8,
            109, 126, 25>>
        },
        key_schema: nil,
        offset: 1,
        raw_key: <<0, 0, 0, 0, 1, 73, 227, 38, 147, 46, 191, 93, 97, 138, 82,
          196, 167, 8, 109, 126, 25>>,
        raw_value: <<0, 0, 0, 0, 13, 2, 73, 227, 38, 147, 46, 191, 93, 97, 138,
          82, 196, 167, 8, 109, 126, 25, 128, 234, 216, 226, 243, 172, 217, 165,
          42, 178, 249, 191, 234, ...>>,
        value_schema: nil
      }, "49e32693-2ebf-5d61-8a52-c4a7086d7e19"}},
    {"platform_nps_responses", 7,
     {"c0ee0235-5069-5773-b11e-280abca4bc20",
      %Rivulet.Kafka.Consumer.Message{
        attributes: 0,
        crc: 1797878796,
        decoded_key: <<240, 49, 92, 84, 178, 114, 94, 147, 134, 249, 152, 151,
          194, 216, 96, 197>>,
        decoded_value: %{
          "comment" => "Updated Comment",
          "inserted_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
          "last_modified_at" => #DateTime<2018-04-15 19:28:53.457296Z>,
          "likelihood_to_recommend" => 10,
          "nps_invitation_uid" => <<192, 238, 2, 53, 80, 105, 87, 115, 177, 30,
            40, 10, 188, 164, 188, 32>>,
          "uid" => <<240, 49, 92, 84, 178, 114, 94, 147, 134, 249, 152, 151,
            194, 216, 96, 197>>
        },
        key_schema: nil,
        offset: 1,
        raw_key: <<0, 0, 0, 0, 1, 240, 49, 92, 84, 178, 114, 94, 147, 134, 249,
          152, 151, 194, 216, 96, 197>>,
        raw_value: <<0, 0, 0, 0, 13, 2, 240, 49, 92, 84, 178, 114, 94, 147, 134,
          249, 152, 151, 194, 216, 96, 197, 128, 234, 216, 226, 243, 172, 217,
          165, 42, 192, 238, 2, ...>>,
        value_schema: nil
      }, "f0315c54-b272-5e93-86f9-9897c2d860c5"}},
    {"platform_nps_responses", 6,
     {"789686bf-fa4d-569a-9a9f-0fd66496fa48",
      %Rivulet.Kafka.Consumer.Message{
        attributes: 0,
        crc: 3092665416,
        decoded_key: <<99, 234, 26, 194, 40, 212, 80, 237, 131, 150, 5, 95, 147,
          38, 179, 128>>,
        decoded_value: %{
          "comment" => "C",
          "inserted_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
          "last_modified_at" => #DateTime<2018-04-15 19:28:25.690017Z>,
          "likelihood_to_recommend" => 10,
          "nps_invitation_uid" => <<120, 150, 134, 191, 250, 77, 86, 154, 154,
            159, 15, 214, 100, 150, 250, 72>>,
          "uid" => <<99, 234, 26, 194, 40, 212, 80, 237, 131, 150, 5, 95, 147,
            38, 179, 128>>
        },
        key_schema: nil,
        offset: 0,
        raw_key: <<0, 0, 0, 0, 1, 99, 234, 26, 194, 40, 212, 80, 237, 131, 150,
          5, 95, 147, 38, 179, 128>>,
        raw_value: <<0, 0, 0, 0, 13, 2, 99, 234, 26, 194, 40, 212, 80, 237, 131,
          150, 5, 95, 147, 38, 179, 128, 208, 179, 224, 241, 164, 171, 217, 165,
          42, 120, 150, ...>>,
        value_schema: nil
      }, "63ea1ac2-28d4-50ed-8396-055f9326b380"}},
    {"platform_nps_invitations", 9,
     {"55471a1f-3f9a-55d2-90c3-8e04c1773617",
      %Rivulet.Kafka.Consumer.Message{
        attributes: 0,
        crc: 1478262688,
        decoded_key: <<85, 71, 26, 31, 63, 154, 85, 210, 144, 195, 142, 4, 193,
          119, 54, 23>>,
        decoded_value: %{
          "created_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
          "customer_name" => "Dan",
          "last_modified_at" => #DateTime<2018-04-15 19:25:57.553577Z>,
          "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
            132, 183, 140, 176, 197>>,
          "phone_number" => "+15555554",
          "sent_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
          "uid" => <<85, 71, 26, 31, 63, 154, 85, 210, 144, 195, 142, 4, 193,
            119, 54, 23>>
        },
        key_schema: nil,
        offset: 0,
        raw_key: <<0, 0, 0, 0, 1, 85, 71, 26, 31, 63, 154, 85, 210, 144, 195,
          142, 4, 193, 119, 54, 23>>,
        raw_value: <<0, 0, 0, 0, 12, 2, 95, 208, 59, 248, 156, 214, 82, 10, 178,
          227, 144, 132, 183, 140, 176, 197, 144, 131, 245, 138, 254, 247, 215,
          165, 42, 6, ...>>,
        value_schema: nil
      }, "55471a1f-3f9a-55d2-90c3-8e04c1773617"}},
    {"platform_nps_invitations", 6,
     {"789686bf-fa4d-569a-9a9f-0fd66496fa48",
      %Rivulet.Kafka.Consumer.Message{
        attributes: 0,
        crc: 2884301174,
        decoded_key: <<120, 150, 134, 191, 250, 77, 86, 154, 154, 159, 15, 214,
          100, 150, 250, 72>>,
        decoded_value: %{
          "created_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
          "customer_name" => "Dan",
          "last_modified_at" => #DateTime<2018-04-15 19:25:57.553577Z>,
          "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
            132, 183, 140, 176, 197>>,
          "phone_number" => "+15555555",
          "sent_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
          "uid" => <<120, 150, 134, 191, 250, 77, 86, 154, 154, 159, 15, 214,
            100, 150, 250, 72>>
        },
        key_schema: nil,
        offset: 0,
        raw_key: <<0, 0, 0, 0, 1, 120, 150, 134, 191, 250, 77, 86, 154, 154,
          159, 15, 214, 100, 150, 250, 72>>,
        raw_value: <<0, 0, 0, 0, 12, 2, 95, 208, 59, 248, 156, 214, 82, 10, 178,
          227, 144, 132, 183, 140, 176, 197, 144, 131, 245, 138, 254, 247, 215,
          165, 42, ...>>,
        value_schema: nil
      }, "789686bf-fa4d-569a-9a9f-0fd66496fa48"}},
    {"platform_nps_invitations", 3,
     {"64ac0fd5-9f23-5d15-97c3-5d2c2086d763",
      %Rivulet.Kafka.Consumer.Message{
        attributes: 0,
        crc: 2056167879,
        decoded_key: <<100, 172, 15, 213, 159, 35, 93, 21, 151, 195, 93, 44, 32,
          134, 215, 99>>,
        decoded_value: %{
          "created_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
          "customer_name" => "Dan",
          "last_modified_at" => #DateTime<2018-04-15 19:25:57.553577Z>,
          "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
            132, 183, 140, 176, 197>>,
          "phone_number" => "+15555558",
          "sent_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
          "uid" => <<100, 172, 15, 213, 159, 35, 93, 21, 151, 195, 93, 44, 32,
            134, 215, 99>>
        },
        key_schema: nil,
        offset: 0,
        raw_key: <<0, 0, 0, 0, 1, 100, 172, 15, 213, 159, 35, 93, 21, 151, 195,
          93, 44, 32, 134, 215, 99>>,
        raw_value: <<0, 0, 0, 0, 12, 2, 95, 208, 59, 248, 156, 214, 82, 10, 178,
          227, 144, 132, 183, 140, 176, 197, 144, 131, 245, 138, 254, 247, 215,
          165, ...>>,
        value_schema: nil
      }, "64ac0fd5-9f23-5d15-97c3-5d2c2086d763"}},
    {"platform_nps_invitations", 2,
     {"c0ee0235-5069-5773-b11e-280abca4bc20",
      %Rivulet.Kafka.Consumer.Message{
        attributes: 0,
        crc: 113904053,
        decoded_key: <<192, 238, 2, 53, 80, 105, 87, 115, 177, 30, 40, 10, 188,
          164, 188, 32>>,
        decoded_value: %{
          "created_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
          "customer_name" => "Dan",
          "last_modified_at" => #DateTime<2018-04-15 19:25:57.553577Z>,
          "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
            132, 183, 140, 176, 197>>,
          "phone_number" => "+15555556",
          "sent_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
          "uid" => <<192, 238, 2, 53, 80, 105, 87, 115, 177, 30, 40, 10, 188,
            164, 188, 32>>
        },
        key_schema: nil,
        offset: 0,
        raw_key: <<0, 0, 0, 0, 1, 192, 238, 2, 53, 80, 105, 87, 115, 177, 30,
          40, 10, 188, 164, 188, 32>>,
        raw_value: <<0, 0, 0, 0, 12, 2, 95, 208, 59, 248, 156, 214, 82, 10, 178,
          227, 144, 132, 183, 140, 176, 197, 144, 131, 245, 138, 254, 247, 215,
          ...>>,
        value_schema: nil
      }, "c0ee0235-5069-5773-b11e-280abca4bc20"}},
    {"platform_nps_invitations", 1,
     {"deccb141-6a3a-5127-8111-1d7936f985df",
      %Rivulet.Kafka.Consumer.Message{
        attributes: 0,
        crc: 3414192757,
        decoded_key: <<222, 204, 177, 65, 106, 58, 81, 39, 129, 17, 29, 121, 54,
          249, 133, 223>>,
        decoded_value: %{
          "created_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
          "customer_name" => "Dan",
          "last_modified_at" => #DateTime<2018-04-15 19:27:23.926410Z>,
          "location_uid" => <<60, 203, 36, 228, 44, 92, 81, 159, 184, 205, 119,
            18, 242, 43, 166, 43>>,
          "phone_number" => "+15555558",
          "sent_at" => #DateTime<2018-04-15 18:37:05.285325Z>,
          "uid" => <<222, 204, 177, 65, 106, 58, 81, 39, 129, 17, 29, 121, 54,
            249, 133, 223>>
        },
        key_schema: nil,
        offset: 1,
        raw_key: <<0, 0, 0, 0, 1, 222, 204, 177, 65, 106, 58, 81, 39, 129, 17,
          29, 121, 54, 249, 133, 223>>,
        raw_value: <<0, 0, 0, 0, 12, 2, 60, 203, 36, 228, 44, 92, 81, 159, 184,
          205, 119, 18, 242, 43, 166, 43, 144, 131, 245, 138, 254, 247, ...>>,
        value_schema: nil
      }, "deccb141-6a3a-5127-8111-1d7936f985df"}}
  ],
  handler: #PID<0.982.0>,
  join_keys: [
    [
      {"b2f9bfea-3ed6-5915-9e97-60589c0b2a27",
       "49e32693-2ebf-5d61-8a52-c4a7086d7e19"},
      {"b2f9bfea-3ed6-5915-9e97-60589c0b2a27",
       "49e32693-2ebf-5d61-8a52-c4a7086d7e19"}
    ],
    [
      {"c0ee0235-5069-5773-b11e-280abca4bc20",
       "f0315c54-b272-5e93-86f9-9897c2d860c5"},
      {"c0ee0235-5069-5773-b11e-280abca4bc20",
       "f0315c54-b272-5e93-86f9-9897c2d860c5"}
    ],
    [
      {"789686bf-fa4d-569a-9a9f-0fd66496fa48",
       "63ea1ac2-28d4-50ed-8396-055f9326b380"}
    ],
    [
      {"55471a1f-3f9a-55d2-90c3-8e04c1773617",
       "55471a1f-3f9a-55d2-90c3-8e04c1773617"}
    ],
    [
      {"789686bf-fa4d-569a-9a9f-0fd66496fa48",
       "789686bf-fa4d-569a-9a9f-0fd66496fa48"}
    ],
    [
      {"64ac0fd5-9f23-5d15-97c3-5d2c2086d763",
       "64ac0fd5-9f23-5d15-97c3-5d2c2086d763"}
    ],
    [
      {"c0ee0235-5069-5773-b11e-280abca4bc20",
       "c0ee0235-5069-5773-b11e-280abca4bc20"}
    ],
    [
      {"b2f9bfea-3ed6-5915-9e97-60589c0b2a27",
       "b2f9bfea-3ed6-5915-9e97-60589c0b2a27"},
      {"deccb141-6a3a-5127-8111-1d7936f985df",
       "deccb141-6a3a-5127-8111-1d7936f985df"}
    ]
  ],
  updates: []}


  join_keys in batcher: [
  ---- these were the first to come in ---
  {"b2f9bfea-3ed6-5915-9e97-60589c0b2a27",
   "b2f9bfea-3ed6-5915-9e97-60589c0b2a27"},
  {"deccb141-6a3a-5127-8111-1d7936f985df",
   "deccb141-6a3a-5127-8111-1d7936f985df"},
  {"c0ee0235-5069-5773-b11e-280abca4bc20",
   "c0ee0235-5069-5773-b11e-280abca4bc20"},
  {"64ac0fd5-9f23-5d15-97c3-5d2c2086d763",
   "64ac0fd5-9f23-5d15-97c3-5d2c2086d763"},
  {"789686bf-fa4d-569a-9a9f-0fd66496fa48",
   "789686bf-fa4d-569a-9a9f-0fd66496fa48"},
  {"55471a1f-3f9a-55d2-90c3-8e04c1773617",
   "55471a1f-3f9a-55d2-90c3-8e04c1773617"},
  {"789686bf-fa4d-569a-9a9f-0fd66496fa48",
   "63ea1ac2-28d4-50ed-8396-055f9326b380"},
  {"c0ee0235-5069-5773-b11e-280abca4bc20",
   "f0315c54-b272-5e93-86f9-9897c2d860c5"},
  {"c0ee0235-5069-5773-b11e-280abca4bc20",
   "f0315c54-b272-5e93-86f9-9897c2d860c5"},
  {"b2f9bfea-3ed6-5915-9e97-60589c0b2a27",
   "49e32693-2ebf-5d61-8a52-c4a7086d7e19"},
  {"b2f9bfea-3ed6-5915-9e97-60589c0b2a27",
   "49e32693-2ebf-5d61-8a52-c4a7086d7e19"}
   --- these were the last to come in ---
]
  """
  def handle_event(:state_timeout, :flush, @filling_state, %Data{} = data) do
    resp =
      data.updates
      |> Enum.reverse
      |> flush

    Enum.each(resp["items"], fn(item) ->
      unless item["update"]["status"] < 300 do
        Logger.error("Elasticsearch updates failed. #{inspect item}")
      end
    end)

    join_key_object_id_combo =
      data.join_keys
      |> Enum.reverse
      |> List.flatten

    Rivulet.Join.Handler.handle_resp(data.handler, join_key_object_id_combo, Enum.reverse(data.ack_data))

    {:next_state, @empty_state, %Data{handler: data.handler}}
  end

  def handle_event(_type, event, _state, _data) do
    Logger.warn("Join Batcher received unknown event: #{inspect event}")
    :keep_state_and_data
  end

  #def handle_event({updates,})

  @spec flush([ElasticSearch.batch]) :: term
  defp flush(batch) do
    ElasticSearch.bulk_update(batch)
  end
end
