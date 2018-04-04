defmodule Rivulet.ElasticSearchSink.Writer.Test do
  use ExUnit.Case

  alias Rivulet.Kafka.Publisher.Message

  @test_module Rivulet.Kafka.Publisher

  setup do
    records = [
      %Rivulet.Kafka.Consumer.Message{
        attributes: 0,
        crc: 1335424316,
        decoded_key: <<120, 150, 134, 191, 250, 77, 86, 154, 154, 159, 15, 214, 100,
          150, 250, 72>>,
        decoded_value: %{
          "adjusted_score" => 100,
          "created_at" => #DateTime<2018-03-24 13:58:23.898272Z>,
          "customer_name" => "Dan Conger",
          "customer_phone" => "+18505857616",
          "invitation_sent_at" => #DateTime<2018-03-24 13:58:23.898272Z>,
          "location_address" => "3190 Auto Center Cir, Stockton, CA 95212, USA",
          "location_name" => "Paul Blanco's Good Car Company",
          "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
            132, 183, 140, 176, 197>>,
          "nps_comment" => "Some Comment",
          "nps_invitation_uid" => <<120, 150, 134, 191, 250, 77, 86, 154, 154, 159,
            15, 214, 100, 150, 250, 72>>,
          "nps_response_uid" => <<99, 234, 26, 194, 40, 212, 80, 237, 131, 150, 5,
            95, 147, 38, 179, 128>>,
          "nps_score" => 10,
          "organization_uid" => <<244, 172, 75, 203, 226, 113, 90, 146, 142, 67, 29,
            103, 106, 136, 33, 250>>,
          "response_received_at" => #DateTime<2018-03-24 13:58:23.898272Z>
        },
        key_schema: nil,
        offset: 0,
        raw_key: nil,
        raw_value: nil,
        value_schema: nil
      }]
    topic = "my-topic"
    config =
      %@test_module.Config{
        unique_constraints: [["a", "b"], ["a"]],
        primary_keys: ["c"],
        table_pattern: name_pattern,
        topic: topic
      }

    post_raw_return_value = %HTTPoison.Response{
      body: %{
        errors: false,
        items: [
          %{
            index: %{
              _id: "V8r7_WEBYAb0v90j0rYd",
              _index: "nps",
              _primary_term: 1,
              _seq_no: 0,
              _shards: %{failed: 0, successful: 1, total: 2},
              _type: "response",
              _version: 1,
              result: "created",
              status: 201
            }
          }
        ],
        took: 46
      }
    }

    {:ok, %{records: records, config: config}}
  end
  describe "filter_for_successfully_inserted/2" do
    test "only returns messages that were successfully saved within elastic", ctx do
      assert %{} == @test_module.group_messages([nil, nil, nil])
    end

    test "successfully traverses an index operation", ctx do
      assert %{} == @test_module.group_messages([nil, nil, nil])
    end

    test "successfully traverses an update operation", ctx do
      assert %{} == @test_module.group_messages([nil, nil, nil])
    end
  end
end
