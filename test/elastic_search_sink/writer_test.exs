defmodule Rivulet.ElasticSearchSink.Writer.Test do
  use ExUnit.Case

  alias Rivulet.Kafka.Publisher.Message

  @test_module Rivulet.Kafka.Publisher

  setup do
    records = [
      %Rivulet.Kafka.Consumer.Message{
        attributes: 0,
        crc: 1335424316,
        decoded_key: "5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5",
        decoded_value: %{
          "adjusted_score" => 100,
          "created_at" => "2018-04-04T14:32:57.658972Z",
          "customer_name" => "Arthur Weagel",
          "customer_phone" => "+15555555555",
          "invitation_sent_at" => "2018-04-04T14:32:57.658972Z",
          "location_address" => "3190 Auto Center Cir, Stockton, CA 95212, USA",
          "location_name" => "Paul Blanco's Good Car Company",
          "location_uid" => "5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5",
          "nps_comment" => "Some Comment",
          "nps_invitation_uid" => "5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5",
          "nps_response_uid" => "5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5",
          "organization_uid" => "5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5",
          "nps_score" => 10,
          "response_received_at" => "2018-04-04T14:32:57.658972Z"
        },
        key_schema: nil,
        offset: 0,
        raw_key: nil,
        raw_value: nil,
        value_schema: nil
      }, %Rivulet.Kafka.Consumer.Message{
        attributes: 0,
        crc: 1335424316,
        decoded_key: "5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5",
        decoded_value: %{
          "adjusted_score" => 100,
          "created_at" => "2018-04-04T14:32:57.658972Z",
          "customer_name" => "Arthur Weagel",
          "customer_phone" => "+15555555555",
          "invitation_sent_at" => "2018-04-04T14:32:57.658972Z",
          "location_address" => "3190 Auto Center Cir, Stockton, CA 95212, USA",
          "location_name" => "Paul Blanco's Good Car Company",
          "location_uid" => "5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5",
          "nps_comment" => "Some Comment",
          "nps_invitation_uid" => "5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5",
          "nps_response_uid" => "5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5",
          "organization_uid" => "5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5",
          "nps_score" => 10,
          "response_received_at" => "2018-04-04T14:32:57.658972Z"
        },
        key_schema: nil,
        offset: 0,
        raw_key: nil,
        raw_value: nil,
        value_schema: nil
      }]

    topic = "my-topic"
    elastic_index = "my-elastic_index"
    elastic_url = "my-elastic_url"
    elastic_type = "my-elastic_type"
    elastic_mapping = %{}

    config =
      %@test_module.Config{
        consumer_group: "#{topic}-elasticsearch-sink",
        elastic_index: elastic_index,
        elastic_url: elastic_url,
        elastic_type: elastic_type,
        topic: topic,
        elastic_mapping: elastic_mapping
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
          },
          %{
            update: %{
              _index: "test",
              _type: "test",
              _id: "332",
              status: 404,
              error: %{
                type: "document_missing_exception",
                reason: "[test][332]: document missing",
                shard: "-1",
                index: "test"
              }
            }
          }
        ],
        took: 46
      }
    }

    {:ok, %{records: records, config: config, es_resp: post_raw_return_value}}
  end

  describe "filter_for_successfully_inserted/2" do
    test "only returns messages that were successfully saved within elastic", ctx do
      post_raw = %{
        errors: true,
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
          },
          %{
            update: %{
              _index: "test",
              _type: "test",
              _id: "332",
              status: 404,
              error: %{
                type: "document_missing_exception",
                reason: "[test][332]: document missing",
                shard: "-1",
                index: "test"
              }
            }
          }
        ],
        took: 46
      }

      assert %{
        "adjusted_score" => 100,
        "created_at" => "2018-04-04T14:32:57.658972Z",
        "customer_name" => "Arthur Weagel",
        "customer_phone" => "+15555555555",
        "invitation_sent_at" => "2018-04-04T14:32:57.658972Z",
        "location_address" => "3190 Auto Center Cir, Stockton, CA 95212, USA",
        "location_name" => "Paul Blanco's Good Car Company",
        "location_uid" => "5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5",
        "nps_comment" => "Some Comment",
        "nps_invitation_uid" => "5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5",
        "nps_response_uid" => "5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5",
        "organization_uid" => "5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5",
        "nps_score" => 10,
        "response_received_at" => "2018-04-04T14:32:57.658972Z"
      } == @test_module.filter_for_successfully_inserted(post_raw, ctx.records)
    end
  end

  describe "message_successfully_inserted?/1" do
    test "successfully traverses an index operation" do
      es_resp_doc = %{
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
      assert @test_module.message_successfully_inserted?(es_resp_doc)
    end

    test "successfully traverses an update operation" do
      es_resp_doc = %{
        update: %{
          _index: "test",
          _type: "test",
          _id: "332",
          status: 404,
          error: %{
            type: "document_missing_exception",
            reason: "[test][332]: document missing",
            shard: "-1",
            index: "test"
          }
        }
      }
      refute @test_module.message_successfully_inserted?(es_resp_doc)
    end
  end
end
