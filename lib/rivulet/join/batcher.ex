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
  # @spec batch_commands(pid, [ElasticSearch.batch], [String.t], Partition.topic, Partition.partition, non_neg_integer)
  # :: :ok
  def batch_commands(batcher, cmds, join_keys, topic, partition, last_message) do
    :gen_statem.call(batcher, {:add_batch, cmds, join_keys, {topic, partition, last_message}})
  end

  def start_link(handler) do
    :gen_statem.start_link(__MODULE__, {handler}, [])
  end

  def callback_mode, do: [:handle_event_function, :state_enter]

  @doc """
  From join.ex:
    {:ok, handler} = Handler.start_link(...)
    {:ok batcher} = Batcher.start_link(handler)

  handler: is the pid of the handler
  """
  def init({handler}) do
    {:ok, @empty_state, %Data{handler: handler}}
  end

  @doc """
  a: %Rivulet.Join.Batcher.Data{
  ack_data: [{"platform_nps_invitations", 2, 0}],
  handler: #PID<0.559.0>,
  join_keys: [["c0ee0235-5069-5773-b11e-280abca4bc20"]],
  updates: [
    [
      ["{\"update\":{\"_type\":\"join_documents\",\"_index\":\"rivulet-joinkey-nps-join\",\"_id\":\"c0ee0235-5069-5773-b11e-280abca4bc20\"}}",
       "\n",
       "{\"doc_as_upsert\":true,\"doc\":{\"join_key\":\"c0ee0235-5069-5773-b11e-280abca4bc20\",\"document\":\"g3QAAAAHbQAAAApjcmVhdGVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQxkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAK7p1hBmQABm1pbnV0ZWEvZAAFbW9udGhhBGQABnNlY29uZGEBZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADWN1c3RvbWVyX25hbWVtAAAADkRhbiBDb25nZXIgVHdvbQAAABBsYXN0X21vZGlmaWVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQxkAARob3VyYRZkAAttaWNyb3NlY29uZGgCYgAK+SFhBmQABm1pbnV0ZWEiZAAFbW9udGhhBGQABnNlY29uZGEQZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADGxvY2F0aW9uX3VpZG0AAAAQX9A7+JzWUgqy45CEt4ywxW0AAAAMcGhvbmVfbnVtYmVybQAAAAwrMTg1MDU4NTc2MTdtAAAAB3NlbnRfYXR0AAAADWQACl9fc3RydWN0X19kAA9FbGl4aXIuRGF0ZVRpbWVkAAhjYWxlbmRhcmQAE0VsaXhpci5DYWxlbmRhci5JU09kAANkYXlhDGQABGhvdXJhFWQAC21pY3Jvc2Vjb25kaAJiAArunWEGZAAGbWludXRlYS9kAAVtb250aGEEZAAGc2Vjb25kYQFkAApzdGRfb2Zmc2V0YQBkAAl0aW1lX3pvbmVtAAAAB0V0Yy9VVENkAAp1dGNfb2Zmc2V0YQBkAAR5ZWFyYgAAB+JkAAl6b25lX2FiYnJtAAAAA1VUQ20AAAADdWlkbQAAABDA7gI1UGlXc7EeKAq8pLwg\"}}",
       "\n"]
    ]
  ]
}
  """
  def handle_event({:call, from}, {:add_batch, cmds, join_keys, {_topic, _partition, _last_message} = ack_data}, @empty_state, %Data{} = data) do
    new_data = %Data{data | updates: [cmds | data.updates], ack_data: [ack_data | data.ack_data], join_keys: [join_keys | data.join_keys]}
    |> IO.inspect(label: "a")
    {:next_state, @filling_state, new_data, [{:reply, from, :accepted}]}
  end

  def handle_event(:enter, @empty_state, @filling_state, %Data{} = data) do
    {:next_state, @filling_state, data, [{:state_timeout, 500, @flush_event}]}
  end

  def handle_event(:enter, _any, @empty_state, %Data{}) do
    :keep_state_and_data
  end

  @doc """
  b: %Rivulet.Join.Batcher.Data{
  ack_data: [
    {"platform_nps_responses", 7, 1},
    {"platform_nps_invitations", 2, 0}
  ],
  handler: #PID<0.559.0>,
  join_keys: [
    ["c0ee0235-5069-5773-b11e-280abca4bc20",
     "c0ee0235-5069-5773-b11e-280abca4bc20"],
    ["c0ee0235-5069-5773-b11e-280abca4bc20"]
  ],
  updates: [
    [
      ["{\"update\":{\"_type\":\"join_documents\",\"_index\":\"rivulet-joinkey-nps-join\",\"_id\":\"f0315c54-b272-5e93-86f9-9897c2d860c5\"}}",
       "\n",
       "{\"doc_as_upsert\":true,\"doc\":{\"join_key\":\"c0ee0235-5069-5773-b11e-280abca4bc20\",\"document\":\"g3QAAAAGbQAAAAdjb21tZW50bQAAAABtAAAAC2luc2VydGVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQxkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAK7p1hBmQABm1pbnV0ZWEvZAAFbW9udGhhBGQABnNlY29uZGEBZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAAEGxhc3RfbW9kaWZpZWRfYXR0AAAADWQACl9fc3RydWN0X19kAA9FbGl4aXIuRGF0ZVRpbWVkAAhjYWxlbmRhcmQAE0VsaXhpci5DYWxlbmRhci5JU09kAANkYXlhDGQABGhvdXJhFmQAC21pY3Jvc2Vjb25kaAJiAAxjhGEGZAAGbWludXRlYSJkAAVtb250aGEEZAAGc2Vjb25kYTBkAApzdGRfb2Zmc2V0YQBkAAl0aW1lX3pvbmVtAAAAB0V0Yy9VVENkAAp1dGNfb2Zmc2V0YQBkAAR5ZWFyYgAAB+JkAAl6b25lX2FiYnJtAAAAA1VUQ20AAAAXbGlrZWxpaG9vZF90b19yZWNvbW1lbmRhCm0AAAASbnBzX2ludml0YXRpb25fdWlkbQAAABDA7gI1UGlXc7EeKAq8pLwgbQAAAAN1aWRtAAAAEPAxXFSycl6ThvmYl8LYYMU=\"}}",
       "\n"],
      ["{\"update\":{\"_type\":\"join_documents\",\"_index\":\"rivulet-joinkey-nps-join\",\"_id\":\"f0315c54-b272-5e93-86f9-9897c2d860c5\"}}",
       "\n",
       "{\"doc_as_upsert\":true,\"doc\":{\"join_key\":\"c0ee0235-5069-5773-b11e-280abca4bc20\",\"document\":\"g3QAAAAGbQAAAAdjb21tZW50bQAAAA9Db21tZW50Mkludml0ZTJtAAAAC2luc2VydGVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQxkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAK7p1hBmQABm1pbnV0ZWEvZAAFbW9udGhhBGQABnNlY29uZGEBZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAAEGxhc3RfbW9kaWZpZWRfYXR0AAAADWQACl9fc3RydWN0X19kAA9FbGl4aXIuRGF0ZVRpbWVkAAhjYWxlbmRhcmQAE0VsaXhpci5DYWxlbmRhci5JU09kAANkYXlhDGQABGhvdXJhFmQAC21pY3Jvc2Vjb25kaAJiAASCGWEGZAAGbWludXRlYSJkAAVtb250aGEEZAAGc2Vjb25kYTZkAApzdGRfb2Zmc2V0YQBkAAl0aW1lX3pvbmVtAAAAB0V0Yy9VVENkAAp1dGNfb2Zmc2V0YQBkAAR5ZWFyYgAAB+JkAAl6b25lX2FiYnJtAAAAA1VUQ20AAAAXbGlrZWxpaG9vZF90b19yZWNvbW1lbmRhCm0AAAASbnBzX2ludml0YXRpb25fdWlkbQAAABDA7gI1UGlXc7EeKAq8pLwgbQAAAAN1aWRtAAAAEPAxXFSycl6ThvmYl8LYYMU=\"}}",
       "\n"]
    ],
    [
      ["{\"update\":{\"_type\":\"join_documents\",\"_index\":\"rivulet-joinkey-nps-join\",\"_id\":\"c0ee0235-5069-5773-b11e-280abca4bc20\"}}",
       "\n",
       "{\"doc_as_upsert\":true,\"doc\":{\"join_key\":\"c0ee0235-5069-5773-b11e-280abca4bc20\",\"document\":\"g3QAAAAHbQAAAApjcmVhdGVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQxkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAK7p1hBmQABm1pbnV0ZWEvZAAFbW9udGhhBGQABnNlY29uZGEBZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADWN1c3RvbWVyX25hbWVtAAAADkRhbiBDb25nZXIgVHdvbQAAABBsYXN0X21vZGlmaWVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQxkAARob3VyYRZkAAttaWNyb3NlY29uZGgCYgAK+SFhBmQABm1pbnV0ZWEiZAAFbW9udGhhBGQABnNlY29uZGEQZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADGxvY2F0aW9uX3VpZG0AAAAQX9A7+JzWUgqy45CEt4ywxW0AAAAMcGhvbmVfbnVtYmVybQAAAAwrMTg1MDU4NTc2MTdtAAAAB3NlbnRfYXR0AAAADWQACl9fc3RydWN0X19kAA9FbGl4aXIuRGF0ZVRpbWVkAAhjYWxlbmRhcmQAE0VsaXhpci5DYWxlbmRhci5JU09kAANkYXlhDGQABGhvdXJhFWQAC21pY3Jvc2Vjb25kaAJiAArunWEGZAAGbWludXRlYS9kAAVtb250aGEEZAAGc2Vjb25kYQFkAApzdGRfb2Zmc2V0YQBkAAl0aW1lX3pvbmVtAAAAB0V0Yy9VVENkAAp1dGNfb2Zmc2V0YQBkAAR5ZWFyYgAAB+JkAAl6b25lX2FiYnJtAAAAA1VUQ20AAAADdWlkbQAAABDA7gI1UGlXc7EeKAq8pLwg\"}}",
       "\n"]
    ]
  ]
}
  """
  def handle_event({:call, from}, {:add_batch, cmds, join_keys, {_topic, _partition, _last_message} = ack_data}, @filling_state, %Data{} = data) do
    new_data = %Data{data | updates: [cmds | data.updates], ack_data: [ack_data | data.ack_data], join_keys: [join_keys | data.join_keys]}
    |> IO.inspect(label: "b")

    {:keep_state, new_data, [{:reply, from, :accepted}]}
  end

  @doc """
  resp:

  %{
    "errors" => false,
    "items" => [
      %{
        "update" => %{
          "_id" => "c0ee0235-5069-5773-b11e-280abca4bc20",
          "_index" => "rivulet-joinkey-nps-location-join",
          "_primary_term" => 1,
          "_seq_no" => 6,
          "_shards" => %{"failed" => 0, "successful" => 1, "total" => 2},
          "_type" => "join_documents",
          "_version" => 1,
          "result" => "created",
          "status" => 201
        }
      },
      %{
        "update" => %{
          "_id" => "c0ee0235-5069-5773-b11e-280abca4bc20",
          "_index" => "rivulet-joinkey-nps-location-join",
          "_shards" => %{"failed" => 0, "successful" => 1, "total" => 2},
          "_type" => "join_documents",
          "_version" => 1,
          "result" => "noop",
          "status" => 200
        }
      },
      %{
        "update" => %{
          "_id" => "789686bf-fa4d-569a-9a9f-0fd66496fa48",
          "_index" => "rivulet-joinkey-nps-location-join",
          "_primary_term" => 1,
          "_seq_no" => 7,
          "_shards" => %{"failed" => 0, "successful" => 1, "total" => 2},
          "_type" => "join_documents",
          "_version" => 1,
          "result" => "created",
          "status" => 201
        }
      },
      %{
        "update" => %{
          "_id" => "789686bf-fa4d-569a-9a9f-0fd66496fa48",
          "_index" => "rivulet-joinkey-nps-location-join",
          "_shards" => %{"failed" => 0, "successful" => 1, "total" => 2},
          "_type" => "join_documents",
          "_version" => 1,
          "result" => "noop",
          "status" => 200
        }
      }
    ],
    "took" => 630
  }

  join_keys:
    ["5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5", "5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5",
   "5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5", "5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5"]

  NOTE: is it safe to assume the return value from handler query has appropriate time stamp?
  If not, then we need to pass all of data down I believe

  c: %Rivulet.Join.Batcher.Data{
  ack_data: [
    {"platform_nps_responses", 7, 1},
    {"platform_nps_invitations", 2, 0}
  ],
  handler: #PID<0.559.0>,
  join_keys: [
    ["c0ee0235-5069-5773-b11e-280abca4bc20",
     "c0ee0235-5069-5773-b11e-280abca4bc20"],
    ["c0ee0235-5069-5773-b11e-280abca4bc20"]
  ],
  updates: [
    [
      ["{\"update\":{\"_type\":\"join_documents\",\"_index\":\"rivulet-joinkey-nps-join\",\"_id\":\"f0315c54-b272-5e93-86f9-9897c2d860c5\"}}",
       "\n",
       "{\"doc_as_upsert\":true,\"doc\":{\"join_key\":\"c0ee0235-5069-5773-b11e-280abca4bc20\",\"document\":\"g3QAAAAGbQAAAAdjb21tZW50bQAAAABtAAAAC2luc2VydGVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQxkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAK7p1hBmQABm1pbnV0ZWEvZAAFbW9udGhhBGQABnNlY29uZGEBZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAAEGxhc3RfbW9kaWZpZWRfYXR0AAAADWQACl9fc3RydWN0X19kAA9FbGl4aXIuRGF0ZVRpbWVkAAhjYWxlbmRhcmQAE0VsaXhpci5DYWxlbmRhci5JU09kAANkYXlhDGQABGhvdXJhFmQAC21pY3Jvc2Vjb25kaAJiAAxjhGEGZAAGbWludXRlYSJkAAVtb250aGEEZAAGc2Vjb25kYTBkAApzdGRfb2Zmc2V0YQBkAAl0aW1lX3pvbmVtAAAAB0V0Yy9VVENkAAp1dGNfb2Zmc2V0YQBkAAR5ZWFyYgAAB+JkAAl6b25lX2FiYnJtAAAAA1VUQ20AAAAXbGlrZWxpaG9vZF90b19yZWNvbW1lbmRhCm0AAAASbnBzX2ludml0YXRpb25fdWlkbQAAABDA7gI1UGlXc7EeKAq8pLwgbQAAAAN1aWRtAAAAEPAxXFSycl6ThvmYl8LYYMU=\"}}",
       "\n"],
      ["{\"update\":{\"_type\":\"join_documents\",\"_index\":\"rivulet-joinkey-nps-join\",\"_id\":\"f0315c54-b272-5e93-86f9-9897c2d860c5\"}}",
       "\n",
       "{\"doc_as_upsert\":true,\"doc\":{\"join_key\":\"c0ee0235-5069-5773-b11e-280abca4bc20\",\"document\":\"g3QAAAAGbQAAAAdjb21tZW50bQAAAA9Db21tZW50Mkludml0ZTJtAAAAC2luc2VydGVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQxkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAK7p1hBmQABm1pbnV0ZWEvZAAFbW9udGhhBGQABnNlY29uZGEBZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAAEGxhc3RfbW9kaWZpZWRfYXR0AAAADWQACl9fc3RydWN0X19kAA9FbGl4aXIuRGF0ZVRpbWVkAAhjYWxlbmRhcmQAE0VsaXhpci5DYWxlbmRhci5JU09kAANkYXlhDGQABGhvdXJhFmQAC21pY3Jvc2Vjb25kaAJiAASCGWEGZAAGbWludXRlYSJkAAVtb250aGEEZAAGc2Vjb25kYTZkAApzdGRfb2Zmc2V0YQBkAAl0aW1lX3pvbmVtAAAAB0V0Yy9VVENkAAp1dGNfb2Zmc2V0YQBkAAR5ZWFyYgAAB+JkAAl6b25lX2FiYnJtAAAAA1VUQ20AAAAXbGlrZWxpaG9vZF90b19yZWNvbW1lbmRhCm0AAAASbnBzX2ludml0YXRpb25fdWlkbQAAABDA7gI1UGlXc7EeKAq8pLwgbQAAAAN1aWRtAAAAEPAxXFSycl6ThvmYl8LYYMU=\"}}",
       "\n"]
    ],
    [
      ["{\"update\":{\"_type\":\"join_documents\",\"_index\":\"rivulet-joinkey-nps-join\",\"_id\":\"c0ee0235-5069-5773-b11e-280abca4bc20\"}}",
       "\n",
       "{\"doc_as_upsert\":true,\"doc\":{\"join_key\":\"c0ee0235-5069-5773-b11e-280abca4bc20\",\"document\":\"g3QAAAAHbQAAAApjcmVhdGVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQxkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAK7p1hBmQABm1pbnV0ZWEvZAAFbW9udGhhBGQABnNlY29uZGEBZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADWN1c3RvbWVyX25hbWVtAAAADkRhbiBDb25nZXIgVHdvbQAAABBsYXN0X21vZGlmaWVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQxkAARob3VyYRZkAAttaWNyb3NlY29uZGgCYgAK+SFhBmQABm1pbnV0ZWEiZAAFbW9udGhhBGQABnNlY29uZGEQZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADGxvY2F0aW9uX3VpZG0AAAAQX9A7+JzWUgqy45CEt4ywxW0AAAAMcGhvbmVfbnVtYmVybQAAAAwrMTg1MDU4NTc2MTdtAAAAB3NlbnRfYXR0AAAADWQACl9fc3RydWN0X19kAA9FbGl4aXIuRGF0ZVRpbWVkAAhjYWxlbmRhcmQAE0VsaXhpci5DYWxlbmRhci5JU09kAANkYXlhDGQABGhvdXJhFWQAC21pY3Jvc2Vjb25kaAJiAArunWEGZAAGbWludXRlYS9kAAVtb250aGEEZAAGc2Vjb25kYQFkAApzdGRfb2Zmc2V0YQBkAAl0aW1lX3pvbmVtAAAAB0V0Yy9VVENkAAp1dGNfb2Zmc2V0YQBkAAR5ZWFyYgAAB+JkAAl6b25lX2FiYnJtAAAAA1VUQ20AAAADdWlkbQAAABDA7gI1UGlXc7EeKAq8pLwg\"}}",
       "\n"]
    ]
  ]
}
  """
  def handle_event(:state_timeout, :flush, @filling_state, %Data{} = data) do
    IO.inspect(data, label: "c")
    resp =
      data.updates
      |> Enum.reverse
      |> flush

    # require IEx; IEx.pry
    IO.inspect(resp, label: "resp")

    Enum.each(resp["items"], fn(item) ->
      unless item["update"]["status"] < 300 do
        Logger.error("Elasticsearch updates failed. #{inspect item}")
      end
    end)

    join_keys =
      data.join_keys
      |> Enum.reverse
      |> List.flatten

    IO.inspect(join_keys, label: "join_keys")
    # require IEx; IEx.pry

    Rivulet.Join.Handler.handle_resp(data.handler, join_keys, Enum.reverse(data.ack_data))

    # require IEx; IEx.pry

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
