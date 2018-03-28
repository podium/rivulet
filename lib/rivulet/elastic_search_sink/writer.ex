defmodule Rivulet.ElasticSearchSink.Writer do
  @moduledoc """
  This module is an abstraction that represents the functionality
  each separate Writer process (i.e., each process that will do some database
  operations within BlackMamba, etc.) will need to successfully write to
  a Postgres database.
  """
  require Logger
  alias Rivulet.JSON
  alias Rivulet.Kafka.Partition
  alias Rivulet.Kafka.Consumer.Message
  alias Rivulet.ElasticSearchSink.{Config}
  alias Elastix.Bulk

  @type index_name :: String.t
  @type type_name :: String.t
  @type mapping :: %{properties: %{}}
  @type exception :: any()
  @type action_type :: String.t
  @type record :: %{}
  @type index_record :: []
  @type upserted_record :: []
  @type query :: %{}

  def start_link(%Config{} = config) do
    GenServer.start_link(__MODULE__, {config})
  end

  def init({config}) do
    {:ok, config}
  end

  @doc """
  The pid here is the identifier for this particular Writer process
  """
  def handle_messages(pid, %Partition{} = partition, messages) do
    GenServer.cast(pid, {:handle_messages, partition, messages})
  end

  def handle_cast({:handle_messages, partition, messages}, %Config{} = state) do
    Logger.debug("Handling Messages by dumping")

    offset = messages |> List.last |> Map.get(:offset)
    Logger.debug("Should get to #{partition.topic}:#{partition.partition} - #{offset}")

    # decoded_messages = decoded_messages(messages, state)
    dan = bulk_index_decoded_messages(messages, state)
      |> IO.inspect(label: "dan")

    # decoded_messages
    # |> upserts
    # |> do_upsert(state, partition)
    #
    # decoded_messages
    # |> deletions
    # |> do_deletes(state, partition)
    #
    # Rivulet.Consumer.ack(Rivulet.client_name!, partition, offset)

    {:noreply, state}
  end

#   [
#   %Rivulet.Kafka.Consumer.Message{
#     attributes: 0,
#     crc: 3330001812,
#     decoded_key: nil,
#     decoded_value: nil,
#     key_schema: nil,
#     offset: 0,
#     raw_key: "789686bf-fa4d-569a-9a9f-0fd66496fa48",
#     raw_value: "{\"response_received_at\":\"2018-03-28T02:53:37.035923Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":10,\"nps_response_uid\":\"63ea1ac2-28d4-50ed-8396-055f9326b380\",\"nps_invitation_uid\":\"789686bf-fa4d-569a-9a9f-0fd66496fa48\",\"nps_comment\":\"Dan\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-03-28T02:53:37.035923Z\",\"customer_phone\":\"+18505857616\",\"customer_name\":\"Dan\",\"created_at\":\"2018-03-28T02:53:37.035923Z\",\"adjusted_score\":100}",
#     value_schema: nil
#   }
# ]

  def only_latest_per_key(messages) when is_list(messages) do
    messages
    |> Enum.group_by(&(&1.raw_key))
    |> Enum.map(fn({_key, messages}) -> List.last(messages) end)
    |> List.flatten
  end

  def bulk_index_decoded_messages(messages, state) when is_list(messages) do
    messages
    |> only_latest_per_key
    |> bulk_index(state)
  end

  def bulk_index(records, %Config{} = state) do
    records = format_bulk_records(state.elastic_index, state.elastic_type, records)
    raw_data = encode_bulk_records(records)

    Bulk.post_raw(state.elastic_url, raw_data, index: state.elastic_index, type: state.elastic_type)
  end

  defp encode_bulk_records(lines) do
    Enum.map(lines, &encode_single_record/1)
  end

  defp encode_single_record(line) do
    with {:ok, encoded} <- JSON.encode(line),
     do: encoded <> "\n"
  end

  @spec format_bulk_records(index_name, type_name, [record]) :: [index_record]
  defp format_bulk_records(index, type, records) do
    records
    |> List.wrap()
    |> Enum.flat_map(&update_line(index, type, &1))
  end

  @spec update_line(index_name, type_name, any) :: upserted_record
  defp update_line(index, type, %Message{} = message) do
    document_id = message.raw_key
    {:ok, record} = JSON.decode(message.raw_value)

    formatted_record = format_for_upsert(record)

    [
      %{"update" => %{
          "_index" => index,
          "_type" => type,
          "_id" => document_id
          }
      },
      formatted_record
    ]
  end

  defp format_for_upsert(record) do
    %{"doc" => record, "doc_as_upsert" => true}
  end
end
