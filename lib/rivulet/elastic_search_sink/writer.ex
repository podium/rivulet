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
  alias Rivulet.ElasticSearchSink.{Config, Filterer}
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
  def handle_messages(writer_pid, %Partition{} = partition, messages, consumer_pid) do
    GenServer.cast(writer_pid, {:handle_messages, partition, messages, consumer_pid})
  end

  def handle_cast({:handle_messages, partition, messages, consumer_pid}, %Config{} = state) do
    Logger.debug("Handling Messages by dumping")

    offset = messages |> List.last |> Map.get(:offset)

    Logger.debug("Should get to #{partition.topic}:#{partition.partition} - #{offset}")

    successfully_inserted =
      messages
      |> bulk_index_decoded_messages(state)
      |> Filterer.filter_for_successfully_inserted(messages)
      |> IO.inspect(label: "successfully_inserted")

    Logger.debug("on_complete callback would be fired here")
    # state.callback_module.on_complete(successfully_inserted)

    Rivulet.Consumer.ack(consumer_pid, partition, offset)

    {:noreply, state}
  end

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
    |> handle_es_response()
  end

  def bulk_index(records, %Config{} = state) do
    records = format_bulk_records(state.elastic_index, state.elastic_type, records)
    raw_data = encode_bulk_records(records)

    Bulk.post_raw(state.elastic_url, raw_data, index: state.elastic_index, type: state.elastic_type) |> IO.inspect(label: "call after post_raw")
  end

  def handle_es_response({:ok, %HTTPoison.Response{body: body}}), do: body
  def handle_es_response({:error, %HTTPoison.Response{}}) do
    %{}
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
