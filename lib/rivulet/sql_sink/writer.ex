defmodule Rivulet.SQLSink.Writer do
  @moduledoc """
  This module is an abstraction that represents the functionality
  each separate Writer process (i.e., each process that will do some database
  operations within BlackMamba, etc.) will need to successfully write to
  a Postgres database.
  """
  require Logger

  alias Rivulet.JSON
  alias Rivulet.Kafka.Consumer.Message
  alias Rivulet.Kafka.Partition
  alias Rivulet.SQLSink.Database.Table
  alias Rivulet.SQLSink.Config, as: SQLSinkConfig

  def start_link(%SQLSinkConfig{} = config) do
    GenServer.start_link(__MODULE__, {config})
  end

  @spec init({SQLSinkConfig.t()}) :: {:ok, SQLSinkConfig.t()}
  def init({config}) do
    {:ok, config}
  end

  def handle_messages(pid, %Partition{} = partition, messages, consumer) do
    GenServer.cast(pid, {:handle_messages, partition, messages, consumer})
  end

  def handle_cast({:handle_messages, partition, messages, consumer}, %SQLSinkConfig{} = config) do
    Logger.debug("Handling Messages by dumping to #{table_name(config, partition)}")

    offset = messages |> List.last |> Map.get(:offset)
    Logger.debug("Should get to #{partition.topic}:#{partition.partition} - #{offset}")

    decoded_messages = decoded_messages(messages, partition, config.decoding_strategy)

    config.repo.transaction(fn ->
      decoded_messages
      |> upserts(config)
      |> do_upsert(config, partition)

      decoded_messages
      |> deletions
      |> do_deletes(config, partition)
    end)

    Rivulet.Consumer.ack(consumer, partition, offset)

    {:noreply, config}
  end

  def do_deletes([], _, _) do
    Logger.debug("No deletions")
    :ok
  end
  def do_deletes(deletions, %SQLSinkConfig{} = config, %Partition{} = partition) when is_list(deletions) do
    import Ecto.Query
    delete = from t in table_name(config, partition), where: field(t, ^config.delete_key_field) in ^deletions
    config.repo.delete_all(delete)
  end

  def do_upsert(upserts, %SQLSinkConfig{} = config, %Partition{} = partition) do
    config
    |> table_name(partition)
    |> config.repo.insert_all(upserts, on_conflict: {:replace, config.whitelist}, conflict_target: List.flatten(config.unique_constraints))
  end

  @spec table_name(SQLSinkConfig.t(), Partition.t()) :: String.t()
  def table_name(%SQLSinkConfig{} = config, %Partition{} = partition) do
    Table.table_name(config.table_pattern, partition.topic)
  end

  def only_latest_per_key(messages) when is_list(messages) do
    messages
    |> Enum.group_by(&(&1.raw_key))
    |> Enum.map(fn({_key, messages}) -> List.last(messages) end)
    |> List.flatten
  end

  def decoded_messages(messages, %Partition{} = partition, :json) when is_list(messages) do
    messages
    |> only_latest_per_key
    |> Enum.map(fn (%Message{raw_key: raw_key}) = msg ->
      case JSON.decode(raw_key) do
        {:ok, decoded_key} ->
          %Message{msg | decoded_key: decoded_key, raw_key: nil}
        {:error, reason} ->
          Logger.error("[TOPIC: #{partition.topic}][OFFSET: #{msg.offset}] failed to decode for reason: #{inspect reason}")
          %Message{msg | decoded_key: {:error, :json_decoding_failed, msg}}
      end
    end)
    |> Enum.filter(&(&1))
    |> Enum.map(fn (%Message{raw_value: raw_value}) = msg ->
      case JSON.decode(raw_value) do
        {:ok, decoded_value} ->
          %Message{msg | decoded_value: decoded_value, raw_value: nil}
        {:error, reason} ->
          Logger.error("[TOPIC: #{partition.topic}][OFFSET: #{msg.offset}] failed to decode for reason: #{inspect reason}")
          %Message{msg | decoded_value: {:error, :json_decoding_failed, msg}}
      end
    end)
    |> Enum.filter(&(&1))
  end

  def decoded_messages(messages, %Partition{} = partition, :avro) when is_list(messages) do
    messages
    |> only_latest_per_key
    |> Rivulet.Avro.bulk_decode(partition.topic)
  end

  def upserts(messages, %SQLSinkConfig{} = config) when is_list(messages) do
    messages =
      messages
      |> Enum.reject(&(&1.decoded_value == nil))
      |> Enum.map(&(&1.decoded_value))

    case config.whitelist do
      :all -> messages
      fields when is_list(fields) -> Enum.map(messages, &(Map.take(&1, fields)))
    end
  end

  def deletions(messages) when is_list(messages) do
    messages
    |> Enum.filter(&((&1.decoded_value) == nil))
    |> Enum.map(&(&1.decoded_key))
  end
end
