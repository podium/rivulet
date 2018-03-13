defmodule Rivulet.SQLSink.Writer do
  require Logger
  alias Rivulet.Kafka.Partition
  alias Rivulet.SQLSink.{Config, Database.Table}

  def start_link(%Config{} = config) do
    GenServer.start_link(__MODULE__, {config})
  end

  def init({config}) do
    {:ok, config}
  end

  def handle_messages(pid, %Partition{} = partition, messages) do
    GenServer.cast(pid, {:handle_messages, partition, messages})
  end

  def handle_cast({:handle_messages, partition, messages}, %Config{} = state) do
    Logger.debug("Handling Messages by dumping to #{table_name(state, partition)}")

    offset = messages |> List.last |> Map.get(:offset)
    Logger.debug("Should get to #{partition.topic}:#{partition.partition} - #{offset}")

    decoded_messages = decoded_messages(messages, partition)
    require IEx
    IEx.pry

    decoded_messages
    |> upserts
    |> do_upsert(state, partition)

    decoded_messages
    |> deletions
    |> do_deletes(state, partition)

    IO.inspect("Acking")
    Rivulet.Consumer.ack(Rivulet.client_name!, partition, offset) |> IO.inspect

    {:noreply, state}
  end

  def do_deletes([], _, _) do
    Logger.debug("No deletions")
    :ok
  end
  def do_deletes(deletions, %Config{} = config, %Partition{} = partition) when is_list(deletions) do
    import Ecto.Query
    delete = from t in table_name(config, partition), where: field(t, ^config.delete_key_field) in ^deletions
    config.repo.delete_all(delete)
  end

  def do_upsert(upserts, %Config{} = config, %Partition{} = partition) do
    config
    |> table_name(partition)
    |> config.repo.insert_all(upserts, on_conflict: :replace_all, conflict_target: List.flatten(config.unique_constraints))
  end

  def table_name(%Config{} = config, %Partition{} = partition) do
    Table.table_name(config.table_pattern, partition.topic)
  end

  def only_latest_per_key(messages) when is_list(messages) do
    messages
    |> Enum.group_by(&(&1.raw_key))
    |> Enum.map(fn({_key, messages}) -> List.last(messages) end)
    |> List.flatten
  end

  def decoded_messages(messages, %Partition{} = partition) when is_list(messages) do
    messages
    |> only_latest_per_key
    |> Rivulet.Avro.bulk_decode(partition.topic)
  end

  def upserts(messages) when is_list(messages) do
    messages
    |> Enum.reject(&(&1.decoded_value == nil))
    |> Enum.map(&(&1.decoded_value))
  end

  def deletions(messages) when is_list(messages) do
    messages
    |> Enum.filter(&((&1.decoded_value |> IO.inspect) == nil))
    |> IO.inspect
    |> Enum.map(&(&1.decoded_key))
  end
end
