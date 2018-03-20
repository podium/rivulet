defmodule Rivulet.ElasticSearchSink.Consumer do
  @moduledoc """
  A Rivulet pipeline for handling incoming objects from the various topics
  """

  @behaviour Rivulet.Consumer

  require Logger

  alias Rivulet.Consumer
  alias Rivulet.Kafka.Partition
  alias FireHydrant.Pool

  def start_link(config = %Rivulet.Consumer.Config{}) do
    Consumer.start_link(__MODULE__, config)
  end

  def consumer_name do
    __MODULE__
  end

  def init(_) do
    Process.register(self(), consumer_name())
    {:ok, {}}
  end

  def config(client_id_base \\ :rivulet_brod_client) do
    process_type = Application.get_env(:fire_hydrant, :process_type)

    if client_id_base != :rivulet_brod_client and process_type == :kafka do
      :ok = :brod.start_client(Rivulet.Application.kafka_brokers, client_id_base, [])
    end

    %Rivulet.Consumer.Config{
      client_id: Rivulet.client_name!,
      consumer_config: [begin_offset: :earliest, max_bytes: 1000000],
      consumer_group_name: Application.get_env(:fire_hydrant, :nps_location_join_consumer_group),
      group_config: [
        offset_commit_policy: :commit_to_kafka_v2,
        offset_commit_interval_seconds: Application.get_env(:fire_hydrant, :commit_interval)
      ],
      topics: ["nps_location_join"]
    }
  end

  def handle_messages(%Partition{} = partition, messages, state) do
    message_count = length(messages)
    Logger.info("#{__MODULE__} received message set with #{message_count} messages for topic: #{partition.topic} and partition: #{partition.partition}")

    Pool.process_messages(messages, partition)

    {:ok, :ack, state}
  end

  def handle_call(_msg, _from, partition) do
    {:reply, {:error, :unknown_message}, partition}
  end
end
