defmodule Rivulet.SQLSink.Consumer do
  alias Rivulet.{
    Consumer,
    Consumer.Config,
    Kafka.Partition,
  }

  alias Rivulet.SQLSink.Config, as: SinkConfig

  defmodule State do
    @enforce_keys [:pattern, :sink]
    defstruct [:pattern, :manager, :sink]
  end

  def start_link(%SinkConfig{} = config, sink) do
    consumer_config =
      %Config{
        client_id: Rivulet.client_name(),
        consumer_group_name: config.consumer_group,
        topics: [config.topic],
        group_config: [
          offset_commit_policy: :commit_to_kafka_v2,
          offset_commit_interval_seconds: 1,
        ],
        consumer_config: [begin_offset: :earliest, max_bytes: 2_000_000]
      }

    Consumer.start_link(__MODULE__, consumer_config, {config.table_pattern, sink})
  end

  def set_pool(consumer, pool) do
    GenServer.call(consumer, pool)
  end

  def init({pattern, sink}) do
    {:ok, %State{pattern: pattern, sink: sink}}
  end

  def handle_messages(partition, messages, %State{manager: nil} = state) do
    manager = Rivulet.SQLSink.Supervisor.find_manager(state.sink)
    state = %State{state | manager: manager}
    handle_messages(partition, messages, state)
  end

  @doc """
  Question: what does state.manager equal here?
  """
  def handle_messages(%Partition{} = partition, messages, %State{} = state) do
    # below line is similar to Pool.ex
    Rivulet.SQLSink.Writer.Manager.handle_batch(state.manager, partition, messages)
    {:ok, state}
  end
end
