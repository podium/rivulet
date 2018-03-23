defmodule Rivulet.ElasticSearchSink.ConsumerTwo do
  alias Rivulet.{
    Consumer,
    Consumer.Config,
    Kafka.Partition,
  }

  alias Rivulet.ElasticSearchSink.Config, as: SinkConfig

  defmodule State do
    @enforce_keys [:pattern, :sink]
    defstruct [:pattern, :manager, :sink]
  end

  @doc """
  NOTE: the sink_supervisor_process was passed in via the self() portion of the
  call within supervisor.ex:

  worker(Rivulet.ElasticSearchSink.ConsumerTwo, [config, self()], id: :consumer)
  """
  def start_link(%SinkConfig{} = config, sink_supervisor_process) do
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

    # NOTE: we'll need to tweak this a bit (spefically the config.table_pattern piece)
    Consumer.start_link(__MODULE__, consumer_config, {config.table_pattern, sink_supervisor_process})
  end

  def set_pool(consumer, pool) do
    GenServer.call(consumer, pool)
  end

  def init({pattern, sink}) do
    {:ok, %State{pattern: pattern, sink: sink}}
  end

  def handle_messages(partition, messages, %State{manager: nil} = state) do
    manager = Rivulet.ElasticSearchSink.Supervisor.find_manager(state.sink)
    state = %State{state | manager: manager}
    handle_messages(partition, messages, state)
  end

  @doc """
  Question: what does state.manager equal here?
  """
  def handle_messages(%Partition{} = partition, messages, %State{} = state) do
    Rivulet.ElasticSearchSink.Writer.Manager.handle_batch(state.manager, partition, messages)
    {:ok, state}
  end
end
