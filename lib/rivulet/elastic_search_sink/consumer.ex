defmodule Rivulet.ElasticSearchSink.Consumer do
  alias Rivulet.{
    Config,
    Consumer,
    Consumer.Config,
    Kafka.Partition,
  }

  alias Rivulet.ElasticSearchSink.Config, as: SinkConfig

  defmodule State do
    @enforce_keys [:sink_supervisor_pid]
    defstruct [:manager_pid, :sink_supervisor_pid]
  end

  @doc """
  NOTE: the sink_supervisor_process was passed in via the self() portion of the
  call within supervisor.ex:

  worker(Rivulet.ElasticSearchSink.ConsumerTwo, [config, self()], id: :consumer)
  """
  def start_link(%SinkConfig{} = config, sink_supervisor_process) do
    consumer_config =
      %Config{
        client_id: Rivulet.client_name!(),
        consumer_group_name: config.consumer_group,
        topics: [config.topic],
        group_config: [
          offset_commit_policy: :commit_to_kafka_v2,
          offset_commit_interval_seconds: 1,
        ],
        consumer_config: [begin_offset: :earliest, max_bytes: Rivulet.Config.max_bytes]
      }

    Consumer.start_link(__MODULE__, consumer_config, {:sink_supervisor_process, sink_supervisor_process})
  end

  @doc """
  pid is passed in via line 35 above
  """
  def init({_, sink}) do
    {:ok, %State{sink_supervisor_pid: sink}}
  end

  def handle_messages(partition, messages, %State{manager_pid: nil} = state) do
    manager_pid = Rivulet.ElasticSearchSink.Supervisor.find_manager(state.sink_supervisor_pid)
    state = %State{state | manager_pid: manager_pid}
    handle_messages(partition, messages, state)
  end

  def handle_messages(%Partition{} = partition, messages, %State{} = state) do
    Rivulet.ElasticSearchSink.Writer.Manager.handle_batch(state.manager_pid, partition, messages, self())
    {:ok, state}
  end
end
