defmodule Rivulet.Kafka.Stage.OffsetCommitter do
  use GenStage

  require Logger

  alias Rivulet.Kafka.{Message, Partition}
  alias KafkaEx.Protocol.OffsetCommit.Request, as: OffsetCommitRequest

  defmodule State do
    @enforce_keys [:partition]
    defstruct [:partition, :offset]

    @type t :: %__MODULE__{
      partition: Partition.t,
      offset: non_neg_integer | nil
    }
  end

  @type reason :: term
  @type ignored :: term

  @spec start_link(Partition.t, GenStage.stage | [GenStage.stage]) :: GenServer.on_start
  def start_link(partition, sources \\ [])

  def start_link(%Partition{} = partition, sources) when is_list(sources) do
    GenStage.start_link(__MODULE__, {partition, sources})
  end

  def start_link(%Partition{} = partition, parent) do
    start_link(partition, [parent])
  end

  def init({%Partition{} = partition, []}) do
    state = %State{
      partition: partition,
      offset: nil
    }

    {:producer_consumer, state}
  end

  def init({%Partition{} = partition, sources}) when is_list(sources) do
    state = %State{
      partition: partition,
      offset: nil
    }

    {:producer_consumer, state, subscribe_to: sources}
  end

  @spec handle_events([Message.t], GenServer.from, State.t)
  :: {:noreply, [term], State.t}
  | {:noreply, [term], State.t, :hibernate}
  | {:stop, reason, State.t}
  def handle_events(events, _from, %State{} = state) do
    offset = highest_offset(events, state.offset)

    if offset do
      commit_req =
        %OffsetCommitRequest{
          topic: state.partition.topic,
          partition: state.partition.partition,
          offset: offset,
          consumer_group: Application.get_env(:kafka_ex, :consumer_group)
        }

      Logger.info("Committing offset: #{inspect commit_req}")
      KafkaEx.offset_commit(:kafka_ex, commit_req)

      {:noreply, events, %State{state | offset: offset}}
    else
      {:noreply, events, state}
    end
  end

  defp highest_offset(events, initial_offset) do
    Enum.reduce(events, initial_offset, fn
      (%{offset: offset}, nil) -> offset
      (%{offset: offset}, current_offset) when offset > current_offset -> offset
      (%{offset: _offset}, current_offset) -> current_offset
    end)
  end
end
