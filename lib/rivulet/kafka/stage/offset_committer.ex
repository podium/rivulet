defmodule Rivulet.Kafka.Stage.OffsetCommitter do
  @moduledoc """
  A stage for committing offsets to kafka.

  When dealing with job validation, there are 3 different meanings of "done".

  1. You've received the job, and the messaging system doesn't need to worry
    about it anymore. This implies that the application is confident in its
    error-handling ability.
  1. You've received the job, and it has been completely processed.
  1. You've received the job, completely processed it, and all previous jobs
     have also been processed completely.

  If using offset tracking, Kafka pretty much forces you to do #3. This offset
  committer assumes that's what you are doing.

  Whenever a batch of events comes through, this stage looks for the
  highest-numbered offset and commits that one. It will track what the highest
  offset it has seen is, and will only commit if the batch includes a number
  which is higher than the current state.

  This means that the developer is responsible for ensuring that all messages
  with a lower offset have been completely processed before sending down a
  batch.
  """
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

  @typedoc "The second element of an error tuple. `{:error, reason}`"
  @type reason :: term

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

  @doc false
  def highest_offset(events, initial_offset) do
    Enum.reduce(events, initial_offset, fn
      (%{offset: offset}, nil) -> offset
      (%{offset: offset}, current_offset) when offset > current_offset -> offset
      (%{offset: _offset}, current_offset) -> current_offset
    end)
  end
end
