defmodule Rivulet.Kafka.Consumer do
  use GenStage

  alias Rivulet.Kafka.{Message, Partition}
  require Logger

  defmodule State do
    @enforce_keys [:partition, :queued]
    defstruct [:partition, :queued, :offset]
    @type t :: %__MODULE__{
      partition: Partition.t,
      queued: [Message.t],
      offset: nil | non_neg_integer
    }
  end

  # Public API

  def start_link(%Partition{} = partition) do
    GenStage.start_link(__MODULE__, {partition, nil})
  end

  def start_link(%Partition{} = partition, offset) when is_integer(offset) do
    GenStage.start_link(__MODULE__, {partition, offset})
  end

  # Callback Functions

  def init({%Partition{} = partition, offset}) do
    Logger.debug("Staring Kafka streamer for: #{partition.topic}:#{partition.partition} from offset: #{offset}")

    state = %State{
      partition: partition,
      queued: [],
      offset: offset
    }

    {:producer, state}
  end

  def handle_demand(demand, %State{} = state) do
    Logger.debug("[#{state.partition.topic}][#{state.partition.partition}] Received #{demand} demand")
    {events, %State{} = state} = pull_data(demand, state)

    {:noreply, events, state}
  end

  def handle_info({:poll, demand}, %State{} = state) do
    Logger.debug("[#{state.partition.topic}][#{state.partition.partition}]Polling for #{demand} more messages from Kafka")
    {events, %State{} = state} = pull_data(demand, state)

    {:noreply, events, state}
  end

  def handle_info(_, %State{} = state) do
    {:noreply, state}
  end

  # Implementation functions

  @spec pull_data(pos_integer, State.t) :: {[Message.t], State.t}
  def pull_data(count, %State{queued: [], offset: offset} = state) do
    Logger.debug("[#{state.partition.topic}][#{state.partition.partition}] Pulling messages from Kafka")

    messages =
      state.partition
      |> fetch(offset)
      |> messages
      |> Enum.map(&Message.from_wire_message/1)
      |> Enum.sort(fn(%Message{} = a, %Message{} = b) ->
           a.offset <= b.offset
         end)

    Logger.debug("[#{state.partition.topic}][#{state.partition.partition}] Found #{length(messages)} messages on Kafka")

    {events, queued} = Enum.split(messages, count)

    delivered_count = length(events)

    if delivered_count < count, do: :timer.send_after(:timer.seconds(1), {:poll, count - delivered_count})


    {events, %State{state | queued: queued, offset: new_offset(offset, messages)}}
  end


  def pull_data(count, %State{queued: queued} = state) do
    Logger.debug("[#{state.partition.topic}][#{state.partition.partition}] Pulling data from kafka consumer buffer")
    {events, queued} = Enum.split(queued, count)

    delivered_count = length(events)

    if delivered_count < count do
      polling_interval = :timer.seconds(1)
      :timer.send_after(polling_interval, {:poll, count - delivered_count})
      Logger.debug("[#{state.partition.topic}][#{state.partition.partition}] Only found #{delivered_count} messages in buffer - will poll for more data in #{polling_interval / 1000} seconds")
    end

    {events, %State{state | queued: queued}}
  end

  def messages([%KafkaEx.Protocol.Fetch.Response{partitions: [%{message_set: messages}]}]) do
    messages
  end

  defp fetch(%Partition{topic: topic, partition: partition}, nil) do
    Logger.debug("Fetching from kafka (no offset given)")
    KafkaEx.fetch(topic, partition)
  end

  defp fetch(%Partition{topic: topic, partition: partition}, offset) do
    Logger.debug("Fetching from kafka (offset: #{offset})")
    KafkaEx.fetch(topic, partition, offset: offset)
  end

  defp new_offset(nil, _messages), do: nil
  defp new_offset(offset, []) do
    offset
  end
  defp new_offset(_offset, messages) do
    highest_offset(messages) + 1
  end

  defp highest_offset(messages) do
    Enum.reduce(messages, 0, fn
      (%Message{offset: offset}, acc) when offset > acc  -> offset
      (_, acc) ->  acc
    end)
  end
end
