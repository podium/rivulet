defmodule Rivulet.Kafka.Consumer do
  use GenStage

  alias Rivulet.Kafka.{Message, Partition}

  defmodule State do
    defstruct [:partition, :queued]
    @type t :: %__MODULE__{
      partition: Partition.t,
      queued: [Message.t]
    }
  end

  # Public API

  def start_link(%Partition{} = partition) do
    GenStage.start_link(__MODULE__, {partition})
  end

  def start_link(%Partition{} = partition, offset) when is_integer(offset) do
    GenStage.start_link(__MODULE__, {partition, offset})
  end

  # Callback Functions

  def init({%Partition{} = partition}) do
    state = %State{
      partition: partition,
      queued: []
    }

    {:producer, state}
  end

  def init({%Partition{} = partition, offset}) when is_integer(offset) do
    queued =
      partition
      |> fetch(offset)
      |> messages
      |> Enum.map(&Message.from_wire_message/1)
      |> Enum.sort(fn(%Message{} = a, %Message{} = b) ->
           a.offset >= b.offset
         end)

    state = %State{
      partition: partition,
      queued: queued
    }

    {:producer, state}
  end

  def handle_demand(demand, %State{} = state) do
    {events, %State{} = state} = pull_data(demand, state)

    {:noreply, events, state}
  end

  # Implementation functions

  @spec pull_data(pos_integer, State.t) :: {[event], State.t}
  def pull_data(count, %State{queued: []} = state) do
    messages =
      state.partition
      |> fetch(nil)
      |> messages
      |> Enum.map(&Message.from_wire_message/1)
      |> Enum.sort(fn(%Message{} = a, %Message{} = b) ->
           a.offset >= b.offset
         end)

    {events, queued} = Enum.split(messages, count)

    {events, %State{state | queued: queued}}
  end


  def pull_data(count, %State{queued: queued} = state) do
    {events, queued} = Enum.split(queued, count)

    {events, %State{state | queued: queued}}
  end

  def messages([%KafkaEx.Protocol.Fetch.Response{partitions: [%{message_set: messages}]}]) do
    messages
  end

  defp fetch(%Partition{topic: topic, partition: partition}, nil) do
    KafkaEx.fetch(topic, partition)
  end

  defp fetch(%Partition{topic: topic, partition: partition}, offset) do
    KafkaEx.fetch(topic, partition, offset: offset)
  end
end
