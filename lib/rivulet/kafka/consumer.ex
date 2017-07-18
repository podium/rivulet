defmodule Rivulet.Kafka.Consumer do
  use GenStage

  defmodule State do
    defstruct [:topic, :partition, :queued]
    @type t :: %__MODULE__{}
  end

  @type event :: term

  # Public API

  def start_link(topic, partition)
  when is_binary(topic)
  and topic != ""
  and is_integer(partition) do
    GenStage.start_link(__MODULE__, {topic, partition})
  end

  def start_link(topic, partition, offset)
  when is_binary(topic) and topic != ""
  and is_integer(partition) do
    GenStage.start_link(__MODULE__, {topic, partition, offset})
  end

  def stream(pid) do
    Stream.resource(
      fn -> pid end,
      fn pid ->
        resp = GenServer.call(pid, {:pull, 1})
        {resp, pid}
      end,
      fn _pid -> :ok end
    )
  end

  # Callback Functions

  def init({topic, partition})
  when is_binary(topic)
  and topic != ""
  and is_integer(partition) do
    state = %State{
      topic: topic,
      partition: partition,
      queued: []
    }

    {:producer, state}
  end

  def init({topic, partition, offset})
  when is_binary(topic)
  and topic != ""
  and is_integer(partition)
  and is_integer(offset) do
    queued =
      topic
      |> fetch(partition, offset)
      |> messages

    state = %State{
      topic: topic,
      partition: partition,
      queued: queued
    }

    {:producer, state}
  end

  def handle_call({:pull, count}, _from, %State{} = state) do
    {events, %State{} = state} = pull_data(count, state)

    {:reply, events, events, state}
  end

  def handle_demand(demand, %State{} = state) do
    {events, %State{} = state} = pull_data(demand, state)

    {:noreply, events, state}
  end

  # Implementation functions

  @spec pull_data(pos_integer, State.t) :: {[event], State.t}
  def pull_data(count, %State{queued: []} = state) do
    messages =
      state.topic
      |> fetch(state.partition, nil)
      |> messages

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

  defp fetch(topic, partition, nil) do
    KafkaEx.fetch(topic, partition)
  end

  defp fetch(topic, partition, offset) do
    KafkaEx.fetch(topic, partition, offset: offset)
  end
end
