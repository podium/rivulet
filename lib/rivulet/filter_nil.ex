defmodule Rivulet.FilterNil do
  use GenStage
  require Logger

  alias Rivulet.Kafka.Message

  defmodule State do
    defstruct []
  end

  @spec start_link()
 :: GenServer.on_start
  def start_link() do
    GenStage.start_link(__MODULE__, {})
  end

  @spec start_link(GenStage.stage | [GenStage.stage])
  :: GenServer.on_start
  def start_link({:global, _} = parent) do
    start_link([parent])
  end

  def start_link({:via, _, _} = parent) do
    start_link([parent])
  end

  def start_link({atom, node} = parent) when is_atom(atom) and is_atom(node) do
    start_link([parent])
  end

  def start_link(parent) when is_pid(parent) or is_atom(parent) do
    start_link([parent])
  end

  def start_link(parents) when is_list(parents) do
    GenStage.start_link(__MODULE__, {parents})
  end

  def init({[]}) do
    {:producer_consumer, %State{}}
  end

  def init({parents}) do
    {:producer_consumer, %State{}, subscribe_to: parents}
  end

  def handle_events(events, _from, %State{} = state) do
    events =
      Enum.reject(events, fn
        (%Message{raw_value: nil}) -> true
        (%Message{}) -> false
      end)

    {:noreply, events, state}
  end
end
