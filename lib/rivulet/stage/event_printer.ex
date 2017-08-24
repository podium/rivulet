defmodule Rivulet.Stage.EventPrinter do
  use GenStage

  require Logger

  defmodule State do
    defstruct [count: 0]
  end

  @spec start_link() :: GenServer.on_start
  def start_link do
    GenStage.start_link(__MODULE__, [])
  end

  @spec start_link(GenStage.stage | [GenStage.stage]) :: GenServer.on_start
  def start_link(sources) when is_list(sources) do
    GenStage.start_link(__MODULE__, sources)
  end

  def start_link(parent) do
    start_link([parent])
  end

  def init([]) do
    state = %State{}

    {:producer_consumer, state}
  end

  def init(sources) when is_list(sources) do
    state = %State{}

    {:producer_consumer, state, subscribe_to: sources}
  end

  def init(_) do
    state = %State{}

    {:producer_consumer, state}
  end

  def handle_events(events, _from, %State{count: count = 100}) do
    Logger.debug("Batch of #{inspect count} completed")
    Enum.map(events, &IO.inspect/1)

    {:noreply, events, %State{count: 0}}
  end

  def handle_events(events, _from, %State{count: count} = state) do
    Enum.map(events, &IO.inspect/1)

    {:noreply, events, %State{state | count: count + 1}}
  end
end
