defmodule Rivulet.EventPrinter do
  use GenStage

  defmodule State do
    defstruct [count: 0]
  end

  def start_link() do
    GenStage.start_link(__MODULE__, [])
  end

  def init(_) do
    state = %State{}

    {:consumer, state}
  end

  def handle_events(events, _from, %State{count: 100}) do
    IO.inspect("Batch of 100 completed")
    Enum.map(events, &IO.inspect/1)

    {:noreply, [], %State{count: 0}}
  end

  def handle_events(events, _from, %State{count: count} = state) do
    Enum.map(events, &IO.inspect/1)

    {:noreply, [], %State{state | count: count + 1}}
  end
end
