defmodule Rivulet.EventPrinter do
  use GenStage

  defmodule State do
    defstruct [count: 0]
  end

  def start_link(topic, partition) do
    GenStage.start_link(__MODULE__, {topic, partition})
  end

  def init({topic, partition}) do
    state = %State{}

    {:consumer, state, subscribe_to: [{{:via, Registry, {Rivulet.Registry, "Kafka.#{topic}.#{inspect partition}"}}, max_demand: 10}]}
  end

  def handle_events(events, _from, %State{count: 100}) do
    IO.inspect("Batch of 100 completed")
    Enum.map(events, &IO.inspect/1)

    {:noreply, [], %State{count: 0}}
  end

  def handle_events(_events, _from, %State{count: count} = state) do
    #Enum.map(events, &IO.inspect/1)

    {:noreply, [], %State{state | count: count + 1}}
  end
end
