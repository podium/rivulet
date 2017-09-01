defmodule Rivulet.Stage.EventBatcher do
  use GenStage
  @moduledoc """
  A Stage which accumulates events until one of two triggers causes it to flush:

  1. A configurable event count threshold is reached
  1. A configurable timeout threshold is reached

  For the timeout threshold, the timer is started upon receipt of the first
  event. Receiving more events does not restart the timer. The timer does reset
  when events are flushed.

  The timeout is specified in milliseconds.

  Examples:

  Creates a batcher which will accumulate up to 2_000 events. If 2_000 events
  are not received from the parents within 500 milliseconds of receiving the
  first event, it will flush the events to the next stage in the pipeline.

  ```elixir
  parents = [...] # List of GenStage.t
  config = %{max: 2_000, timeout: 500}

  Rivulet.Stage.EventBatcher.start_link(config, parents)
  ```

  Creates a batcher which will accumulate up to 100_000 events. If 100_000
  events are not received from the parents within 500 milliseconds of receiving
  the first event, it will flush the events to the next stage in the pipeline.

  ```elixir
  parent = [...] # List of GenStage.t
  config = %{max: 100_000, timeout: :timer.seconds(5)}

  Rivulet.Stage.EventBatcher.start_link(config, parent)
  ```
  """
  alias Rivulet.Pipeline.Batcher

  defmodule State do
    defstruct [:batcher]
  end

  @type config :: %{
      max: non_neg_integer,
      timeout: pos_integer
    }

  @spec start_link(config) :: GenServer.on_start
  def start_link(%{} = config) do
    GenStage.start_link(__MODULE__, {config})
  end

  @spec start_link(config, GenStage.stage | [GenStage.stage]) :: GenServer.on_start
  def start_link(%{} = config, sources) when is_list(sources) do
    GenStage.start_link(__MODULE__, {config, sources})
  end

  def start_link(%{} = config, parent) do
    start_link(config, [parent])
  end

  def init({%{} = config}) do
    {:ok, batcher} = Batcher.start_link(config.max, config.timeout)

    state = %State{batcher: batcher}

    {:producer_consumer, state}
  end

  def init({%{} = config, sources}) when is_list(sources) do
    {:ok, batcher} = Batcher.start_link(config.max, config.timeout)

    state = %State{batcher: batcher}

    {:producer_consumer, state, subscribe_to: sources}
  end

  def init(_) do
    state = %State{}

    {:producer_consumer, state}
  end

  def handle_events(events, _from, %State{batcher: batcher} = state) do
    case Batcher.events(batcher, events) do
      :continue -> {:noreply, [], state}
      {:events, events} -> {:noreply, events, state}
    end
  end

  def handle_info({:events, events}, %State{} = state) do
    {:noreply, events, state}
  end

  def handle_info(_, state) do
    {:noreply, [], state}
  end
end
