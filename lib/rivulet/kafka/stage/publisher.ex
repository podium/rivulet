defmodule Rivulet.Kafka.Stage.Publisher do
  use GenStage

  require Logger

  alias Rivulet.Kafka.Publisher
  alias Rivulet.Kafka.Publish.Message

  @type ignored :: term
  @type reason :: term

  defmodule State do
    defstruct []

    @type t :: %__MODULE__{}
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

  @spec handle_events([Message.t], GenServer.from, State.t)
  :: {:noreply, [term], State.t}
  | {:noreply, [term], State.t, :hibernate}
  | {:stop, reason, State.t}
  def handle_events(events, _from, %State{} = state) do
    Publisher.publish(events)

    {:noreply, events, state}
  end
end
