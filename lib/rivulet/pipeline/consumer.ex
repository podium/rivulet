defmodule Rivulet.Pipeline.Consumer do
  use GenStage

  @spec start_link() :: GenServer.on_start
  def start_link() do
    GenStage.start_link(__MODULE__, [])
  end

  @spec start_link(GenStage.stage | [GenStage.stage]) :: GenServer.on_start
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

  def init({sources}) do
    {:consumer, {}, subscribe_to: sources}
  end

  def init(_) do
    {:consumer, {}}
  end

  def handle_events(_events, _from, state) do
    {:noreply, [], state}
  end
end
