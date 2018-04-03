defmodule Rivulet.GenWorker.Server do
  @moduledoc false
  use GenServer
  # require Logger

  # alias GenWorker.State
  #
  # @spec init(State.t) :: {:ok, State.t}
  def init(state) do
    # Logger.debug("GenWorker: Init worker with state: #{inspect(state)}")
    # schedule_work(state)
    {:ok, state}
  end
end
