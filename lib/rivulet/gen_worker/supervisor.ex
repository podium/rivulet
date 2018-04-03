defmodule Rivulet.GenWorker.Supervisor do
  use Supervisor

  # require Logger

  alias Rivulet.GenWorker.State

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts)
  end

  def init(consumer_opts) do
    IO.inspect(consumer_opts, label: "consumer_opts")
    %State{} = config = State.init!(consumer_opts)

    # {:ok, _} = Application.ensure_all_started(:httpoison)

    # NOTE: right now this tries to create an index / mapping regardless of whether one
    # already exists or not. We'll need to change this.
    # Rivulet.ElasticSearchSink.ensure_es_setup!(config)

    # count = 1
    #
    # children =
    #   [
    #     worker(Rivulet.ElasticSearchSink.Writer.Manager, [self(), count], id: :manager),
    #     worker(Rivulet.ElasticSearchSink.Consumer, [config, self()], id: :consumer),
    #     worker(Rivulet.ElasticSearchSink.Writer, [config], id: "writer_1"),
    #   ]
    #
    children = []
    opts = [strategy: :rest_for_one]

    supervise(children, opts)
  end

  # def find_manager(sink) do
  #   find_by_id(sink, :manager)
  # end
  #
  # def find_writer(sink, n) do
  #   find_by_id(sink, "writer_#{n}")
  # end
  #
  # defp find_by_id(children, id) when is_list(children) do
  #   Enum.find_value(children, fn
  #     ({^id, pid, _, _}) -> pid
  #     (_) -> false
  #   end)
  # end

  # defp find_by_id(sup, id) do
  #   sup
  #   |> Supervisor.which_children
  #   |> find_by_id(id)
  # end
end
