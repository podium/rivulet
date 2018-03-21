defmodule Rivulet.ElasticSearchSink.Supervisor do
  use Supervisor

  require Logger

  alias Rivulet.ElasticSearchSink.Config

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts)
  end

  def init(consumer_opts) do
    %Config{} = config = Config.from_sink_opts(consumer_opts)

    {:ok, _} = Application.ensure_all_started(:httpoison)

    Rivulet.ElasticSearchSink.ensure_es_setup!(config)

    children =
      [
        # worker(Rivulet.ElasticSearchSink.Writer.Manager, [self(), count], id: :manager),
        worker(Rivulet.ElasticSearchSink.Consumer, [config, self()], id: :consumer),
        # worker(Rivulet.ElasticSearchSink.Writer, [config], id: "writer_1"),
      ]

    opts = [strategy: :rest_for_one]

    supervise(children, opts)
  end
end
