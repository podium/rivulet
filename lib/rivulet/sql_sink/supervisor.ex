defmodule Rivulet.SQLSink.Supervisor do
  use Supervisor

  require Logger

  alias Rivulet.SQLSink.Config

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts)
  end

  def init(consumer_opts) do
    %Config{} = config = Config.from_sink_opts(consumer_opts)

    Rivulet.SQLSink.ensure_table_created!(config)

    count = pool_size(config.repo)

    children =
      [
        worker(Rivulet.SQLSink.Writer.Manager, [self(), count], id: :manager),
        worker(Rivulet.SQLSink.Consumer, [config, self()], id: :consumer),
      ]

    children =
      Enum.reduce(1..count, children, fn(n, acc) ->
        [worker(Rivulet.SQLSink.Writer, [config], id: "writer_#{n}") | acc]
      end)


    opts = [strategy: :rest_for_one]

    supervise(children, opts)
  end

  def find_manager(sink) do
    find_by_id(sink, :manager)
  end

  def find_writer(sink, n) do
    find_by_id(sink, "writer_#{n}")
  end

  defp find_by_id(children, id) when is_list(children) do
    Enum.find_value(children, fn
      ({^id, pid, _, _}) -> pid
      (_) -> false
    end)
  end

  defp find_by_id(sup, id) do
    sup
    |> Supervisor.which_children
    |> find_by_id(id)
  end

  def pool_size(repo) do
    repo.config |> Keyword.get(:otp_app) |> Application.get_env(repo) |> Keyword.get(:pool_size) || 2
  end
end
