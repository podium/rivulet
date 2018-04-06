defmodule Rivulet.ElasticSearchSink.Supervisor do
  @moduledoc false
  use Supervisor

  alias Rivulet.ElasticSearchSink.Config

  @defaults []

  @doc """
  Starts the repo supervisor.
  """
  def start_link(caller, otp_app, opts) do
    Supervisor.start_link(__MODULE__, {caller, otp_app, opts}, [])
  end

  def init({caller, otp_app, opts}) do
    case runtime_config(:supervisor, caller, otp_app, opts) do
      {:ok, consumer_opts} ->
        %Config{} = config = Config.from_sink_opts(consumer_opts)

        {:ok, _} = Application.ensure_all_started(:httpoison)

        count = 1

        children =
          [
            worker(Rivulet.ElasticSearchSink.Writer.Manager, [self(), count], id: :manager),
            worker(Rivulet.ElasticSearchSink.Consumer, [config, self()], id: :consumer),
            worker(Rivulet.ElasticSearchSink.Writer, [config], id: "writer_1"),
          ]

        opts = [strategy: :rest_for_one]

        supervise(children, opts)
      :ignore ->
        :ignore
    end
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

  @doc """
  Retrieves the runtime configuration.
  type: :supervisor
  repo: Rivulet.TestDan
  otp_app: :rivulet
  """
  def runtime_config(type, repo, otp_app, _custom) do
    if config = Application.get_env(otp_app, repo) do
      config = [otp_app: otp_app, callback_module: repo] ++ config

      case sink_module_init(type, repo, config) do
        {:ok, config} ->
          {:ok, config}
        :ignore ->
          :ignore
      end
    else
      raise ArgumentError,
        "configuration for #{inspect repo} not specified in #{inspect otp_app} environment"
    end
  end

  defp sink_module_init(type, sink_module, config) do
    if Code.ensure_loaded?(sink_module) and function_exported?(sink_module, :init, 2) do
      sink_module.init(type, config)
    else
      {:ok, config}
    end
  end

  @doc """
  Retrieves the compile time configuration.
  """
  def compile_config(caller, opts) do
    otp_app = Keyword.fetch!(opts, :otp_app)
    config  = Application.get_env(otp_app, caller, [])

    {otp_app, config}
  end
end
