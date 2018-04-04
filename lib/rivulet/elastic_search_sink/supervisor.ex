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
    IO.inspect(caller, label: "caller")
    IO.inspect(otp_app, label: "otp_app")
    IO.inspect(opts, label: "opts")
    case runtime_config(:supervisor, caller, otp_app, opts) |> IO.inspect(label: "runtime") do
      {:ok, consumer_opts} ->
 #        [elastic_url: "http://elasticsearch:9200", otp_app: :rivulet,
 # callback_module: Rivulet.TestDan, elastic_index: "nps", elastic_type: "response",
 # elastic_mapping: %{}, topic: "platform_nps_location_joins"]

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

        # children = [adapter.child_spec(repo, opts)]
        # if Keyword.get(opts, :query_cache_owner, true) do
        #   :ets.new(repo, [:set, :public, :named_table, read_concurrency: true])
        # end
        # supervise(children, strategy: :one_for_one)
      :ignore ->
        :ignore
    end
  end
  # def init({caller, consumer_opts}) do
  #   IO.inspect(caller, label: "caller")
  #   IO.inspect(consumer_opts, label: "consumer_opts")
  #   # NOTE: instead of Config.from_sink_opts() it could be runtime_config() from below
  #   %Config{} = config = Config.from_sink_opts(consumer_opts) |> IO.inspect(label: "sassy")
  #
  #   {:ok, _} = Application.ensure_all_started(:httpoison)
  #
  #   # NOTE: right now this tries to create an index / mapping regardless of whether one
  #   # already exists or not. We'll need to change this.
  #   # Rivulet.ElasticSearchSink.ensure_es_setup!(config)
  #
  #   count = 1
  #
  #   children =
  #     [
  #       worker(Rivulet.ElasticSearchSink.Writer.Manager, [self(), count], id: :manager),
  #       worker(Rivulet.ElasticSearchSink.Consumer, [config, self()], id: :consumer),
  #       worker(Rivulet.ElasticSearchSink.Writer, [config], id: "writer_1"),
  #     ]
  #
  #   opts = [strategy: :rest_for_one]
  #
  #   supervise(children, opts)
  # end

  @doc false
  # def init(opts) do
  #   children = [
  #     supervisor(Task.Supervisor, [[name: job_supervisor()]]),
  #     worker(Cronex.Table, [[scheduler: __MODULE__], [name: table()]])
  #   ]
  #
  #   supervise(children, strategy: :one_for_one)
  # end

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

  # def start_link(repo, otp_app, opts) do
  #   name = Keyword.get(opts, :name, repo)
  #   Supervisor.start_link(__MODULE__, {repo, otp_app, opts}, [name: name])
  # end

  # def init({repo, otp_app, opts}) do
  #   case runtime_config(:supervisor, repo, otp_app, opts) do
  #     {:ok, opts} ->
  #       children = [adapter.child_spec(repo, opts)]
  #       if Keyword.get(opts, :query_cache_owner, true) do
  #         :ets.new(repo, [:set, :public, :named_table, read_concurrency: true])
  #       end
  #       supervise(children, strategy: :one_for_one)
  #     :ignore ->
  #       :ignore
  #   end
  # end

  @doc """
  Retrieves the runtime configuration.
  type: :supervisor
  repo: Rivulet.TestDan
  otp_ap: :rivulet
  """
  def runtime_config(type, repo, otp_app, _custom) do
    if config = Application.get_env(otp_app, repo) do
      config = [otp_app: otp_app] ++ config

      case repo_init(type, repo, config) do
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

  defp repo_init(type, repo, config) do
    if Code.ensure_loaded?(repo) and function_exported?(repo, :init, 2) do
      repo.init(type, config)
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
    config = [callback_module: caller] ++ config

    {otp_app, config}
  end
end
