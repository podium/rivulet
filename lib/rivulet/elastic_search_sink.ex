defmodule Rivulet.ElasticSearchSink do
  @type t :: module
  @type es_dumped_messages :: [%{}]

  @doc false
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @behaviour Rivulet.ElasticSearchSink

      {otp_app, config} = Rivulet.ElasticSearchSink.Supervisor.compile_config(__MODULE__, opts)

      @otp_app otp_app
      @config  config

      def config do
        {:ok, config} = Rivulet.ElasticSearchSink.Supervisor.runtime_config(:dry_run, __MODULE__, @otp_app, [])
        config
      end

      def child_spec(opts) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [opts]},
          type: :supervisor
        }
      end

      def start_link(opts \\ []) do
        Rivulet.ElasticSearchSink.Supervisor.start_link(__MODULE__, @otp_app, opts)
      end

      def stop(pid, timeout \\ 5000) do
        Supervisor.stop(pid, :normal, timeout)
      end

      @doc false
      def on_complete(_params) do
        raise "Behaviour function #{__MODULE__}.on_complete/1 is not implemented!"
      end

      defoverridable [on_complete: 1, child_spec: 1]
    end
  end

  @optional_callbacks init: 2

  @doc """
  Returns the sink configuration stored in the `:otp_app` environment.

  If the `c:init/2` callback is implemented in the calling module,
  it will be invoked with the first argument set to `:dry_run`.
  """
  @callback config() :: Keyword.t

  @doc """
  Starts any supervision and return `{:ok, pid}`
  or just `:ok` if nothing needs to be done.

  Returns `{:error, {:already_started, pid}}` if the supervisor is already
  started or `{:error, term}` in case anything else goes wrong.
  """
  @callback start_link(opts :: Keyword.t) :: {:ok, pid} |
                            {:error, {:already_started, pid}} |
                            {:error, term}

  @doc """
  A callback executed when the repo starts or when configuration is read.

  The first argument is the context the callback is being invoked.
  The second argument is the configuration as stored in the
  application environment. It must return `{:ok, keyword}` with the updated
  list of configuration or `:ignore` (only in the `:supervisor` case).
  """
  @callback init(:supervisor | :dry_run, config :: Keyword.t) :: {:ok, Keyword.t} | :ignore

  @doc """
  Shuts down the repository represented by the given pid.
  """
  @callback stop(pid, timeout) :: :ok

  @doc """
  A callback that is invoked when a writer finishes dumping to elasticsearch
  """
  @callback on_complete(es_dumped_messages) :: nil | no_return

  alias Rivulet.ElasticSearchSink.Config
  alias Rivulet.ElasticSearchSink.Database.ElasticSearchGenerator

  def ensure_index_and_mapping_created!(%Config{} = config) do
    with :ok <- ensure_index_created!(config),
         :ok <- ensure_mapping_created!(config),
     do: :ok
  end

  @doc """
  Will return :ok
  """
  def ensure_index_created!(%Config{elastic_url: url, elastic_index: index} = config) do
    case Elastix.Index.exists?(url, index) do
      {:ok, false} -> ElasticSearchGenerator.create_index(config)
      {:ok, true} -> :ok
    end
  end

  @doc """
  Will return :ok
  """
  def ensure_mapping_created!(config) do
    ElasticSearchGenerator.create_mapping(config)
  end
end
