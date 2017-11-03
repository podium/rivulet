defmodule Rivulet.Avro.Cache do
  use GenServer

  alias Rivulet.Avro
  alias Rivulet.Kafka.Partition

  defmodule State do
    defstruct [tid: nil]
  end

  @opaque t :: pid
  @type cache_name :: atom

  # Public API

  @spec start_link(cache_name) :: GenServer.on_start
  def start_link(name \\ :avro_cache) do
    GenServer.start_link(__MODULE__, [name])
  end

  @spec start(cache_name) :: GenServer.on_start
  def start(name \\ :avro_cache) do
    GenServer.start(__MODULE__, [name])
  end

  @spec stop(t) :: :ok
  def stop(cache) when is_pid(cache) do
    GenServer.stop(cache)
  end

  @spec put(cache_name, Avro.schema_id | Partition.topic, Avro.schema | term) :: true
  def put(name \\ :avro_cache, schema_id, schema) do
    :ets.insert(name, {schema_id, schema})
    name
  end

  @spec get(cache_name, Avro.schema_id | Partition.topic) :: Avro.schema | nil | :no_cache
  def get(cache_name \\ :avro_cache, schema_id) do
    case :ets.lookup(cache_name, schema_id) do
      [] -> nil
      [{^schema_id, schema}] -> schema # Since this table is a set, there will only ever be 0 or 1 matches.
    end
  rescue
    ArgumentError -> :no_cache
  end

  # Callback Functions
  def init([name]) do
    tid = :ets.new(name, [:set, :public, :named_table, {:keypos, 1}, {:read_concurrency, true}])
    {:ok, %State{tid: tid}}
  end
end
