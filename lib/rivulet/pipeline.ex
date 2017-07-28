defmodule Rivulet.Pipeline do
  use GenServer

  alias Rivulet.Kafka.Partition

  @callback init(Partition.t, GenStage.stage) :: :ok | {:error, term}

  defmacro __using__(_) do
    quote do
      @behaviour Rivulet.Pipeline
    end
  end

  @spec start_link(module, Partition.t, Partition.offset | nil, GenServer.options)
  :: GenServer.on_start
  def start_link(module, %Partition{} = partition, offset \\ nil, opts \\ []) do
    GenServer.start_link(__MODULE__, {module, partition, offset}, opts)
  end

  def init({module, %Partition{} = partition, offset}) do
    resp =
      with {:ok, kafka} <- kafka(partition, offset)  do
        module.init(partition, kafka)
      end

    case resp do
      :ok -> {:ok, {}}
      {:error, reason} -> {:stop, reason}
      _ -> {:stop, :invalid_init_return}
    end
  end

  defp kafka(%Partition{} = partition, nil) do
    Rivulet.Kafka.Consumer.start_link(partition)
  end

  defp kafka(%Partition{} = partition, offset) do
    Rivulet.Kafka.Consumer.start_link(partition, offset)
  end
end
