defmodule Rivulet.TestPipeline do
  use Rivulet.Pipeline

  alias Rivulet.Kafka.Partition

  @spec start_link(Partition.t, Partition.offset | nil, GenServer.options)
  :: GenServer.on_start
  def start_link(%Partition{} = partition, offset \\ nil, opts \\ []) do
    Rivulet.Pipeline.start_link(__MODULE__, partition, offset, opts)
  end

  @spec init(Partition.t, GenStage.stage) :: :ok | no_return
  def init(%Partition{} = partition, kafka) do
    with {:ok, deserializer} <- Rivulet.Avro.Stage.Deserializer.start_link(partition, kafka),
         {:ok, printer} <- Rivulet.Stage.EventPrinter.start_link(deserializer),
         {:ok, committer} <- Rivulet.Kafka.Stage.OffsetCommitter.start_link(partition, printer),
         {:ok, _consumer} <- Rivulet.Pipeline.Consumer.start_link(committer) do
      :ok
    end
  end
end
