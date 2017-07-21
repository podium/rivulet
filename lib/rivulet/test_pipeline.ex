defmodule Rivulet.TestPipeline do
  use GenServer

  alias Rivulet.Kafka.Partition

  def start_link(%Partition{} = partition, offset \\ nil) do
    GenServer.start_link(__MODULE__, {partition, offset})
  end

  def init({%Partition{} = partition, nil}) do
    :ok =
      with {:ok, kafka} <- Rivulet.Kafka.Consumer.start_link(partition) do
        starting(partition, kafka)
      end

    {:ok, {}}
  end

  def init({%Partition{} = partition, offset}) do
    :ok =
      with {:ok, kafka} <- Rivulet.Kafka.Consumer.start_link(partition, offset) do
        starting(partition, kafka)
      end

    {:ok, {}}
  end

  @spec starting(Partition.t, GenStage.stage) :: :ok | no_return
  defp starting(%Partition{} = partition, kafka) do
    with {:ok, printer1} <- Rivulet.EventPrinter.start_link(kafka),
         {:ok, deserializer} <- Rivulet.Avro.Deserializer.start_link(partition, printer1),
         {:ok, printer2} <- Rivulet.EventPrinter.start_link(deserializer),
         {:ok, _consumer} <- Rivulet.Pipeline.Consumer.start_link(printer2) do
      :ok
    end
  end
end
