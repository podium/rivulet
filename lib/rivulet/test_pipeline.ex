defmodule Rivulet.TestPipeline do
  use GenServer

  def start_link(topic, partition, offset \\ nil) do
    GenServer.start_link(__MODULE__, {topic, partition, offset})
  end

  def init({topic, partition, nil}) do
    :ok =
      with {:ok, kafka} <- Rivulet.Kafka.Consumer.start_link(topic, partition) do
        starting(topic, partition, kafka)
      end

    {:ok, {}}
  end

  def init({topic, partition, offset}) do
    :ok =
      with {:ok, kafka} <- Rivulet.Kafka.Consumer.start_link(topic, partition, offset) do
        starting(topic, partition, kafka)
      end

    {:ok, {}}
  end

  @spec starting(Partition.topic, Partition.partition, GenStage.stage) :: :ok | no_return
  defp starting(topic, partition, kafka) do
    with {:ok, printer1} <- Rivulet.EventPrinter.start_link(kafka),
         {:ok, deserializer} <- Rivulet.Avro.Deserializer.start_link(topic, partition, printer1),
         {:ok, printer2} <- Rivulet.EventPrinter.start_link(deserializer),
         {:ok, _consumer} <- Rivulet.Pipeline.Consumer.start_link(printer2) do
      :ok
    end
  end
end
