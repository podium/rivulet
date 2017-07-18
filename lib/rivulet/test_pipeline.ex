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

  defp starting(topic, partition, kafka) do
    with {:ok, deserializer} <- Rivulet.Avro.Deserializer.start_link(topic, partition, kafka),
         {:ok, printer} <- Rivulet.EventPrinter.start_link(deserializer),
         {:ok, _consumer} <- Rivulet.Pipeline.Consumer.start_link(printer) do
      :ok
    end
  end
end
