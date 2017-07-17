defmodule Rivulet.TestPipeline do
  def start_link(topic, partition) do
    with {:ok, kafka} <- Rivulet.Kafka.Consumer.start_link(topic, partition) do
      starting(topic, partition, kafka)
    end
  end

  def start_link(topic, partition, offset) do
    with {:ok, kafka} <- Rivulet.Kafka.Consumer.start_link(topic, partition, offset) do
      starting(topic, partition, kafka)
    end
  end

  defp starting(topic, partition, kafka) do
    with {:ok, deserializer} <- Rivulet.Avro.Deserializer.start_link(topic, partition),
         {:ok, printer} <- Rivulet.EventPrinter.start_link do

        GenStage.sync_subscribe(deserializer, to: kafka)
        GenStage.sync_subscribe(printer, to: deserializer)

        {:ok, kafka}
    end
  end
end
