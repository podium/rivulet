defmodule Rivulet.Transformer.AvroInspect do
  alias Rivulet.Avro

  def handle_message(%Rivulet.Kafka.Consumer.Message{} = m) do
    with {:ok, key} <- Avro.decode(m.raw_key),
         {:ok, value} <- Avro.decode(m.raw_value) do
           nil
    end
  end
end
