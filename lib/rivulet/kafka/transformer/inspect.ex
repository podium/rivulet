defmodule Rivulet.Transformer.Inspect do
  def handle_message(%Rivulet.Kafka.Consumer.Message{} = m) do
    IO.inspect({m.raw_key, m.raw_value})
    nil
  end
end
