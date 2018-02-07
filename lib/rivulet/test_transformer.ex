defmodule Rivulet.TestTransformer do
  use Rivulet.Transformer

  def handle_message(%Rivulet.Kafka.Consumer.Message{} = m) do
    [nil, {m.raw_key, m.raw_value}]
  end
end
