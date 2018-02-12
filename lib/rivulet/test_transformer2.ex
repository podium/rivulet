defmodule Rivulet.TestTransformer2 do
  use Rivulet.Transformer

  def handle_message(%Rivulet.Kafka.Consumer.Message{} = m) do
    {m.raw_key, m.raw_value}
  end
end
