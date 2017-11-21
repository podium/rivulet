defmodule Rivulet.TestConsumer do
  alias Rivulet.Kafka.Consumer.Message
  alias Rivulet.Kafka.Partition
  alias Rivulet.Avro

  def start_link(%Rivulet.Consumer.Config{} = config) do
    Rivulet.Consumer.start_link(__MODULE__, config)
  end

  def init(_) do
    {:ok, {}}
  end

  def handle_messages(%Partition{topic: topic}, messages, state) when is_list(messages) do
    messages
    |> Enum.reject(fn
      (%Message{raw_value: nil}) -> true
      (%Message{}) -> false
    end)
    |> Avro.bulk_decode(topic)
    |> Enum.each(fn(%Message{decoded_key: k, decoded_value: v}) ->
      IO.inspect("#{inspect k} = #{inspect v}")
    end)

    {:ok, :ack, state}
  end

  def handle_call(_msg, _from, state) do
    {:reply, {:error, :unknown_message}, state}
  end
end
