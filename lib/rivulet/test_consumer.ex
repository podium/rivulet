defmodule Rivulet.TestConsumer do
  require Record

  alias Rivulet.Kafka.{Message, Partition}
  alias Rivulet.Avro


  def init(topic, partition) do
    {:ok, %Partition{topic: topic, partition: partition}}
  end

  def handle_message(messages, %Partition{} = partition) do
    messages
    |> Message.from_wire_message
    |> Enum.reject(fn
         (%Message{raw_value: nil}) -> true
         (%Message{}) -> false
       end)
    |> Avro.bulk_decode(partition)
    |> Enum.each(fn(%Message{decoded_key: k, decoded_value: v}) ->
         IO.inspect("#{inspect k} = #{inspect v}")
       end)

    {:async_commit, partition}
  end

  def handle_call(_msg, _from, state) do
    {:reply, {:error, :unknown_message}, state}
  end
end
