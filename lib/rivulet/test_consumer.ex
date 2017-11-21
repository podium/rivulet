defmodule Rivulet.TestConsumer do
  require Record

  alias Rivulet.Kafka.Consumer.Message
  alias Rivulet.Avro

  Record.defrecord(:kafka_message_set, Record.extract(:kafka_message_set, from_lib: "brod/include/brod.hrl"))

  def start_link(client_id, group_id, topics, group_config, consumer_config) do
    :brod.start_link_group_subscriber(client_id, group_id, topics,
                                      group_config, consumer_config,
                                      :message_set,
                                      _CallbackModule  = __MODULE__,
                                      _CallbackInitArg = {})
  end

  def init(_group_id, {}) do
    {:ok, {}}
  end

  def handle_message(topic, _partition, messages, state) when Record.is_record(messages, :kafka_message_set) and is_binary(topic) do
    messages
    |> kafka_message_set(:messages)
    |> Message.from_wire_message
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
