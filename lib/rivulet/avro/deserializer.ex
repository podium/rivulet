defmodule Rivulet.Avro.Deserializer do
  use GenStage
  require Logger

  alias Rivulet.Avro

  defmodule State do
    defstruct [topic: "", partition: -1]
    @type t :: %__MODULE__{
      topic: String.t,
      partition: integer
    }
  end

  def start_link(topic, partition) do
    GenStage.start_link(__MODULE__, {topic, partition})
  end

  def init({topic, partition}) do
    {:producer_consumer, %State{topic: topic, partition: partition}}
  end

  def handle_events(events, _from, %State{topic: topic, partition: partition} = state) do
    decoded_events =
      events
      |> Enum.map(fn(%KafkaEx.Protocol.Fetch.Message{} = msg) ->
           %KafkaEx.Protocol.Fetch.Message{msg | key: decode_value(msg.key, topic, partition, msg.offset)}
         end)
      |> Enum.map(fn(%KafkaEx.Protocol.Fetch.Message{} = msg) ->
           %KafkaEx.Protocol.Fetch.Message{msg | value: decode_value(msg.value, topic, partition, msg.offset)}
         end)

    {:noreply, decoded_events, state}
  end

  @spec decode_value(Avro.avro_message, Partition.topic, Partition.partition | -1, Partition.offset)
  :: term
  | {:error, :avro_decoding_failed, Avro.avro_message}
  defp decode_value(msg, topic, partition, offset) when is_binary(msg) do
    case Avro.decode(msg) do
      {:ok, new_value} -> new_value
      {:error, reason} ->
        Logger.error("[TOPIC: #{topic}][PARTITION: #{partition}][OFFSET: #{offset}] failed to decode for reason: #{inspect reason}")
        %KafkaEx.Protocol.Fetch.Message{msg | value: {:error, :avro_decoding_failed, msg}}
    end
  end
end
