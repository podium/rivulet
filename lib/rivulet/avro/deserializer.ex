defmodule Rivulet.Avro.Deserializer do
  use GenStage
  require Logger

  alias Rivulet.Avro

  defmodule State do
    defstruct [topic: ""]
    @type t :: %__MODULE__{
      topic: String.t
    }
  end

  def start_link(topic) do
    GenStage.start_link(__MODULE__, [topic])
  end

  def init([topic]) do
    {:producer_consumer, %State{topic: topic}}
  end

  def handle_events(events, _from, %State{topic: topic} = state) do
    decoded_events =
      Enum.map(events, fn(%KafkaEx.Protocol.Fetch.Message{} = msg) ->
        case Avro.decode(msg.value) do
          {:ok, new_value} -> %KafkaEx.Protocol.Fetch.Message{msg | value: new_value}
          {:error, reason} ->
            Logger.error("[TOPIC: #{topic}] [OFFSET: #{inspect msg.offset}]Avro-Decoding message failed - #{inspect reason}")
            %KafkaEx.Protocol.Fetch.Message{msg | value: {:error, :avro_decoding_failed, msg.value}}
        end
      end)

    {:noreply, decoded_events, state}
  end
end
