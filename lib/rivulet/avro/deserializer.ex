defmodule Rivulet.Avro.Deserializer do
  use GenStage
  require Logger

  alias Rivulet.Avro
  alias Rivulet.Kafka.{Message, Partition}

  defmodule State do
    @enforce_keys [:partition]
    defstruct [:partition]
    @type t :: %__MODULE__{
      partition: Partition.t
    }
  end

  @spec start_link(Partition.t)
 :: GenServer.on_start
  def start_link(%Partition{} = partition) do
    GenStage.start_link(__MODULE__, {partition, []})
  end

  @spec start_link(Partition.t, GenStage.stage | [GenStage.stage])
  :: GenServer.on_start
  def start_link(%Partition{} = partition, {:global, _} = parent) do
    start_link(partition, [parent])
  end

  def start_link(%Partition{} = partition, {:via, _, _} = parent) do
    start_link(partition, [parent])
  end

  def start_link(%Partition{} = partition, {atom, node} = parent) when is_atom(atom) and is_atom(node) do
    start_link(partition, [parent])
  end

  def start_link(%Partition{} = partition, parent) when is_pid(parent) or is_atom(parent) do
    start_link(partition, [parent])
  end

  def start_link(%Partition{} = partition, parents) when is_list(parents) do
    GenStage.start_link(__MODULE__, {partition, parents})
  end

  def init({%Partition{} = partition, []}) do
    {:producer_consumer, %State{partition: partition}}
  end

  def init({%Partition{} = partition, parents}) do
    {:producer_consumer, %State{partition: partition}, subscribe_to: parents}
  end

  def handle_events(events, _from, %State{partition: partition} = state) do
    Logger.debug("Deserializing events")
    decoded_events =
      events
      |> Enum.map(fn(%Message{} = msg) ->
           %Message{msg | decoded_key: decode_value(msg.raw_key, partition, msg.offset)}
         end)
      |> Enum.map(fn(%Message{} = msg) ->
           %Message{msg | decoded_value: decode_value(msg.raw_value, partition, msg.offset)}
         end)

    {:noreply, decoded_events, state}
  end

  @spec decode_value(Avro.avro_message, Partition.t, Partition.offset)
  :: Avro.schema
  | {:error, :avro_decoding_failed, Avro.avro_message}
  defp decode_value(msg, %Partition{} = partition, offset) when is_binary(msg) do
    case Avro.decode(msg) do
      {:ok, new_value} -> new_value
      {:error, reason} ->
        Logger.error("[TOPIC: #{partition.topic}][PARTITION: #{partition.partition}][OFFSET: #{offset}] failed to decode for reason: #{inspect reason}")
        {:error, :avro_decoding_failed, msg}
    end
  end
end
