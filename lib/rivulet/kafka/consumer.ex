defmodule Rivulet.Consumer do
  defstruct [:pid]
  defmodule Config do
    alias Rivulet.Kafka.Partition
    @enforce_keys [:client_id, :consumer_group_name, :topics, :group_config, :consumer_config]
    defstruct [
      :client_id,
      :consumer_group_name,
      :topics,
      group_config: [
        offset_commit_policy: :commit_to_kafka_v2,
        offset_commit_interval_seconds: 5
      ],
      consumer_config: [begin_offset: :earliest]
    ]

    @type t :: %__MODULE__{
      client_id: atom,
      consumer_group_name: String.t,
      topics: [Partition.topic],
      group_config: Keyword.t,
      consumer_config: Keyword.t,
    }
  end
  require Record

  alias Rivulet.Kafka.Partition
  alias Rivulet.Kafka.Consumer.Message

  Record.defrecord(:kafka_message_set, Record.extract(:kafka_message_set, from_lib: "brod/include/brod.hrl"))

  @type state :: term
  @type messages :: [Rivulet.Kafka.Consumer.Message.t]

  @callback handle_messages(Partition.t, messages, state)
  :: {:ok, state}
  | {:ok, :ack, state}

  @callback init(term) :: {:ok, state}

  @behaviour :brod_group_subscriber

  # Public API

  @doc """

  """
  @spec start_link(atom, Config.t, [term]) :: GenServer.on_start
  def start_link(callback_module, %Config{} = config, extra \\ []) do
    :brod_group_subscriber.start_link(config.client_id, config.consumer_group_name,
                                      config.topics, config.group_config,
                                      config.consumer_config,
                                      :message_set, _CallbackModule  = __MODULE__,
                                      _CallbackInitArg = {callback_module, extra})
  end

  @spec ack(atom | pid, Partition.t, Partition.offset) :: :ok | {:error, term}
  def ack(ref, %Partition{topic: topic, partition: partition}, offset)
  when is_binary(topic)
  and is_integer(partition)
  and is_integer(offset) do
    ack(ref, topic, partition, offset)
  end

  def ack(ref, topic, partition, offset)
  when is_binary(topic)
  and is_integer(partition)
  and is_integer(offset) do
    :brod_group_subscriber.ack(ref, topic, partition, offset)
  end

  # Callback Functions

  def init(_group_id, {callback_module, extra}) do
    {:ok, state} = apply(callback_module, :init, [extra])
    {:ok, {callback_module, state}}
  end

  @doc """
  handle_message partition:

  message_set:

  from_wire_message:

  callback_module:

  handle_message partition: %Rivulet.Kafka.Partition{partition: 2, topic: "platform_locations"}
message_set: [
  {:kafka_message, 0, 1, 0,
   <<0, 0, 0, 0, 1, 95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144, 132, 183,
     140, 176, 197>>,
   <<0, 0, 0, 0, 3, 2, 0, 208, 233, 186, 215, 241, 153, 231, 164, 42, 60, 80,
     97, 117, 108, 32, 66, 108, 97, 110, 99, 111, 39, 115, 32, 71, 111, 111,
     100, 32, 67, 97, 114, 32, 67, 111, 109, ...>>, 2775125410, :create, -1},
  {:kafka_message, 1, 1, 0,
   <<0, 0, 0, 0, 1, 95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144, 132, 183,
     140, 176, 197>>,
   <<0, 0, 0, 0, 3, 2, 0, 208, 233, 186, 215, 241, 153, 231, 164, 42, 60, 80,
     97, 117, 108, 32, 66, 108, 97, 110, 99, 111, 39, 115, 32, 71, 111, 111,
     100, 32, 67, 97, 114, 32, 67, 111, ...>>, 2889956219, :create, -1}
]
from_wire_message: [
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 2775125410,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 0,
    raw_key: <<0, 0, 0, 0, 1, 95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
      132, 183, 140, 176, 197>>,
    raw_value: <<0, 0, 0, 0, 3, 2, 0, 208, 233, 186, 215, 241, 153, 231, 164,
      42, 60, 80, 97, 117, 108, 32, 66, 108, 97, 110, 99, 111, 39, 115, 32, 71,
      111, 111, 100, 32, 67, 97, 114, 32, 67, ...>>,
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 2889956219,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 1,
    raw_key: <<0, 0, 0, 0, 1, 95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
      132, 183, 140, 176, 197>>,
    raw_value: <<0, 0, 0, 0, 3, 2, 0, 208, 233, 186, 215, 241, 153, 231, 164,
      42, 60, 80, 97, 117, 108, 32, 66, 108, 97, 110, 99, 111, 39, 115, 32, 71,
      111, 111, 100, 32, 67, 97, 114, 32, ...>>,
    value_schema: nil
  }
]
  """
  def handle_message(topic, partition, messages, {callback_module, state}) when Record.is_record(messages, :kafka_message_set) and is_binary(topic) do
    partition = %Partition{topic: topic, partition: partition}
      |> IO.inspect(label: "handle_message partition")
    messages =
      messages
      |> kafka_message_set(:messages)
      |> IO.inspect(label: "message_set")
      |> Message.from_wire_message
      |> IO.inspect(label: "from_wire_message")

    Enum.each(messages, fn (%Message{raw_key: rk, raw_value: rv}) ->
      IO.inspect(Rivulet.Avro.decode(rk), label: "decoded raw_key")
      IO.inspect(Rivulet.Avro.decode(rv), label: "decoded raw_value")
    end)

    IO.inspect(callback_module, label: "callback_module")
    case apply(callback_module, :handle_messages, [partition, messages, state]) do
      {:ok, state} ->
        {:ok, {callback_module, state}}
      {:ok, :ack, state} ->
        {:ok, :ack, {callback_module, state}}
    end
  end
end
