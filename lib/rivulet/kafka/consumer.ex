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

  def handle_message(topic, partition, messages, {callback_module, state}) when Record.is_record(messages, :kafka_message_set) and is_binary(topic) do
    partition = %Partition{topic: topic, partition: partition}
    messages =
      messages
      |> kafka_message_set(:messages)
      |> Message.from_wire_message

    case apply(callback_module, :handle_messages, [partition, messages, state]) do
      {:ok, state} ->
        {:ok, {callback_module, state}}
      {:ok, :ack, state} ->
        {:ok, :ack, {callback_module, state}}
    end
  end
end
