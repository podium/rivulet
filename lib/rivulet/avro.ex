defmodule Rivulet.Avro do
  @typedoc """
  An avro message with 40 bytes of metadata plus the actual message. The first
  byte of metadata is a magic byte specifying metadata version (currently 0).
  The remaining metadata is the Schema ID as returned by the Schema Registry.
  """
  @type avro_message :: <<_ :: 40, _ :: _*1>>
  @type schema_id :: pos_integer
  @type schema :: term
  @type buffer :: binary
  @type decoded_message :: {term, buffer}

  require Logger

  alias Rivulet.Avro.{Cache, Registry, Schema}
  alias Rivulet.Kafka.{Message, Partition}

  defmodule DeserializationError do
    defexception [:message]
  end

  defdelegate schema_for(topic), to: Registry

  @spec bulk_decode([Message.t], Partition.t) :: [Message.t]
  def bulk_decode(messages, %Partition{} = partition) do
    %{key: key_schema, value: value_schema} = schema_for(partition.topic)

    messages
    |> Enum.map(fn(%Message{} = msg) ->
         decoded_key = decode_value(msg.raw_key, partition, msg.offset)
         %Message{msg | decoded_key: decoded_key, key_schema: key_schema}
       end)
    |> Enum.map(fn(%Message{} = msg) ->
         decoded_value = decode_value(msg.raw_value, partition, msg.offset)
         %Message{msg | decoded_value: decoded_value, value_schema: value_schema}
       end)
  end

  @spec decode(avro_message)
  :: {:ok, decoded_message} | {:error, term}
  def decode(msg) do
    schema_resp =
      msg
      |> schema_id
      |> schema

    case schema_resp do
      {:ok, %Schema{schema: schema}} ->
        decode(msg, schema)
      {:error, _reason} = err -> err
    end
  end

  @spec decode(avro_message, Schema.t | schema)
  :: {:ok, term} | no_return
  def decode(msg, %Schema{schema: schema}) do
    decode(msg, schema)
  end

  def decode(msg, schema) do
    AvroEx.decode(schema, message(msg))
  end

  @spec decode!(avro_message) :: decoded_message | no_return
  def decode!(msg) do
    {:ok, decoded_message} = decode(msg)
    decoded_message
  end

  @spec decode!(avro_message, Schema.t | schema) :: decoded_message | no_return
  def decode!(msg, schema) do
    {:ok, decoded_message} = decode(msg, schema)
    decoded_message
  end

  @spec encode(term, schema_id | Schema.t)
  :: {:ok, avro_message } | {:error, term}
  def encode(msg, %Schema{schema_id: schema_id, schema: schema}) do
    encode(msg, schema_id, schema)
  end

  def encode(msg, schema_id) when is_integer(schema_id) do
    with {:ok, %Schema{schema: schema}} <- schema(schema_id) do
      encode(msg, schema_id, schema)
    end
  end

  @spec encode(bitstring, schema_id, schema) :: {:ok, avro_message}
  def encode(msg, schema_id, schema) do
    with {:ok, msg} <- AvroEx.encode(schema, msg) do
      {:ok, wrap(msg, schema_id)}
    end
  end

  @spec encode!(term, schema_id | Schema.t) :: avro_message | no_return
  def encode!(msg, schema) do
    {:ok, avro_msg} = encode(msg, schema)
    avro_msg
  end

  @spec encode!(term, schema_id, schema) :: avro_message | no_return
  def encode!(msg, schema_id, schema) do
    {:ok, avro_msg} = encode(msg, schema_id, schema)
    avro_msg
  end

  @spec schema_id(avro_message) :: pos_integer | no_return
  def schema_id(<<0, _id :: size(32), "" :: bitstring>>) do
    raise DeserializationError, "Avro message has no message after the headers"
  end

  def schema_id(<<0, id :: size(32), _rest :: bitstring>>) do
    id
  end

  def schema_id(_) do
    raise DeserializationError, "Avro message wasn't encoded in the confluent style"
  end

  @spec message(avro_message) :: bitstring
  def message(<<0, _id :: size(32), message :: bitstring>>) do
    message
  end

  @spec schema(schema_id) :: {:ok, Schema.t} | {:error, term}
  def schema(schema_id) do
    cached = Cache.get(schema_id)

    if cached do
      Logger.debug("Found #{inspect schema_id} in Avro Cache")
      {:ok, cached}
    else
      Logger.debug("#{inspect schema_id} not found in Avro Cache - checking registry")
      with {:ok, %Schema{} = schema} <- Registry.get_schema(schema_id) do
        Cache.put(schema_id, schema)
        {:ok, schema}
      end
    end
  end

  def wrap(msg, schema_id) do
    <<0, schema_id :: size(32), msg :: binary>>
  end

  @spec decode_value(avro_message | nil, Partition.t, Partition.offset)
  :: decoded_message
  | {:error, :avro_decoding_failed, avro_message}
  defp decode_value(msg, %Partition{} = partition, offset) when is_binary(msg) do
    case decode(msg) do
      {:ok, new_value} -> new_value
      {:error, reason} ->
        Logger.error("[TOPIC: #{partition.topic}][PARTITION: #{partition.partition}][OFFSET: #{offset}] failed to decode for reason: #{inspect reason}")
        {:error, :avro_decoding_failed, msg}
    end
  end
end
