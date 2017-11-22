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
  alias Rivulet.Kafka.Partition
  alias Rivulet.Kafka.Consumer.Message

  defmodule DeserializationError do
    defexception [:message]
  end

  def schema_for_subject(subject) when is_binary(subject) do
    cached = Cache.get(subject)

    if cached do
      Logger.debug("Found #{inspect subject} in Avro Cache")
      {:ok, cached}
    else
      Logger.debug("#{inspect subject} not found in Avro Cache - checking registry")
      with {:ok, id} <- Registry.schema_for(subject) do
        Cache.put(subject, id)
        {:ok, id}
      end
    end
  end

  @spec bulk_decode([Message.t], Partition.topic) :: [Message.t]
  def bulk_decode(messages, topic) when is_binary(topic) do
    messages
    |> Enum.map(fn(%Message{} = msg) ->
      schema_id = schema_id(msg.raw_value)

      if schema_id do
        schema = schema(schema_id)

        case decode(msg.raw_key) do
          {:ok, decoded_key} ->
            %Message{msg | decoded_key: decoded_key, raw_key: nil}
          {:error, reason} ->
            Logger.error("[TOPIC: #{topic}][OFFSET: #{msg.offset}] failed to decode for reason: #{inspect reason}")
            %Message{msg | decoded_key: {:error, :avro_decoding_failed, msg}, key_schema: schema}
          end
      end
    end)
    |> Enum.filter(&(&1)) # Remove nil values (when schema_id is encoded incorrectly)
    |> Enum.map(fn(%Message{} = msg) ->
      schema_id = schema_id(msg.raw_value)

      if schema_id do
        schema = schema(schema_id)

        case decode(msg.raw_value) do
          {:ok, decoded_value} ->
            %Message{msg | decoded_value: decoded_value, raw_value: nil}
          {:error, reason} ->
            Logger.error("[TOPIC: #{topic}][OFFSET: #{msg.offset}] failed to decode for reason: #{inspect reason}")
            %Message{msg | decoded_value: {:error, :avro_decoding_failed, msg}, value_schema: schema}
        end
      end
    end)
    |> Enum.filter(&(&1)) # Remove nil values (when schema_id is encoded incorrectly)
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

  @spec schema_id(avro_message) :: pos_integer | nil
  def schema_id(<<0, id :: size(32), _rest :: bitstring>>) do
    id
  end

  def schema_id(_) do
    nil
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
end
