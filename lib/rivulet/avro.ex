defmodule Rivulet.Avro do
  @type avro_message :: <<_ :: 40, _ :: _*1>>
  @type schema_id :: pos_integer
  @type schema :: term

  alias Rivulet.Avro.{Cache, Registry}
  alias Rivulet.Kafka.Partition

  defmodule DeserializationError do
    defexception [:message]
  end

  @spec decode(avro_message) :: {:ok, term} | {:error, Registry.reason}
  def decode(msg) do
    schema_resp =
      msg
      |> schema_id
      |> schema

    case schema_resp do
      {:ok, schema} -> {:ok, :eavro.decode(schema, message(msg))}
      {:error, _reason} = err -> err
    end
  end

  @spec encode(bitstring, schema_id) :: avro_message
  def encode(msg, schema_id) when is_integer(schema_id) do
    with {:ok, schema} <- schema(schema_id) do
      encode(msg, schema_id, schema)
    end
  end

  @spec encode(bitstring, schema_id, schema) :: avro_message
  def encode(msg, schema_id, schema) do
    msg = :eavro.encode(schema, msg)
    <<0, schema_id :: size(32), msg :: binary>>
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

  @spec schema_for(Partition.topic) :: schema | nil
  def schema_for(_topic) do
    raise "Function not implemented yet!"
  end

  @spec message(avro_message) :: bitstring
  def message(<<0, _id :: size(32), message :: bitstring>>) do
    message
  end

  @spec schema(schema_id) :: {:ok, schema} | {:error, Registry.reason}
  def schema(schema_id) do
    cached = Cache.get(schema_id)

    if cached do
      {:ok, cached}
    else
      with {:ok, schema} <- Registry.get_schema(schema_id) do
        Cache.put(schema_id, schema)
        {:ok, schema}
      end
    end
  end
end
