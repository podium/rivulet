defmodule Rivulet.Kafka.Message do
  alias Rivulet.Avro

  require Record
  import Record

  Record.defrecord(:kafka_message, Record.extract(:kafka_message, from_lib: "brod/include/brod.hrl"))

  defstruct attributes: 0, crc: nil, offset: nil, raw_key: nil, raw_value: nil, decoded_key: nil, decoded_value: nil, key_schema: nil, value_schema: nil
  @type t :: %__MODULE__{
    attributes: non_neg_integer,
    crc: non_neg_integer,
    offset: non_neg_integer,
    raw_key: binary,
    raw_value: binary,
    key_schema: Avro.schema | nil,
    value_schema: Avro.schema | nil,
    decoded_key: term | nil,
    decoded_value: term | nil
  }

  def from_wire_message(messages) when is_list(messages) do
    messages
    |> Enum.map(&from_wire_message/1)
    |> Enum.sort(fn(%__MODULE__{} = a, %__MODULE__{} = b) ->
         a.offset <= b.offset
       end)
  end

  def from_wire_message(msg) when is_record(msg, :kafka_message) do
    %__MODULE__{
      attributes: kafka_message(msg, :attributes),
      crc: msg.crc,
      offset: msg.offset,
      raw_key: msg.key,
      raw_value: msg.value,
      decoded_key: nil,
      decoded_value: nil
    }
  end
end
