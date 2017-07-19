defmodule Rivulet.Kafka.Message do
  defstruct attributes: 0, crc: nil, offset: nil, raw_key: nil, raw_value: nil, decoded_key: nil, decoded_value: nil
  @type t :: %__MODULE__{
    attributes: non_neg_integer,
    crc: non_neg_integer,
    offset: non_neg_integer,
    raw_key: binary,
    raw_value: binary,
    decoded_key: nil | term,
    decoded_value: nil | term
  }

  def from_wire_message(%KafkaEx.Protocol.Fetch.Message{} = msg) do
    %__MODULE__{
      attributes: msg.attributes,
      crc: msg.crc,
      offset: msg.offset,
      raw_key: msg.key,
      raw_value: msg.value,
      decoded_key: nil,
      decoded_value: nil
    }
  end
end
