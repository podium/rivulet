defmodule Rivulet.Avro.Schema do
  @enforce_keys [:schema_id, :schema]
  defstruct [:schema_id, :schema]
  @type t :: %__MODULE__{
    schema_id: pos_integer,
    schema: term # As returned by :eavro
  }
end
