defmodule Rivulet.Avro.Schema do
  alias Rivulet.Avro

  @enforce_keys [:schema_id, :schema]
  defstruct [:schema_id, :schema]
  @type t :: %__MODULE__{
    schema_id: Avro.schema_id,
    schema: Avro.schema # As returned by :eavro
  }
end
