defmodule Rivulet.SQLSink.AvroConverter.Type do
  alias AvroEx.Schema.{Context, Fixed, Primitive, Union, Enum}
  alias Rivulet.SQLSink.Database.Table

  require Logger

  @type schema :: Primitive.t | Union.t | Fixed.t

  @spec column_type(schema, Context.t) :: Table.column | :unknown
  def column_type(%Primitive{type: :string, metadata: %{"logicalType" => "bounded", "maxSize" => max_size}}, _),
    do: {:varchar, max_size}
  def column_type(%Primitive{type: :string, metadata: %{"logicalType" => "bounded"}}, _),
    do: {:varchar, 255}
  def column_type(%Primitive{type: :string}, _), do: :text
  def column_type(%Enum{}, _), do: :text

  def column_type(%Primitive{type: :integer, metadata: %{"logicalType" => "date"}}, _), do: :date
  def column_type(%Primitive{type: :integer, metadata: %{"logicalType" => "time-micros"}}, _), do: :time
  def column_type(%Primitive{type: :integer, metadata: %{"logicalType" => "time-millis"}}, _), do: :time
  def column_type(%Primitive{type: :integer}, _), do: :integer

  def column_type(%Primitive{type: :long, metadata: %{"logicalType" => "timestamp-nanos"}}, _), do: :utc_datetime
  def column_type(%Primitive{type: :long, metadata: %{"logicalType" => "timestamp-micros"}}, _), do: :utc_datetime
  def column_type(%Primitive{type: :long, metadata: %{"logicalType" => "timestamp-millis"}}, _), do: :utc_datetime
  def column_type(%Primitive{type: :long}, _), do: :long
  def column_type(%Primitive{type: :float}, _), do: :float
  def column_type(%Primitive{type: :double}, _), do: :double
  def column_type(%Primitive{type: :bytes}, _), do: :blob
  def column_type(%Fixed{metadata: %{"logicalType" => "uuid"}, size: 16}, _), do: :uuid

  def column_type(%Primitive{type: :boolean}, _), do: :boolean
  def column_type(%Union{possibilities: [%Primitive{type: nil}, other]}, %Context{} = context) do
    case column_type(other, context) do
      :unknown -> :unknown
      type -> {:nullable, type}
    end
  end
  def column_type(name, %Context{} = context) when is_binary(name) do
    Logger.debug("Checking type of named column: #{name}")
    name
    |> AvroEx.named_type(context)
    |> column_type(context)
  end
  def column_type(_, _), do: :unknown
end
