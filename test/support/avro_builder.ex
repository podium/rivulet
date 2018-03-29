defmodule Rivulet.AvroBuilder do
  alias AvroEx.Schema
  alias AvroEx.Schema.{Context, Fixed, Record, Record.Field, Primitive, Union}
  def record(name) do
    %Record{name: name, fields: []}
  end

  def with_field(%Record{} = record, name, type) do
    %Record{record | fields: record.fields ++ [%Field{name: name, type: type}]}
  end

  def primitive(type, metadata \\ %{}) do
    %Primitive{type: type, metadata: metadata}
  end

  def nullable(type) do
    %Union{possibilities: [primitive(nil), type]}
  end

  def date() do
    primitive(:integer, logical_type("date"))
  end

  def date_time(:micro) do
    primitive(:long, logical_type("timestamp-micros"))
  end

  def date_time(:milli) do
    primitive(:long, logical_type("timestamp-millis"))
  end

  def logical_type(type) do
    %{"logicalType" => type}
  end

  def wrap_in_schema(schema) do
    %Schema{schema: schema, context: %Context{}}
  end

  def uuid do
    %Fixed{size: 16, metadata: logical_type("uuid")}
  end
end
