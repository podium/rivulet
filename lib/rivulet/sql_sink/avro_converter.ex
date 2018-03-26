defmodule Rivulet.SQLSink.AvroConverter do
  alias AvroEx.Schema
  alias AvroEx.Schema.{Context, Primitive, Record, Record.Field, Union}
  alias Rivulet.SQLSink.AvroConverter.Type
  alias Rivulet.SQLSink.Database.Table
  alias Rivulet.Kafka.Partition

  require Logger

  @spec definition(Schema.t, Table.name_pattern, Partition.topic)
  :: :error | Table.t
  def definition(%Schema{} = schema, name_pattern, topic) do
    Logger.debug("Getting table definition for avro schema")
    case columns(schema) do
      {:error, _reason} = err -> err
      columns ->
        table =
          name_pattern
          |> Table.table_name(topic)
          |> Table.new

        columns
        |> Enum.reduce(table, fn({column_name, column_type}, %Table{} = table) ->
          Table.with_column(table, column_name, column_type)
        end)
        |> Table.sort_columns
    end
  end

  @spec columns(Schema.t) :: [Table.column_definition] | :error
  def columns(%Schema{schema: %Union{possibilities: [%Primitive{type: nil}, %Record{} = record]}, context: %Context{} = context}) do
    Logger.debug("Getting column definitions for union")
    do_columns(record, context)
  end
  def columns(%Schema{schema: %Record{} = record, context: %Context{} = context}) do
    Logger.debug("Getting column definitions for record")
    do_columns(record, context)
  end

  defp do_columns(%Record{fields: fields} = record, %Context{} = context) do
    if insertable?(record, context) do
      Enum.map(fields, &({&1.name, Type.column_type(&1.type, context)}))
    else
      {:error, :schema_not_insertable}
    end
  end
  defp do_columns(_, _), do: {:error, :invalid_schema}

  @spec insertable?(Record.t, Context.t) :: boolean
  defp insertable?(%Record{fields: fields}, %Context{} = context) do
    Logger.debug("Checking insertability of record")
    Enum.all?(fields, &(do_insertable?(&1, context)))
  end

  defp do_insertable?(%Field{type: type}, %Context{} = context) do
    Logger.debug("Checking insertability of field")
    do_insertable?(type, context)
  end
  defp do_insertable?(type, %Context{} = context) when is_binary(type) do
    Logger.debug("Checking insertability of named type")
    type
    |> AvroEx.named_type(context)
    |> do_insertable?(context)
  end
  defp do_insertable?(type, %Context{} = context) do
    Logger.debug("Checking insertability of other type: #{inspect type}")
    if Type.column_type(type, context) != :unknown do
      Logger.debug("Type #{inspect type} is insertable")
      true
    else
      Logger.debug("Type #{inspect type} is NOT insertable")
      false
    end
  end
end
