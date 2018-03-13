defmodule Rivulet.SQLSink.Database.Postgres do
  def column_type({:varchar, length}) when is_integer(length), do: "VARCHAR(#{inspect length})"
  def column_type(:text), do: "TEXT"
  def column_type(:date), do: "DATE"
  def column_type(:time), do: "TIME"
  def column_type(:integer), do: "INT"
  def column_type(:utc_datetime), do: "TIMESTAMP"
  def column_type(:long), do: "BIGINT"
  def column_type(:float), do: "REAL"
  def column_type(:double), do: "DOUBLE PRECISION"
  def column_type(:blob), do: "BYTEA"
  def column_type(:uuid), do: "UUID"
  def column_type(:boolean), do: "BOOLEAN"
  def column_type({:nullable, type}), do: column_type(type)
end
