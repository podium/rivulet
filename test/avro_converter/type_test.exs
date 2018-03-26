defmodule Rivulet.SQLSink.AvroConverter.Type.Test.Macros do
  defmacro __using__(_) do
    quote do
      require __MODULE__.Macros
      import __MODULE__.Macros
    end
  end
  defmacro translates_to(json, type) do
    quote do
      @tag :avro_converter
      test "translates to #{inspect unquote(type)}-#{:erlang.unique_integer()}" do
        type =
          unquote(json)
          |> AvroEx.parse_schema!
          |> Map.get(:schema)
          |> @test_module.column_type(%AvroEx.Schema.Context{})

        assert type == unquote(type)
      end
    end
  end

  defmacro nullable(term) do
    quote do
      {:nullable, unquote(term)}
    end
  end
end

defmodule Rivulet.SQLSink.AvroConverter.Type.Test do
  use ExUnit.Case
  use __MODULE__.Macros

  @test_module Rivulet.SQLSink.AvroConverter.Type

  def avro(json) do
    json
  end

  describe "string" do
    translates_to(~S("string"), :text)
    translates_to(~S(["null", "string"]), {:nullable, :text})

    translates_to(~S({"type": "string", "logicalType": "bounded"}), {:varchar, 255})
    translates_to(~S(["null", {"type": "string", "logicalType": "bounded"}]), {:nullable, {:varchar, 255}})

    translates_to(~S({"type": "string", "logicalType": "bounded", "maxSize": 254}), {:varchar, 254})
    translates_to(~S(["null", {"type": "string", "logicalType": "bounded", "maxSize": 254}]), {:nullable, {:varchar, 254}})
  end

  describe "integer" do
    translates_to(~S("int"), :integer)
    translates_to(~S(["null", "int"]), nullable(:integer))

    translates_to(~S({"type": "int", "logicalType": "date"}), :date)
    translates_to(~S(["null", {"type": "int", "logicalType": "date"}]), nullable(:date))

    translates_to(~S({"type": "int", "logicalType": "time-millis"}), :time)
    translates_to(~S(["null", {"type": "int", "logicalType": "time-millis"}]), nullable(:time))

    translates_to(~S({"type": "int", "logicalType": "time-micros"}), :time)
    translates_to(~S(["null", {"type": "int", "logicalType": "time-micros"}]), nullable(:time))
  end

  describe "long" do
    translates_to(~S({"type": "long"}), :long)
    translates_to(~S(["null", {"type": "long"}]), nullable(:long))

    translates_to(~S({"type": "long", "logicalType": "timestamp-millis"}), :utc_datetime)
    translates_to(~S(["null", {"type": "long", "logicalType": "timestamp-millis"}]), nullable(:utc_datetime))

    translates_to(~S({"type": "long", "logicalType": "timestamp-micros"}), :utc_datetime)
    translates_to(~S(["null", {"type": "long", "logicalType": "timestamp-micros"}]), nullable(:utc_datetime))
  end

  describe "boolean" do
    translates_to(~S({"type": "boolean"}), :boolean)
    translates_to(~S(["null", {"type": "boolean"}]), nullable(:boolean))
  end

  describe "bytes" do
    translates_to(~S({"type": "bytes"}), :blob)
    translates_to(~S(["null", {"type": "bytes"}]), nullable(:blob))
  end

  describe "float" do
    translates_to(~S({"type": "float"}), :float)
    translates_to(~S(["null", {"type": "float"}]), nullable(:float))
  end

  describe "double" do
    translates_to(~S({"type": "double"}), :double)
    translates_to(~S(["null", {"type": "double"}]), nullable(:double))
  end

  describe "fixed" do
    translates_to(~S({"type": "fixed", "size": 16, "logicalType": "uuid", "name": "uid"}), :uuid)
    translates_to(~S(["null", {"type": "fixed", "size": 16, "logicalType": "uuid", "name": "uid"}]), nullable(:uuid))
  end
end
