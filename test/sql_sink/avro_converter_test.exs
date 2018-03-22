defmodule Rivulet.AvroConverter.Test do
  use ExUnit.Case
  import Rivulet.AvroBuilder
  alias Rivulet.SQLSink.Database.Table

  @test_module Rivulet.SQLSink.AvroConverter

  setup do
    record =
      "Table1"
      |> record
      |> with_field("bool", primitive(:boolean))
      |> with_field("nbool", :boolean |> primitive |> nullable)
      |> with_field("text", :string |> primitive)
      |> with_field("ntext", :string |> primitive |> nullable)
      |> with_field("vchar255", :string |> primitive(logical_type("bounded")))
      |> with_field("nvchar255", :string |> primitive(logical_type("bounded")) |> nullable)
      |> with_field("vchar254", :string |> primitive(%{"logicalType" => "bounded", "maxSize" => 254}))
      |> with_field("nvchar254", :string |> primitive(%{"logicalType" => "bounded", "maxSize" => 254}) |> nullable)
      |> with_field("int", :integer |> primitive)
      |> with_field("nint", :integer |> primitive |> nullable)
      |> with_field("timemicro", :integer |> primitive(logical_type("time-micros")))
      |> with_field("ntimemicro", :integer |> primitive(logical_type("time-micros")) |> nullable)
      |> with_field("timemilli", :integer |> primitive(logical_type("time-millis")))
      |> with_field("ntimemilli", :integer |> primitive(logical_type("time-millis")) |> nullable)
      |> with_field("date", date())
      |> with_field("ndate", date() |> nullable)
      |> with_field("datetimemill", date_time(:milli))
      |> with_field("ndatetimemill", date_time(:milli) |> nullable)
      |> with_field("datetimemicro", date_time(:micro))
      |> with_field("ndatetimemicro", date_time(:micro) |> nullable)
      |> with_field("long", :long |> primitive)
      |> with_field("nlong", :long |> primitive |> nullable)
      |> with_field("float", :float |> primitive)
      |> with_field("nfloat", :float |> primitive |> nullable)
      |> with_field("double", :double |> primitive)
      |> with_field("ndouble", :double |> primitive |> nullable)
      |> with_field("blob", :bytes |> primitive)
      |> with_field("nblob", :bytes |> primitive |> nullable)
      |> with_field("uuid", uuid())
      |> with_field("nuuid", uuid() |> nullable)
      |> wrap_in_schema

    {:ok, %{record: record}}
  end

  @expected_table_definition %Table{
    database: :postgres,
    name: "kafka_platform_things",
    column_definitions: [
      {"bool", :boolean},
      {"nbool", {:nullable, :boolean}},
      {"int", :integer},
      {"nint", {:nullable, :integer}},
      {"date", :date},
      {"ndate", {:nullable, :date}},
      {"timemicro", :time},
      {"ntimemicro", {:nullable, :time}},
      {"timemilli", :time},
      {"ntimemilli", {:nullable, :time}},
      {"datetimemill", :utc_datetime},
      {"ndatetimemill", {:nullable, :utc_datetime}},
      {"datetimemicro", :utc_datetime},
      {"ndatetimemicro", {:nullable, :utc_datetime}},
      {"long", :long},
      {"nlong", {:nullable, :long}},
      {"float", :float},
      {"nfloat", {:nullable, :float}},
      {"double", :double},
      {"ndouble", {:nullable, :double}},
      {"uuid", :uuid},
      {"nuuid", {:nullable, :uuid}},
      {"vchar254", {:varchar, 254}},
      {"nvchar254", {:nullable, {:varchar, 254}}},
      {"vchar255", {:varchar, 255}},
      {"nvchar255", {:nullable, {:varchar, 255}}},
      {"text", :text},
      {"ntext", {:nullable, :text}},
      {"blob", :blob},
      {"nblob", {:nullable, :blob}},
    ]
  }

  describe "definition" do
    test "returns the table name", context do
      assert %Table{name: "kafka_platform_things"} =
        @test_module.definition(context.record, "kafka_$$", "platform-things")
    end

    test "returns the column_definitions", context do
      assert @expected_table_definition =
        @test_module.definition(context.record, "kafka_$$", "platform.things")
    end

    test "can handle named types" do
      {:ok, schema} =
        AvroEx.parse_schema(~S({"type": "record", "name": "MyRecord", "fields": [
          {"type": {"type": "fixed", "size": 16, "logicalType": "uuid", "namespace": "com.podium.platform", "name": "uuidType"}, "name": "field1"},
          {"type": "com.podium.platform.uuidType", "name": "field2"}
        ]}))

      assert %Table{column_definitions: [{"field1", :uuid}, {"field2", :uuid}]} =
        @test_module.definition(schema, "kafka_$$", "platform_things")
    end
  end
end
