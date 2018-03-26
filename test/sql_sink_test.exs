defmodule Rivulet.SQLSink.Test do
  use ExUnit.Case
  import Rivulet.AvroBuilder
  alias Rivulet.SQLSink.Database.Table

  @test_module Rivulet.SQLSink

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

    name_pattern = "blue_kafka_$$"
    topic = "my-topic"
    config =
      %@test_module.Config{
        unique_constraints: [["a", "b"], ["a"]],
        primary_keys: ["c"],
        table_pattern: name_pattern,
        topic: topic
      }

    {:ok, %{schema: record, config: config}}
  end

  describe "table_definition" do
    @tag :current
    test "returns a %Table{} on success", context do
      assert %Table{} =
        @test_module.table_definition(context.schema, context.config)
    end
  end
end
