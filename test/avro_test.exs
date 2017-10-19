defmodule Rivulet.Avro.Test.Macros do
  defmacro test_deserialize(schema, encode) do
    quote do
      test "decode #{:erlang.unique_integer}" do
        schema =
          case unquote(schema) do
            {:ok, schema} -> schema
            schema -> schema
          end

        {:ok, encoded} = AvroEx.encode(schema, unquote(encode))
        {:ok, decoded} = AvroEx.decode(schema, encoded)

        assert decoded == unquote(encode)
      end
    end
  end
end
defmodule Rivulet.Avro.Test do
  use ExUnit.Case

  @test_module Rivulet.Avro

  @record_schema_json """
  {
    "type" : "record",
    "name": "human",
    "namespace": "mynamespace",
    "fields": [
      {"name": "myfield", "type": "string"},
      { "name": "age", "type": "int"}
    ]
  }
  """

  @null_integer_union_schema_json """
  ["null", "int"]
  """

  @null_record_union_schema_json """
  ["null", #{@record_schema_json}]
  """

  @enum_schema_json """
  {"type": "enum", "name": "Suit", "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]}
  """

  @array_schema_json """
  {"type": "array", "items": "string"}
  """

  @map_schema_json """
  {"type": "map", "values": "string"}
  """

  @fixed_schema_json """
  {"type": "fixed", "name": "md5", "size": 16}
  """

  alias Rivulet.Avro.Schema

  describe "schema_id/1" do
    test "accepts a correctly encoded message" do
      @test_module.schema_id(<<0, 1234 :: size(32), "abcd" :: binary>>)
    end

    test "raises an exception if the magic bit isn't set" do
      assert_raise @test_module.DeserializationError,
        "Avro message wasn't encoded in the confluent style",
      fn ->
        @test_module.schema_id(<<2147483648 :: size(32), "abcd" :: binary>>)
      end
    end

    test "raises an exception if the rest isn't provided" do
      assert_raise @test_module.DeserializationError,
        "Avro message has no message after the headers",
      fn ->
        2147483648 = @test_module.schema_id(<<0, 2147483648 :: size(32)>>)
      end
    end
  end

  describe "encode" do
    setup [:get_schema]

    test "accepts a %Schema{}", %{schema: schema} do
      @test_module.encode(%{"first" => "First", "last" => "Last"}, schema)
    end

    test "accepts a schema id + schema", %{schema: schema} do
      @test_module.encode(%{"first" => "First", "last" => "Last"}, schema.schema_id, schema.schema)
    end
  end

  describe "decode" do
    setup [:get_schema]

    test "accepts a %Schema{}", %{schema: schema} do
      msg = %{"first" => "First", "last" => "Last"}

      assert ^msg =
        msg
        |> @test_module.encode!(schema)
        |> @test_module.decode!(schema)
    end

    test "accepts a schema", %{schema: schema} do
      msg = %{"first" => "First", "last" => "Last"}

      assert ^msg =
        msg
        |> @test_module.encode!(schema)
        |> @test_module.decode!(schema.schema)
    end
  end

  describe "decode primitive types" do
    require __MODULE__.Macros
    import __MODULE__.Macros

    @tag :current
    test_deserialize(AvroEx.parse_schema!(~S("null")), nil)
    test_deserialize(AvroEx.parse_schema!(~S("boolean")), true)
    test_deserialize(AvroEx.parse_schema!(~S("int")), 1)
    test_deserialize(AvroEx.parse_schema!(~S("long")), 1)
    test_deserialize(AvroEx.parse_schema!(~S("float")), 1.0)
    test_deserialize(AvroEx.parse_schema!(~S("double")), 1.0)
    test_deserialize(AvroEx.parse_schema!(~S("bytes")), <<1, 2, 3, 4, 5>>)
    test_deserialize(AvroEx.parse_schema!(~S("string")), "Hello")
  end

  describe "decode complex types" do
    require __MODULE__.Macros
    import __MODULE__.Macros

    test_deserialize(AvroEx.parse_schema!(@record_schema_json), %{"myfield" => "Cody Poll", "age" => 30})
    test_deserialize(AvroEx.parse_schema!(@null_integer_union_schema_json), 30)
    test_deserialize(AvroEx.parse_schema!(@null_integer_union_schema_json), nil)
    test_deserialize(
      AvroEx.parse_schema!(@null_record_union_schema_json),
      %{"myfield" => "Cody Poll", "age" => 30}
    )
    test_deserialize(AvroEx.parse_schema!(@null_record_union_schema_json), nil)
    test_deserialize(AvroEx.parse_schema!(@enum_schema_json), "SPADES")
    test_deserialize(AvroEx.parse_schema!(@array_schema_json), ["hello", "world"])
    test_deserialize(AvroEx.parse_schema!(@map_schema_json), %{"Hello" => "world", "it's" => "me"})
    test_deserialize(AvroEx.parse_schema!(@fixed_schema_json), "aaaaaaaaaaaaaaaa")
  end


  def get_schema(_) do
    schema =
      [File.cwd!(), "priv", "avro_schemas", "test-log", "value.avsc"]
      |> Path.join
      |> File.read!
      |> AvroEx.parse_schema!

    {:ok, %{schema: %Schema{schema: schema, schema_id: 404}}}
  end
end

