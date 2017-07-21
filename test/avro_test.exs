defmodule Rivulet.Avro.Test do
  use ExUnit.Case

  @test_module Rivulet.Avro

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
      @test_module.encode(["First", "Last"], schema)
    end

    test "accepts a schema id + schema", %{schema: schema} do
      @test_module.encode(["First", "Last"], schema.schema_id, schema.schema)
    end
  end

  describe "decode" do
    setup [:get_schema]

    test "accepts a %Schema{}", %{schema: schema} do
      msg = ["First", "Last"]

      assert {^msg, ""} =
        msg
        |> @test_module.encode!(schema)
        |> @test_module.decode!(schema)
    end

    test "accepts a schema", %{schema: schema} do
      msg = ["First", "Last"]

      assert {^msg, ""} =
        msg
        |> @test_module.encode!(schema)
        |> @test_module.decode!(schema.schema)
    end
  end

  def get_schema(_) do
    schema =
      [File.cwd!(), "priv", "avro_schemas", "test-log", "value.avsc"]
      |> Path.join
      |> File.read!
      |> :eavro.parse_schema

    {:ok, %{schema: %Schema{schema: schema, schema_id: 404}}}
  end
end

