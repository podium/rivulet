defmodule Rivulet.Avro.Test do
  use ExUnit.Case

  @test_module Rivulet.Avro

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
end
