defmodule Rivulet.JSON.Test do
  use ExUnit.Case

  @test_module Rivulet.JSON

  defmodule TestJsonHandler do
    def encode(data), do: {:ok, data.test}
    def decode(data), do: {:ok, data.test}
  end

  describe "handler/1" do
    test "if no json_handler is set it should default to jiffy" do
      assert @test_module.handler == Rivulet.JSON.Jiffy
    end
    test "should use test module to encode/decode json" do
      Application.put_env(:rivulet, :json_handler, TestJsonHandler)
      assert @test_module.encode(%{test: :test_encode}) == {:ok, :test_encode}
      assert @test_module.decode(%{test: :test_decode}) == {:ok, :test_decode}
      Application.put_env(:rivulet, :json_handler, Rivulet.JSON.Jiffy)
    end
    test "jiffy should encode correctly" do
      assert @test_module.encode(%{test: :test_encode}) == {:ok, "{\"test\":\"test_encode\"}"}
    end
  end
end
