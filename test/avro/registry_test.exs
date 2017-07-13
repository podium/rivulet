defmodule Rivulet.Avro.Registry.Test do
  use ExUnit.Case, async: false

  @test_module Rivulet.Avro.Registry

  @schema """
{
     "type": "record",
     "namespace": "com.example",
     "name": "FullName",
     "fields": [
       { "name": "first", "type": "string" },
       { "name": "last", "type": "string" }
     ]
} 
  """

  setup do
    uri = %URI{scheme: "http", host: "localhost", port: 65000}
    Application.put_env(:rivulet, :avro_schema_registry_uri, uri)

    {:ok,%{base_uri: uri}}
  end

  describe "process_url" do
    test "returns a string" do
      url = @test_module.process_url("/hello")
      assert is_binary(url)
    end

    test "is based on the base_uri and the passed in path" do
      assert "http://localhost:65000/hello" = @test_module.process_url("/hello")
    end
  end

  describe "process_response_body" do
    test "fails if schema isn't wrapped in a schema key" do
      assert {:error, :no_schema_provided} = @test_module.process_response_body(@schema)
    end

    test "returns an ok tuple on success" do
      assert {:ok, _} = @test_module.process_response_body(%{schema: @schema} |> Poison.encode!)
    end
  end
end
