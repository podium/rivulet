defmodule Rivulet.Avro.Registry.Test do
  use ExUnit.Case, async: false

  @test_module Rivulet.Avro.Registry

  alias Rivulet.Avro.Schema

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

    test "uses Application.get_env and the given path" do
      assert "http://localhost:65000/hello" = @test_module.process_url("/hello")
    end
  end

  describe "process_response_body" do
    test "returns an ok tuple on success" do
      assert {:ok, _} = @test_module.process_response_body(%{schema: @schema} |> Poison.encode!)
    end

    test "returns an error tuple on failure" do
      assert {:error, _} = @test_module.process_response_body("wat")
    end
  end

  describe "get_schema" do
    setup [:mock_responses]

    test "returns a %Schema{} on success", %{success: resp, schema_id: schema_id} do
      assert {:ok, %Schema{schema_id: ^schema_id}} = @test_module.handle_get_schema_response(resp, schema_id)
    end

    test "returns an error if error status code > 299", %{not_found: resp, schema_id: schema_id} do
      assert {:error, _} = @test_module.handle_get_schema_response(resp, schema_id)
    end

    test "returns the reason json decoding failed if decoding fails", %{json_failed: resp, schema_id: schema_id} do
      assert {:error, :reason} = @test_module.handle_get_schema_response(resp, schema_id)
    end
  end

  describe "get version" do
    setup [:mock_responses]

    test "returns a %Schema{} on success", %{success: resp} do
      assert {:ok, %Schema{}} = @test_module.handle_get_version_response(resp)
    end

    test "returns an error if error status code > 299", %{not_found: resp} do
      assert {:error, _} = @test_module.handle_get_version_response(resp)
    end

    test "returns the reason json decoding failed if decoding fails", %{json_failed: resp} do
      assert {:error, :reason} = @test_module.handle_get_version_response(resp)
    end
  end

  def mock_responses(_) do
    success = %HTTPoison.Response{status_code: 200, body: {:ok, %{"schema" => @schema}}}
    not_found = %HTTPoison.Response{status_code: 404, body: {:ok, %{"error_code" => 40402, "message" => "Version not found."}}}
    json_failed = %HTTPoison.Response{status_code: 404, body: {:error, :reason}}

    {:ok, %{success: success, not_found: not_found, json_failed: json_failed, schema_id: 404}}
  end
end
