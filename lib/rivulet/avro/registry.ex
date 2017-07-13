defmodule Rivulet.Avro.Registry do
  alias Rivulet.Avro

  use HTTPoison.Base

  @type reason :: :no_schema_provided

  def process_url(path) do
    %URI{} = uri = Application.get_env(:rivulet, :avro_schema_registry_uri)
    uri = %URI{uri | path: path}

    uri
    |> URI.to_string
    |> URI.encode
  end

  @spec process_response_body(Poison.t) :: {:ok, Avro.schema} | {:error, reason}
  def process_response_body(body) do
    schema =
      body
      |> Poison.decode!
      |> Map.get("schema")
      |> :eavro.parse_schema

    {:ok, schema}
  catch
    :exit, {:badarg, nil} -> {:error, :no_schema_provided}
  end

  @spec get_schema(Avro.schema_id) :: {:ok, Avro.schema} | {:error, reason}
  def get_schema(schema_id) do
    with {:ok, %HTTPoison.Response{body: {:ok, schema}}} <- get("/schemas/ids/#{schema_id}") do
      {:ok, schema}
    else
      {:ok, %HTTPoison.Response{body: {:error, _reason} = err}} -> err
      {:error, _} = err -> err
    end
  end

  def schema do
    """
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
  end
end
