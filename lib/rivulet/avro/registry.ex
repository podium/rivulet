defmodule Rivulet.Avro.Registry do
  use HTTPoison.Base

  require Logger

  alias Rivulet.Avro
  alias Rivulet.Avro.Schema
  alias Rivulet.Kafka.Partition

  @type version_id :: String.t
  @type subject :: String.t
  @type json :: String.t

  def process_url(path) do
    %URI{} = uri =
      :rivulet
      |> Application.get_env(:avro_schema_registry_uri)
      |> URI.parse

    uri = %URI{uri | path: path}


    uri =
      uri
      |> URI.to_string
      |> URI.encode

    Logger.debug("Making request to Avro Schema Registry: #{uri}")

    uri
  end

  @spec process_response_body(json) :: term
  def process_response_body(body) do
    Poison.decode(body)
  end

  @spec create_schema(subject, json) :: {:ok, Schema.t} | {:error, term}
  def create_schema(subject, schema) when is_binary(subject) and is_binary(schema) do
    Logger.debug("Attempting to create schema for subject: #{subject}")
    post("/subjects/#{subject}/versions", %{schema: schema} |> Poison.encode!, [{"Content-Type", "application/vnd.schemaregistry.v1+json"}])
  end

  @spec get_schema(Avro.schema_id) :: {:ok, Schema.t} | {:error, term}
  def get_schema(schema_id) do
    Logger.debug("Attempting to get schema: #{schema_id}")
    case get("/schemas/ids/#{schema_id}") do
      {:ok, resp} -> handle_get_schema_response(resp, schema_id)
      {:error, reason} = err ->
        Logger.error("Could not get schema: #{schema_id}. Probably couldn't connect to registry. Reason: #{inspect reason}")
        err
    end
  end

  @doc false
  def handle_get_schema_response(%HTTPoison.Response{body: {:ok, json}, status_code: status}, schema_id) when status <= 299 do
    {:ok, schema} =
      json
      |> Map.get("schema")
      |> AvroEx.parse_schema

    Logger.debug("Successfully retrieved and parsed schema: #{schema_id} with status: #{status}")

    {:ok, %Schema{schema_id: schema_id, schema: schema}}
  catch
    :exit, {:badarg, nil} ->
      Logger.error("Connected to registry, but could not parse schema: #{schema_id} Status: #{status}")
      {:error, :schema_not_found}
  end

  def handle_get_schema_response(%HTTPoison.Response{body: {:ok, json}, status_code: status}, _) when status >= 300 do
    Logger.error("Connected to registry, but received status code: #{status}")
    {:error, json["message"]}
  end

  def handle_get_schema_response(%HTTPoison.Response{body: {:error, _} = err}, _) do
    err
  end

  @spec get_version(subject, version_id) :: {:ok, Schema.t} | {:error, term}
  def get_version(subject, version_id) do
    case get("/subjects/#{subject}/versions/#{version_id}") do
      {:ok, resp} ->
        handle_get_version_response(resp)
      {:error, _} = err -> err
    end
  end

  @doc false
  def handle_get_version_response(%HTTPoison.Response{body: {:ok, json}} = resp) do
    schema_id = json["id"]
    handle_get_schema_response(resp, schema_id)
  end

  def handle_get_version_response(%HTTPoison.Response{} = resp) do
    handle_get_schema_response(resp, nil)
  end

  @spec schema_for(Partition.topic) :: {:ok, Schema.t} | {:error, term}
  def schema_for(subject) when is_binary(subject) do
    case get("/subjects/#{subject}/versions") do
      {:ok, %HTTPoison.Response{status_code: status, body: {:ok, json}}} when is_list(json) and length(json) > 0 and status <= 299 ->
        id = List.last(json)
        get_version(subject, id)
      {:ok, %HTTPoison.Response{} = resp} -> {:error, resp}
      {:error, _} = err -> err
    end
  end
end
