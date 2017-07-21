defmodule Rivulet.Avro.Registry do
  alias Rivulet.Avro
  alias Rivulet.Avro.Schema

  use HTTPoison.Base

  @type version_id :: String.t
  @type subject :: String.t

  def process_url(path) do
    %URI{} = uri =
      :rivulet
      |> Application.get_env(:avro_schema_registry_uri)
      |> URI.parse

    uri = %URI{uri | path: path}

    uri
    |> URI.to_string
    |> URI.encode
  end

  @spec process_response_body(Poison.t) :: term
  def process_response_body(body) do
    Poison.decode(body)
  end

  @spec create_schema(subject, Poison.t) :: {:ok, Schema.t} | {:error, term}
  def create_schema(subject, schema) when is_binary(subject) and is_binary(schema) do
    post("/subjects/#{subject}/versions", %{schema: schema} |> Poison.encode!, [{"Content-Type", "application/vnd.schemaregistry.v1+json"}])
  end

  @spec get_schema(Avro.schema_id) :: {:ok, Schema.t} | {:error, term}
  def get_schema(schema_id) do
    case get("/schemas/ids/#{schema_id}") do
      {:ok, resp} -> handle_get_schema_response(resp, schema_id)
      {:error, _} = err -> err
    end
  end

  @doc false
  def handle_get_schema_response(%HTTPoison.Response{body: {:ok, json}, status_code: status}, schema_id) when status <= 299 do
    schema =
      json
      |> Map.get("schema")
      |> :eavro.parse_schema

    {:ok, %Schema{schema_id: schema_id, schema: schema}}
  catch
    :exit, {:badarg, nil} -> {:error, :schema_not_found}
  end

  def handle_get_schema_response(%HTTPoison.Response{body: {:ok, json}, status_code: status}, _) when status >= 300 do
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

  @spec schema_for(Partition.topic) :: %{key: Schema.t, value: Schema.t} | {:error, term}
  def schema_for(topic) when is_binary(topic) do
    with {:ok, %Schema{} = key} <- schema_for(topic, :key),
         {:ok, %Schema{} = value} <- schema_for(topic, :value) do
      %{key: key, value: value}
    end
  end

  @spec schema_for(Partition.topic, :key | :value) :: {:ok, Schema.t} | {:error, term}
  def schema_for(topic, k_or_v) when is_binary(topic) and k_or_v == :key or k_or_v == :value do
    subject = "#{topic}-#{k_or_v}"

    case get("/subjects/#{subject}/versions") do
      {:ok, %HTTPoison.Response{status_code: status, body: {:ok, json}}} when is_list(json) and length(json) > 0 and status <= 299 ->
        id = List.last(json)
        get_version(subject, id)
      {:ok, %HTTPoison.Response{} = resp} -> {:error, resp}
      {:error, _} = err -> err
    end
  end
end
