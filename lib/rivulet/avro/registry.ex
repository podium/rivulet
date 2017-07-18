defmodule Rivulet.Avro.Registry do
  alias Rivulet.Avro
  alias Rivulet.Avro.Schema

  use HTTPoison.Base

  @type version_id :: String.t
  @type subject :: String.t

  def process_url(path) do
    %URI{} = uri = Application.get_env(:rivulet, :avro_schema_registry_uri)
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
    with {:ok, %HTTPoison.Response{body: {:ok, json}}} <- get("/schemas/ids/#{schema_id}") do
      schema =
        json
        |> Map.get("schema")
        |> :eavro.parse_schema

      {:ok, %Schema{schema_id: schema_id, schema: schema}}
    else
      {:ok, %HTTPoison.Response{body: {:error, _reason} = err}} -> err
      {:error, _} = err -> err
    end
  catch
    :exit, {:badarg, nil} -> {:error, :no_schema_provided}
  end

  @spec get_version(subject, version_id) :: {:ok, Schema.t} | {:error, term}
  def get_version(subject, version_id) do
    with {:ok, %HTTPoison.Response{body: {:ok, json}}} <- get("/subjects/#{subject}/versions/#{version_id}") do
      schema =
        json
        |> Map.get("schema")
        |> :eavro.parse_schema

      schema_id = json["id"]

      {:ok, %Schema{schema_id: schema_id, schema: schema}}
    else
      {:ok, %HTTPoison.Response{body: {:error, _reason} = err}} -> err
      {:error, _} = err -> err
    end
  catch
    :exit, {:badarg, nil} -> {:error, :no_schema_provided}
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
    with {:ok, %HTTPoison.Response{body: {:ok, json}}} when is_list(json) and length(json) > 0 <- get("/subjects/#{subject}/versions") do
      id = List.last(json)

      get_version(subject, id)
    end
  end
end
