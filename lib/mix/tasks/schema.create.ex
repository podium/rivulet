defmodule Mix.Tasks.Schema.Create do
  use Mix.Task

  alias Rivulet.Avro.Registry
  require Logger

  @shortdoc "POST a k/v schema from priv/avro_schemas to the registry"

  def run([topic]) do
    Application.ensure_all_started(:rivulet)

    schemas_dir = Path.join(["priv", "avro_schemas"])
    key_schema = File.read!(Path.join([schemas_dir, topic, "key.avsc"]))
    value_schema = File.read!(Path.join([schemas_dir, topic, "value.avsc"]))

    with {:ok, _} <- create_schema(topic <> "-key", key_schema),
         {:ok, _} <- create_schema(topic <> "-value", value_schema) do
           :ok
    else
      {:error, _} -> System.halt(1)
    end
  end

  def create_schema(subject, schema) do
    Logger.info("Creating schema for subjejct: #{subject}")

    case Registry.create_schema(subject, schema) do
      {:ok, %HTTPoison.Response{status_code: code}} = resp when code >= 300 ->
        Logger.error("Could not create schema for #{subject} - failed: #{inspect resp}")
        resp
      {:ok, %HTTPoison.Response{status_code: code}} = resp when code <= 299 ->
        Logger.info("Successfully created schema for #{subject}: #{inspect resp}")
        resp
      {:error, reason} ->
        Logger.error("Could not create schema for #{subject} - failed for reason: #{inspect reason}")
        {:error, reason}
    end
  end
end
