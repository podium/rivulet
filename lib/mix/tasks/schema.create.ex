defmodule Mix.Tasks.Schema.Create do
  use Mix.Task

  alias Rivulet.Avro.Registry
  require Logger

  @shortdoc "POST a k/v schema from priv/avro_schemas to the registry"

  def run([type | subjects]) do
    Application.ensure_all_started(:rivulet)

    schemas_dir = Path.join(["priv", "avro_schemas"])
    key_schema = File.read!(Path.join([schemas_dir, type, "key.avsc"]))
    value_schema = File.read!(Path.join([schemas_dir, type, "value.avsc"]))

    create_subject(key_schema, value_schema, type)
    Enum.each(subjects, &(create_subject(key_schema, value_schema, &1)))
  end

  def create_subject(key_schema, value_schema, subject) do
    with {:ok, _} <- create_schema(subject <> "-key", key_schema),
         {:ok, _} <- create_schema(subject <> "-value", value_schema) do
           :ok
    else
      {:error, _} = err ->
        IO.inspect("Failed for reason: #{inspect err}")
        System.halt(1)
    end
  end

  def create_schema(subject, schema) do
    Logger.info("Creating schema for subject: #{subject}")

    case Registry.create_schema(subject, schema) do
      {:ok, %HTTPoison.Response{status_code: code} = resp} when code >= 300 ->
        Logger.error("Could not create schema for #{subject} - failed: #{inspect resp}")
        {:error, resp}
      {:ok, %HTTPoison.Response{status_code: code}} = resp when code <= 299 ->
        Logger.info("Successfully created schema for #{subject}: #{inspect resp}")
        resp
      {:error, reason} ->
        Logger.error("Could not create schema for #{subject} - failed for reason: #{inspect reason}")
        {:error, reason}
    end
  end
end
