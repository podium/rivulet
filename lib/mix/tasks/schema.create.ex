defmodule Mix.Tasks.Schema.Create do
  use Mix.Task

  alias Rivulet.Avro.Registry
  require Logger

  def run([topic]) do
    Application.ensure_all_started(:httpoison)
    Application.ensure_all_started(:logger)

    schemas_dir = Path.join([File.cwd!(), "priv", "avro_schemas"])
    key_schema = File.read!(Path.join([schemas_dir, topic, "key.avsc"]))
    value_schema = File.read!(Path.join([schemas_dir, topic, "value.avsc"]))
    IO.puts(value_schema)
    IO.puts(%{schema: value_schema} |> Poison.encode!)

    create_schema(topic <> "-key", key_schema)
    create_schema(topic <> "-value", value_schema)
  end

  def create_schema(subject, schema) do
    case Registry.create_schema(subject, schema) do
      {:ok, %HTTPoison.Response{status_code: code} = resp} when code >= 300 ->
        Logger.error("Could not create schema for #{subject} - failed: #{inspect resp}")
        resp
      {:ok, %HTTPoison.Response{status_code: code} = resp} when code <= 299 ->
        Logger.info("Successfully created schema for #{subject}: #{inspect resp}")
        resp
      {:error, reason} ->
        Logger.error("Could not create schema for #{subject} - failed for reason: #{inspect reason}")
    end
  end
end
