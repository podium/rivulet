defmodule Rivulet.Application do
  use Application
  require Logger

  def start(_, _) do
    import Supervisor.Spec

    configure_kafka()
    configure_schema_registry()

    children = [
      supervisor(Registry, [:unique, Rivulet.Registry]),
      worker(Rivulet.Avro.Cache, [])
      #worker(Rivulet.Pipeline.Supervisor, ["test-log", Rivulet.TestPipeline, TestPipeline]) # For testing
    ]

    opts = [strategy: :one_for_one]

    Supervisor.start_link(children, opts)
  end

  defp configure_kafka do
    Logger.debug("Configuring Kafka")
    config = Application.get_all_env(:rivulet)

    config =
      if Keyword.get(config, :dynamic_hosts) do
        hosts =
          case System.get_env("KAFKA_HOSTS") do
            nil -> Logger.error("KAFKA_HOSTS not set")
            value -> kafka_hosts(value)
          end

        Keyword.put(config, :brokers, hosts)
      else
        config
      end

    config
    |> Enum.reverse
    |> Enum.each(fn({k, v}) ->
         Application.put_env(:kafka_ex, k, v, persistent: true)
       end)

    Logger.debug("Kafka should be configured to: #{inspect config}")
    Logger.debug("Kafka config: #{inspect Application.get_all_env(:kafka_ex)}")

    if Mix.env != :test do
      Logger.info("Starting KafkaEx")
      Application.ensure_all_started(:kafka_ex)
    else
      Logger.info("Test Environment detected - not starting :kafka_ex")
    end
  end

  defp configure_schema_registry() do
    case Application.get_env(:rivulet, :avro_schema_registry_uri) do
      {:system, var} ->
        Application.put_env(:rivulet, :avro_schema_registry_uri, System.get_env(var))
      val when is_binary(val) ->
        :ok
      any ->
        Logger.warn("Got value: #{inspect any} for schema registry url")
    end
  end

  defp kafka_hosts(string) do
    string
    |> String.split(",")
    |> Enum.map(fn(host_string) ->
         case String.split(host_string, ":") do
           [host, port] ->
             {host, String.to_integer(port)}
           [host] ->
             {host, 9092}
         end
       end)
  end
end
