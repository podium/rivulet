defmodule Rivulet.Application do
  use Application
  require Logger

  def start(_, _) do
    import Supervisor.Spec

    configure_kafka()
    configure_schema_registry()

    children = [
      supervisor(Registry, [:unique, Rivulet.Registry]),
      worker(Rivulet.Avro.Cache, []),
      supervisor(Task.Supervisor, [[name: Task.Supervisor, restart: :transient]]),
      worker(Rivulet.TestRouter, []),
      #worker(Rivulet.TestConsumer, [test_consumer_config])
    ]

    opts = [strategy: :one_for_one]

    Supervisor.start_link(children, opts)
  end

  defp configure_kafka do
    Logger.debug("Configuring Kafka")

    kafka_hosts = kafka_brokers()

    if System.get_env("MIX_ENV") != "test" do
      client_name = Application.get_env(:rivulet, :publish_client_name)
      unless client_name do
        raise "Application.get_env(:rivulet, :publish_client_name) not configured"
      end
      :ok = :brod.start_client(kafka_hosts, client_name, _client_config=[auto_start_producers: true])
    else
      Logger.info("Test Environment detected - not starting :brod")
    end
  end

  def kafka_brokers do
    config = Application.get_all_env(:rivulet)

    if Keyword.get(config, :dynamic_hosts) do
      case System.get_env("KAFKA_HOSTS") do
        nil -> Logger.error("KAFKA_HOSTS not set")
        value -> kafka_hosts(value)
      end
    else
      Application.get_env(:rivulet, :kafka_brokers)
    end
  end

  defp configure_schema_registry() do
    case Application.get_env(:rivulet, :avro_schema_registry_uri) do
      {:system, var} ->
        Application.put_env(:rivulet, :avro_schema_registry_uri, System.get_env(var))
      val when is_binary(val) ->
        :ok
      %URI{} ->
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
             {String.to_atom(host), String.to_integer(port)}
           [host] ->
             {String.to_atom(host), 9092}
         end
       end)
  end
end
