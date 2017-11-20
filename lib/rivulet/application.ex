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
      #supervisor(KafkaEx.ConsumerGroup, [Rivulet.TestConsumer, "rivulet", ["firehose"], [heartbeat_interval: :timer.seconds(1), commit_interval: 1000]])
    ]


    opts = [strategy: :one_for_one]

    Supervisor.start_link(children, opts)
  end

  defp configure_kafka do
    Logger.debug("Configuring Kafka")
    config = Application.get_all_env(:rivulet)

    kafka_hosts =
      if Keyword.get(config, :dynamic_hosts) do
        case System.get_env("KAFKA_HOSTS") do
          nil -> Logger.error("KAFKA_HOSTS not set")
          value -> kafka_hosts(value)
        end
      else
        Application.get_env(:rivulet, :kafka_brokers)
      end

    # config
    # |> Enum.reverse
    # |> Enum.each(fn({k, v}) ->
    #      # Application.put_env(:kafka_ex, k, v, persistent: true)
    #    end)

    # Logger.debug("Kafka should be configured to: #{inspect config}")
    # Logger.debug("Kafka config: #{inspect Application.get_all_env(:kafka_ex)}")

    if System.get_env("MIX_ENV") != "test" do
      :ok = :brod.start_client(kafka_hosts, :rivulet_brod_client, _client_config=[auto_start_producers: true])
    else
      Logger.info("Test Environment detected - not starting :brod")
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
