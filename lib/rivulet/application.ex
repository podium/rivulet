defmodule Rivulet.Application do
  use Application
  require Logger

  def start(_, _) do
    import Supervisor.Spec

    configure_kafka()
    configure_schema_registry()

    #hostname = System.get_env("HOSTNAME")
    #test_consumer_config =
    #  %Rivulet.Consumer.Config{
    #    client_id: :"rivulet_brod_client-#{hostname}",
    #    consumer_group_name: "consumer_group_name",
    #    topics: ["firehose"],
    #    group_config: [
    #      offset_commit_policy: :commit_to_kafka_v2,
    #      offset_commit_interval_seconds: 5
    #    ],
    #    consumer_config: [begin_offset: :earliest],
    #    message_type: :message_set
    #  }

    children = [
      supervisor(Registry, [:unique, Rivulet.Registry]),
      worker(Rivulet.Avro.Cache, []),
      #worker(Rivulet.TestConsumer, [test_consumer_config])
    ]

    opts = [strategy: :one_for_one]

    Supervisor.start_link(children, opts)
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

  defp configure_kafka do
    Logger.debug("Configuring Kafka")

    kafka_hosts = kafka_brokers()

    hostname = System.get_env("HOSTNAME")
    if System.get_env("MIX_ENV") != "test" do
      :ok = :brod.start_client(kafka_hosts, :"rivulet_brod_client-#{hostname}", _client_config=[auto_start_producers: true])
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
