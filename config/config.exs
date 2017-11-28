use Mix.Config

config :rivulet,
  avro_schema_registry_uri: %URI{scheme: "http", host: "localhost", port: 8081},
  kafka_brokers: [localhost: 9092],
  consumer_group: "rivulet",
  kafka_version: "0.11.0",
  use_ssl: false,
  publish_client_name: :"rivulet_brod_client-#{System.get_env("HOSTNAME")}"
