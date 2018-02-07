use Mix.Config

config :rivulet,
  avro_schema_registry_uri: %URI{scheme: "http", host: "rivulet_schema-registry_1", port: 8081},
  kafka_brokers: [rivulet_kafka_1: 9092],
  consumer_group: "rivulet",
  kafka_version: "0.11.0",
  use_ssl: false,
  publish_client_name: :"rivulet-client-#{System.get_env("HOSTNAME")}"
