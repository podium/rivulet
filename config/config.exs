use Mix.Config

config :rivulet,
  avro_schema_registry_uri: %URI{scheme: "http", host: "localhost", port: 8081},
  brokers: [{"localhost", 9092}],
  consumer_group: "rivulet",
  kafka_version: "0.10.2",
  use_ssl: false
