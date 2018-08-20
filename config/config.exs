use Mix.Config

config :rivulet,
  avro_schema_registry_uri: %URI{scheme: "http", host: "rivulet_schema-registry_1", port: 8081},
  kafka_brokers: [rivulet_kafka_1: 9092],
  client_name: :"rivulet-client-#{System.get_env("HOSTNAME")}",
  json_handler: Rivulet.JSON.Jiffy

# Configure elastix
config :elastix,
  poison_options: [keys: :atoms],
  httpoison_options: [hackney: [pool: :elastix_pool]]
