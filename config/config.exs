use Mix.Config

config :kafka_ex,
  brokers: [{"localhost", 9092}],
  consumer_group: "rivulet",
  kafka_version: "0.10.2",
  use_ssl: false
