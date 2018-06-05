# Rivulet

Stream Processing for Elixir

## Installation

In your project `mix.exs`:

```elixir
def deps do
  [{:rivulet, git: "git@github.com:podium/rivulet"}]
end
```

And in your `config/config.exs`, add:

```
hostname = System.get_env("HOSTNAME")

config :rivulet,
  avro_schema_registry_uri: {:system, "SCHEMA_REGISTRY_URL"},
  client_name: :"my-app-#{hostname}",
  dynamic_hosts: true,
  default_max_bytes: get_int_env.("MY_APP_MAX_BYTES", 100_000)
```

An example .env for such an app:

```
SCHEMA_REGISTRY_URL=http://localhost:8081
KAFKA_HOSTS=localhost:9092
HOSTNAME=abc123
```

## Usage

```elixir
defmodule MyApp.Application do
  use Application

  def start(_, _) do
    children =
      [
        supervisor(KafkaEx.ConsumerGroup, [
          Rivulet.TestConsumer,
          "rivulet",
          ["firehose"],
          [heartbeat_interval: :timer.seconds(1), commit_interval: 1000]
        ])
      ]

    opts = [strategy: :one_for_one, name: MyApp.Supervisor]

    Supervisor.start_link(children, opts)
  end
end
```

```elixir
defmodule MyApp.SomeConsumer do
  use KafkaEx.GenConsumer

  def init(topic, partition) do
    state = ...
    {:ok, state}
  end

  def handle_message_set(messages, state) do
    # Process messages

    {:async_commit, state}
  end
end
```
