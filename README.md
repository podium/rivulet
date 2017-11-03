# Rivulet

Stream Processing for Elixir

## Installation

In your project `mix.exs`:

```elixir
def deps do
  [{:rivulet, git: "git@github.com:podium/rivulet"}]
end
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
