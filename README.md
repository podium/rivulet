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
        supervisor(Rivulet.Pipeline, ["kafka-topic", MyApp.SomePipeline, [name: MyPipeline]])
      ]

    opts = [strategy: :one_for_one, name: MyApp.Supervisor]

    Supervisor.start_link(children, opts)
  end
end
```

```elixir
defmodule MyApp.SomePipeline do
  use Supervisor

  def start_link(topic, partition_number) do
    Supervisor.start_link(__MODULE__, {topic, partition_number}, name: :"MyApp.SomePipeline.#{topic}.#{inspect partition_number}")
  end

  def init({topic, partition_number})
    children =
      [
        worker(Rivulet.Kafka.Consumer, [topic, partition_number]),
        worker(Rivulet.EventPrinter, [topic, partition_number])
      ]

    opts = [strategy: :one_for_all]

    supervise(children, opts)
  end
end
```

This will create a supervisor (named `MyPipeline`), ask kafka how many
partitions "kafka-topic" has, and start up an instance of `MyApp.SomePipeline`
for each partition under `MyPipeline`'s supervision'.

In this example, `SomePipeline` has two children, which form a GenStage
pipeline: A Kafka producer, and an Event Printer, which simply logs the
messages to STDOUT.

Since your application has full control over MyApp.SomePipeline, you can create
any children workers of any type, with any supervision strategy. All Rivulet is
doing here is creating an instance of this supervisor for each partition in
Kafka, and telling your supervisor which topic and partition number it is
responsible for.

If you use GenStage or Flow, you may create these pipelines in any way you
choose. You have full control.
