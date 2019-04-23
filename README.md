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
  client_name: :"<my-app>-#{hostname}",
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

### Tutorial

For an in-depth tutorial, see https://stackoverflow.com/c/podium/a/176

### Publishing

Publishing to Kafka is done using Rivulet.Kafka.Publisher.
If you're publishing a single message, you would probably use `publish/5`.
If you're doing a bulk publish, `publish/1` accepts a list of
`%Rivulet.Kafka.Publisher.Message{}` structs.

`publish/5` accepts the following arguments:

1. A topic (string). Example: "platform_locations"
1. A partition strategy. Can be one of the following:
  - `:random` to publish onto a random partition
  - `{:key, key_string}` will publish anything with the same key onto the same partition.
  - An integer, to publish directly onto a specific partition. This is not recommended.
1. An encoding strategy. Can be one of the following:
  - `:avro`, which will attempt to pull an avro schema from the schema registry and encode the message and its key using that schema.
  - `:json`, which will attempt to JSON encode the given message and its key. By default, Rivulet will attempt to encode the data using `:jiffy`, which is extremely fast but doesn't handle Elixir Structs the way Poison does. If Poison is desired, you can configure Rivulet to use Poison instead of Jiffy.
  - `:raw` will simply accept raw binaries (including strings) and publish them as-is.
1. A key, which is used to identify the record being published
1. A value, which is the message being published.

### Raw Consumer

```elixir
defmodule MyApp.Application do
  use Application

  def start(_, _) do
    children =
      [
        worker(MyApp.SomeConsumer, [])
      ]

    opts = [strategy: :one_for_one, name: MyApp.Supervisor]

    Supervisor.start_link(children, opts)
  end
end
```

```elixir
defmodule MyApp.SomeConsumer do
  @behaviour Rivulet.Kafka.Consumer

  alias Rivulet.Kafka.{Consumer.Config, Consumer.Message, Partition}

  @type state :: term # as returned by your `init/1` function

  def start_link(any, args, you, want) do
    config = %Config{} = build_config(any, args, you, want)

    # This will be passed to your `init/1` function
    init_callback_data = {anything, you, want}
    Rivulet.Consumer.start_link(__MODULE__, config, init_callback_data)
  end

  # Build a config struct in this function. This is only an example - adjust as
  # necessary.
  defp build_config(any, args, you, want) do
  %Config{
    client_id: Rivulet.client_name(),
    consumer_group_name: "your-consumer-group",
    topics: ["topic-a", "topic-b", "topic-c"]
  }
  end

  def init(init_callback_data) do
    state = ...
    {:ok, state}
  end

  @spec handle_messages(Partition.t, [Message.t], state)
  :: {:ok, state}
  | {:ok, :ack, state}
  def handle_messages(%Partition{} = partition, messages, state) when is_list(messages) do

    # Process your stuff

    # If you want to manually ack your message set later, return:
    #   {:ok, state}
    # If you want the consumer to automatically ack your offset, return:
    #   {:ok, :ack, state}

    # If acking your offset manually, you can call:
    #   Rivulet.Consumer.ack(consumer_pid, partition, offset)
  end
end
```

### Router

If you want to take things off a topic, transform them, and put them onto
another topic, the Router abstraction is your best friend.

A router

```elixir
defmodule MyApp.Application do
  use Application

  def start(_, _) do
    children =
      [
        worker(MyApp.MyRouter, [])
      ]

    opts = [strategy: :one_for_one, name: MyApp.Supervisor]

    Supervisor.start_link(children, opts)
  end
end
```

```elixir
defmodule MyApp.MyRouter do
  use Rivulet.Kafka.Router, consumer_group: "my-consumer-group"

  defstream "source-topic" do
    transformer MyApp.MyTransformer do
      publish_to: "destination-topic-1", partition: :key
    end

    transformer MyApp.AnotherTransformer do
      publish_to: "destination-topic-2", partition: :random
    end

    transformer Rivulet.Trasnformer.Inspect do
      # If this block is empty, the message is handled by the transformer, but
      # Not published to any other topic. Useful for sinks and other
      # side-effecty transformers.
    end
  end

  defstream
end
```

A Transformer is a module which has a handle_message/1 function:

```elixir
defmodule MyApp.MyTransformer do
  use Rivulet.Transformer

  alias Rivulet.Kafka.Consumer.Message

  @type key :: String.t
  @type value :: String.t

  @spec handle_message(Message.t)
  :: nil # publish nothing
  | [] # publish nothing
  | {key, value} # Publishes a message with the given key & value
  | [{key, value}] # Publishes a list of messages (in order) with the given key/value pairs
  | [{key, value} | nil] # Publishes a list of messages (in order) with the given key/value pairs, filtering out the `nil` elements.
  def handle_message(%Message{} = m) do
    # Process the message
  end
end
```

Rivulet comes with a few pre-built transformers:

- Rivulet.Transformer.AvroInspect, which decodes an avro message logs it out, and returns `nil`
- Rivulet.Transformer.Inspect, which logs out the key & value of a message and returns `nil`
- Rivulet.Transformer.PassThrough, which does nothing but return the message unmodified. Surprisingly useful when debugging.

### SQL Sink

If you have a topic of records you'd like inserted into a database, the SQLSink
is your goto abstraction.

```elixir
defmodule MyApp.Application do
  use Application

  def start(_, _) do
    children =
      [
        worker(MyApp.MySQLSink, [])
      ]

    opts = [strategy: :one_for_one, name: MyApp.Supervisor]

    Supervisor.start_link(children, opts)
  end
end
```

```elixir
defmodule MyApp.SomeSQLSink do
  def start_link do
    topic = "..."

    Rivulet.SQLSink.start_link([
      consumer_group: "my-app-#{topic}-sink",
      delete_key_field: :uid, # Allows the sink to delete rows based on the Message Key
      primary_keys: :sequence, # Can also be a list of fields ["field1", "field2"]
      repo: MyApp.MyRepo,
      table_pattern: "$$_sink", # $$ is replaced with the topic name. You can also just put the name of a table here.
      topic: topic, # Kafka Topic to pull from
      whitelist: ["fields", "you", "want", "whitelisted"]
    ])
  end
end
```

Note that you should create the table in your apps' migrations before running
the sink.
