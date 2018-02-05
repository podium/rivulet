defmodule Rivulet.Router do
  defmacro __using__(opts) do
    Module.register_attribute(__CALLER__.module, :sources, accumulate: true)

    consumer_group = Keyword.get(opts, :consumer_group)

    unless consumer_group do
      raise """


      \tConsumer Group not configured for #{__CALLER__.module}.
      \tPlease configure in your `use` statement.

      \tExample:

      ```
      \tdefmodule MyModule do
      \t  use #{__MODULE__},
      \t    consumer_group: "my-consumer-group"
      \tend
      \t```

      """
    end

    Module.put_attribute(__CALLER__.module, :consumer_group, consumer_group)

    quote do
      import unquote(__MODULE__)
      alias unquote(__MODULE__)
      @before_compile unquote(__MODULE__)
    end
  end

  @type ignored :: term
  @type topic :: String.t
  @type transformer :: module

  defmacro __before_compile__(env) do
    check_duplicate_sources(env.module)

    consumer_group = Module.get_attribute(env.module, :consumer_group)
    sources =
      case Module.get_attribute(env.module, :sources) do
        sources when is_list(sources) -> sources
        source -> [source]
      end

    source_topics =
      Enum.map(sources, fn({topic, _routes}) ->
        topic
      end)

    quote do
      def start_link do
        require Logger
        config =
          %Rivulet.Consumer.Config{
            client_id: Application.get_env(:rivulet, :publish_client_name),
            consumer_group_name: unquote(consumer_group),
            topics: unquote(source_topics),
            group_config: [
              offset_commit_policy: :commit_to_kafka_v2,
              offset_commit_interval_secons: 1
            ],
            consumer_config: [begin_offset: :earliest],
            message_type: :message_set
          }

        Logger.info("Configuration for #{__MODULE__}: #{inspect config}")

        Rivulet.Consumer.start_link(__MODULE__, config)
      end

      def init(_) do
        {:ok, {}}
      end

      def handle_messages(%Rivulet.Kafka.Partition{topic: topic} = partition, messages, state) do
        require Logger
        {^topic, routes} =
          Enum.find(unquote(sources), fn
            ({^topic, _}) -> true
            (_) -> false
          end)

        routes =
          case routes do
            [:route | _] = route -> [route]
            routes when is_list(routes) -> routes
          end

        Enum.map(routes, fn([:route, transformer, publish_topics]) ->
          transformed_messages =
            Enum.map(messages, fn(message) ->
              case transformer.handle_message(message) do
                nil -> nil
                {key, value} = m ->
                  %Rivulet.Kafka.Publisher.Message{
                    key: key,
                    value: value,
                    encoding_strategy: :raw,
                    topic: :unknown,
                    partition_strategy: :unknown
                  }
                messages when is_list(messages) -> messages
                other ->
                  Logger.error("#{transformer}.handle_message returned #{inspect other}, which is an unsupported type.")
                  nil
              end
            end)

          publish_messages =
            transformed_messages
            |> List.flatten
            |> Enum.reject(fn
              (nil) -> true
              (_) -> false
            end)

          publish_topics =
            case publish_topics do
              topics when is_list(topics) -> topics
              {topic, partition_strategy} when is_binary(topic) -> [{topic, partition_strategy}]
            end

          Enum.each(publish_topics, fn
            ({publish_topic, :key}) ->
              publish_messages
              |> Enum.map(fn(%Rivulet.Kafka.Publisher.Message{} = m) ->
                %Rivulet.Kafka.Publisher.Message{m | topic: publish_topic, partition_strategy: {:key, m.key}}
              end)
              |> Rivulet.Kafka.Publisher.publish
            ({publish_topic, :random}) ->
              publish_messages
              |> Enum.map(fn(%Rivulet.Kafka.Publisher.Message{} = m) ->
                %Rivulet.Kafka.Publisher.Message{m | topic: publish_topic, partition_strategy: :random}
              end)
              |> Rivulet.Kafka.Publisher.publish
          end)
        end)

        {:ok, :ack, state}
      end
    end
  end

  defmacro defsource(topic, [do: routes]) do
    routes =
      Macro.postwalk(routes, fn
        ({:transformer, _, [transformer, [do: {:__block__, _, publishes}]]}) ->
          [:route, Macro.expand_once(transformer, __ENV__), publishes]
        ({:transformer, _, [transformer, [do: publishes]]}) ->
          [:route, Macro.expand_once(transformer, __ENV__), publishes]
        ({:publish_to, _, [topic, [partition: :key]]}) ->
          {topic, :key}
        ({:publish_to, _, [topic, [partition: :random]]}) ->
          {topic, :random}
        (node) -> node
      end)

    routes =
      case routes do
        {:__block__, [], routes} -> routes
        routes when is_list(routes) -> routes
        route  -> [route]
      end

    set_source(__CALLER__.module, {topic, routes})

    nil
  end

  @spec set_source(module, {topic, [term]})
  :: ignored
  defp set_source(module, source) do
    Module.put_attribute(module, :sources, source)
  end

  @spec check_duplicate_sources(module) :: ignored
  defp check_duplicate_sources(module) do
    require Logger
    case Module.get_attribute(module, :sources) do
      [] ->
        Logger.warn("You didn't define any sources in #{module}. Was this an error?")
      sources when is_list(sources) ->
        {_, dups} =
        Enum.reduce(sources, {[], []}, fn({source_topic, _}, {topics, dups}) ->
          if source_topic in topics do
            {topics, [source_topic | dups]}
          else
            {[source_topic | topics], dups}
          end
        end)

        case dups do
          [] -> :ok
          duplicates -> raise "The following source topics were defined multiple times in #{module}: #{inspect duplicates}"
        end

      _single_source -> :ok
    end
  end
end
