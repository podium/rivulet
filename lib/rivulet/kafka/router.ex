defmodule Rivulet.Kafka.Router do
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
        Rivulet.Kafka.Router.Funcs.start_link(__MODULE__, unquote(consumer_group), unquote(source_topics))
      end

      def init(_) do
        {:ok, {}}
      end

      def handle_messages(partition, messages, state) do
        Rivulet.Kafka.Router.Funcs.handle_messages(partition, messages, unquote(sources))

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
