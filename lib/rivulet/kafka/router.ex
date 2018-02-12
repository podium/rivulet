defmodule Rivulet.Kafka.Router do
  @partition_strategies [:key, :random]
  defmacro __using__(opts) do
    Module.register_attribute(__CALLER__.module, :streams, accumulate: true)

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

  defp get_stuff(env) do
    check_duplicate_streams(env.module)

    consumer_group = Module.get_attribute(env.module, :consumer_group)

    streams =
      case Module.get_attribute(env.module, :streams) do
        streams when is_list(streams) -> streams
        stream -> [stream]
      end

    stream_topics =
      Enum.map(streams, fn({topic, _routes}) ->
        topic
      end)

    {consumer_group, stream_topics, streams}
  end

  defmacro __before_compile__(env) do
    {consumer_group, stream_topics, streams} = get_stuff(env)

    quote do
      def start_link do
        Rivulet.Kafka.Router.Funcs.start_link(__MODULE__, unquote(consumer_group), unquote(stream_topics))
      end

      def init(_) do
        {:ok, {}}
      end

      def handle_messages(partition, messages, state) do
        Rivulet.Kafka.Router.Funcs.handle_messages(partition, messages, unquote(streams))

        {:ok, :ack, state}
      end
    end
  end

  defmacro defstream(topic, [do: routes]) do
    routes =
      Macro.postwalk(routes, fn
        ({:transformer, _, [transformer, [do: {:__block__, _, publishes}]]}) ->
          [:route, Macro.expand_once(transformer, __ENV__), publishes]
        ({:transformer, _, [transformer, [do: publishes]]}) ->
          [:route, Macro.expand_once(transformer, __ENV__), publishes]
        ({:publish_to, _, [topic, config]}) ->
          partition_strategy = Keyword.get(config, :partition, :key)
          unless partition_strategy in @partition_strategies do
            raise "#{__CALLER__.module} publishing to #{topic} has an unsupported partition strategy: #{partition_strategy}"
          end

          {topic, partition_strategy}
        (node) -> node
      end)

    routes =
      case routes do
        {:__block__, [], routes} -> routes
        routes when is_list(routes) -> routes
        route  -> [route]
      end

    set_stream(__CALLER__.module, {topic, routes})

    nil
  end

  @spec set_stream(module, {topic, [term]})
  :: ignored
  defp set_stream(module, stream) do
    Module.put_attribute(module, :streams, stream)
  end

  @spec check_duplicate_streams(module) :: ignored
  defp check_duplicate_streams(module) do
    require Logger
    case Module.get_attribute(module, :streams) do
      [] ->
        Logger.warn("You didn't define any streams in #{module}. Was this an error?")
      streams when is_list(streams) ->
        {_, dups} =
        Enum.reduce(streams, {[], []}, fn({stream_topic, _}, {topics, dups}) ->
          if stream_topic in topics do
            {topics, [stream_topic | dups]}
          else
            {[stream_topic | topics], dups}
          end
        end)

        case dups do
          [] -> :ok
          duplicates -> raise "The following stream topics were defined multiple times in #{module}: #{inspect duplicates}"
        end

      _single_stream -> :ok
    end
  end
end
