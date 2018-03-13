defmodule Rivulet.Kafka.Join do
  require Logger

  defmacro __using__(opts) do
    Module.register_attribute(__CALLER__.module, :streams, accumulate: true)
    Module.register_attribute(__CALLER__.module, :transformers, accumulate: true)

    consumer_group = Keyword.get(opts, :consumer_group)

    unless consumer_group do
      raise """


      \tConsumer Group not configured for #{__CALLER__.module}.
      \tPlease configure in your `use` statement.

      \tExample:

      \t```
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
    consumer_group = Module.get_attribute(env.module, :consumer_group)
    streams = Module.get_attribute(__CALLER__.module, :streams, [])
    transformers = Module.get_attribute(env.module, :transformers)

    case streams do
      [] -> raise "#{__CALLER__.module} did not define any streams"
      {_topic, _module} -> raise "#{__CALLER__.module} only defined a single stream"
      streams when is_list(streams) ->
        {_, duplicated} =
          streams
          |> Enum.reduce({[], []}, fn({topic, _}, {defined, duplicated}) ->
            if topic in defined,
            do: {defined, [topic | duplicated]},
            else: {[topic | defined], duplicated}
          end)

        case duplicated do
          [] -> :ok
          dups ->
            raise "#{env.module} is overwriting the following streams: #{inspect dups}"
        end
    end

    topics = Enum.map(streams, fn({topic, _module}) -> topic end)
    join_id = Module.get_attribute(env.module, :join_id)

    quote do
      def start_link do
        Rivulet.Kafka.Join.Funcs.start_link(__MODULE__, unquote(consumer_group), unquote(topics))
      end

      def init(_) do
        Rivulet.Join.ElasticSearch.create_join_index(unquote(join_id))
        {:ok, handler} = Rivulet.Join.Handler.start_link(unquote(join_id), unquote(transformers), self())
        {:ok, batcher} = Rivulet.Join.Batcher.start_link(handler)

        {:ok, {handler, batcher, unquote(streams)}}
      end

      def handle_messages(partition, messages, {handler, batcher, streams} = state) do
        Rivulet.Kafka.Join.Funcs.handle_messages(partition, messages, streams, unquote(join_id), batcher)

        {:ok, state}
      end
    end
  end

  defmacro join_id(join_id) when is_binary(join_id) do
    set_join_id(__CALLER__.module, join_id)
  end

  defmacro stream(topic, [key_on: module]) when is_binary(topic) and is_atom(module) do
    Module.put_attribute(__CALLER__.module, :streams, {topic, module})
  end

  defmacro stream(topic, [key_on: {:__aliases__, _, _segments} = module]) when is_binary(topic) do
    module = Macro.expand_once(module, __ENV__)
    Module.put_attribute(__CALLER__.module, :streams, {topic, module})
  end

  defmacro join_transformer(module, [do: publishes]) do
    module = Macro.expand_once(module, __CALLER__)
    case Module.get_attribute(__CALLER__.module, :transformers) do
      [] -> :ok
      {^module, _} -> raise "#{__CALLER__.module} can't redefine transformer: #{module}"
      modules ->
        if Enum.find(modules, fn
          ({^module, _}) -> true
          (_) -> false
        end) do
          raise "#{__CALLER__.module} can't redefine transformer: #{module}"
        end
    end
    res =
      Macro.postwalk(publishes, fn
        ({:publish_to, _, [topic, [partition: partition_strategy]]}) when partition_strategy in [:key, :random] ->
          {topic, partition_strategy}
        ({:publish_to, _, [topic, partition: partition_strategy]}) ->
          raise "#{__CALLER__.module} publishing to #{topic} has an unsupported partition strategy: #{partition_strategy}"
        ({:publish_to, _, _}) ->
          raise "#{__CALLER__.module} publish_to requires a valid partition_strategy"
        ({:__block__, _, block}) -> block
        (ast) -> ast
      end)

    publishes =
      case res do
        list when is_list(list) -> list
        other -> [other]
      end

    {_, dups} =
     Enum.reduce(publishes, {[], []}, fn({topic, _partition_strategy}, {topics, dups} = acc) ->
        cond do
          topic in topics  and topic in dups ->
            acc
          topic in topics ->
            {topics, [topic | dups]}
          true ->
            {[topic | topics], dups}
        end
      end)

    case dups do
      [] -> :ok
      _ -> raise """


        \t#{__CALLER__.module} is attempting to publish to topics: #{inspect dups} multiple times from transformer: #{module}. That's probably a mistake.

        """
    end

    Module.put_attribute(__CALLER__.module, :transformers, {module, publishes})
  end

  @spec set_join_id(module, binary)
  :: ignored
  defp set_join_id(module, join_id) do
    already_defined = Module.get_attribute(module, :join_id, nil)
    if already_defined, do: raise "#{module} has already defined a join_id"
    Module.put_attribute(module, :join_id, join_id)
  end
end
