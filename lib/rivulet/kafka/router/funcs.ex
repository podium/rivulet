defmodule Rivulet.Kafka.Router.Funcs do
  require Logger
  alias Rivulet.Kafka.Publisher.Message

  defp to_list(list) when is_list(list), do: list
  defp to_list(other), do: [other]

  def start_link(module, consumer_group, source_topics) do
    config =
      %Rivulet.Consumer.Config{
        client_id: Rivulet.client_name(),
        consumer_group_name: consumer_group,
        topics: source_topics,
        group_config: [
          offset_commit_policy: :commit_to_kafka_v2,
          offset_commit_interval_secons: 1
        ],
        consumer_config: [
          begin_offset: :earliest,
          max_bytes: Rivulet.Config.max_bytes()
        ]
      }

    Logger.info("Configuration for #{module}: #{inspect config}")

    Rivulet.Consumer.start_link(module, config)
  end

  defp to_publish(nil), do: nil
  defp to_publish({k, v}) when is_binary(k) and is_binary(v) do
    %Rivulet.Kafka.Publisher.Message{
      key: k,
      value: v,
      encoding_strategy: :raw,
      topic: :unknown,
      partition_strategy: :unknown
    }
  end

  defp to_publish(other) do
    Logger.error("handle_message returned #{inspect other}, which is an unsupported type.")
    :error
  end

  def handle_messages(%Rivulet.Kafka.Partition{topic: topic}, messages, sources) do
    routes =
      topic
      |> routes_for_topic(sources)
      |> listify_routes

    Enum.map(routes, fn([:route, transformer, publish_topics]) ->
      transformed_messages = transform(messages, transformer)

      case :error in transformed_messages do
        true ->
          Logger.error("Could not publish messages because transformer returned an error. Moving on.")
        false ->
          publish_topics
          |> listify_publish_topics
          |> publish_transformed_messages(transformed_messages)
      end
    end)
  end

  defp publish_transformed_messages(topics, transformed_messages) do
    Enum.each(topics, fn
      ({topic, partition_strategy}) ->
        transformed_messages
        |> Enum.map(fn(message) ->
          to_message(message, topic, partition_strategy)
        end)
        |> Rivulet.Kafka.Publisher.publish
    end)
  end

  def to_message(%Message{} = m, publish_topic, {:key, value}) do
    %Message{m | topic: publish_topic, partition_strategy: {:key, value}}
  end

  def to_message(%Message{} = m, publish_topic, :key) do
    %Message{m | topic: publish_topic, partition_strategy: {:key, m.key}}
  end

  def to_message(%Message{} = m, publish_topic, :random) do
    %Message{m | topic: publish_topic, partition_strategy: :random}
  end

  # Raise pattern match exception if no route was defined for the topic - that
  # is an error.
  defp routes_for_topic(topic, sources) do
    {^topic, routes} =
      Enum.find(sources, fn
        ({^topic, _}) -> true
        (_) -> false
      end)

    routes
  end

  defp listify_routes([:route | _] = route), do: [route]
  defp listify_routes(routes) when is_list(routes), do: routes

  defp listify_publish_topics({topic, _partition_strategy} = t) when is_binary(topic), do: [t]
  defp listify_publish_topics(topics) when is_list(topics), do: topics

  defp transform(messages, transformer) do
      messages
      |> Enum.map(fn(message) ->
        Task.async(fn ->
          message
          |> transformer.handle_message
          |> to_list
          |> List.flatten
          |> Enum.map(&to_publish/1)
        end)
      end)
      |> Enum.map(fn(task) ->
        Task.await(task, :timer.seconds(15))
      end)
      |> List.flatten
      |> Enum.reject(&is_nil/1)
  end
end
