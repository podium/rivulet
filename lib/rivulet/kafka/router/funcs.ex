defmodule Rivulet.Kafka.Router.Funcs do
  require Logger
  alias Rivulet.Kafka.Publisher.Message

  defp to_list(list) when is_list(list), do: list
  defp to_list(other), do: [other]

  def start_link(module, consumer_group, source_topics) do
    config =
      %Rivulet.Consumer.Config{
        client_id: Application.get_env(:rivulet, :publish_client_name),
        consumer_group_name: consumer_group,
        topics: source_topics,
        group_config: [
          offset_commit_policy: :commit_to_kafka_v2,
          offset_commit_interval_secons: 1
        ],
        consumer_config: [begin_offset: :earliest],
        message_type: :message_set
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
    {^topic, routes} =
      Enum.find(sources, fn
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
        messages
        |> Enum.map(fn(message) ->
          message
          |> transformer.handle_message
          |> to_list
          |> List.flatten
          |> Enum.map(&to_publish/1)
        end)
        |> List.flatten
        |> Enum.reject(fn
          (nil) -> true
          (_) -> false
        end)


        if :error in transformed_messages do
          Logger.error("Could not publish messages because transformer returned an error. Moving on.")
        else

          publish_topics =
            case publish_topics do
              topics when is_list(topics) -> topics
              {topic, partition_strategy} when is_binary(topic) -> [{topic, partition_strategy}]
            end

            Enum.each(publish_topics, fn
              ({publish_topic, partition_strategy}) ->
                transformed_messages
                |> Enum.map(fn(message) ->
                  to_message(message, publish_topic, partition_strategy)
                end)
                |> Rivulet.Kafka.Publisher.publish
            end)
        end
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
end
