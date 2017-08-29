defmodule Rivulet.Pipeline.Supervisor do
  use Supervisor

  alias Rivulet.Kafka.Partition
  require Logger

  @type extra_args :: [term]

  @spec start_link(Partition.topic, module, atom, [term])
  :: Supervisor.on_start
  def start_link(topic, child_module, rivulet_name, extra_args \\ [])
  when is_binary(topic)
  and is_atom(rivulet_name)
  and is_list(extra_args) do
    Supervisor.start_link(__MODULE__, {topic, child_module, extra_args}, name: rivulet_name)
  end

  def init({topic, child_module, extra_args}) when is_binary(topic) do
    Logger.debug("Getting partition count for #{topic}")

    {:ok, partition_count} = partition_count(topic)

    Logger.debug("#{topic} has #{partition_count} partitions")

    topic
    |> children(range(partition_count), child_module, extra_args)
    |> supervise([strategy: :one_for_one])
  end

  defp children(topic, range, child_module, extra_args) do
    Enum.map(range, fn(partition) ->
      partition = %Partition{topic: topic, partition: partition}
      supervisor(child_module, [partition] ++ extra_args, id: "#{child_module}.#{topic}.#{partition.partition}")
    end)
  end

  defp range(count) when is_integer(count) do
    0..(count - 1)
  end

  defp partition_count(topic) do
    case Rivulet.Kafka.Partition.partition_count(topic) do
      {:ok, count} -> {:ok, count}
      {:error, :topic_not_found, _} ->
        interval = retry_interval()

        Logger.error("#{__MODULE__}.partition_count/1 could not find topic: #{topic}. Retrying in #{interval} ms")

        :timer.sleep(interval)

        partition_count(topic)
    end
  end

  # TODO: Add exponential backoff if this is insufficient.
  defp retry_interval() do
    :timer.seconds(10)
  end
end
