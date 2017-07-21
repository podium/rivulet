defmodule Rivulet.Pipeline do
  use Supervisor

  alias Rivulet.Kafka.Partition

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
    {:ok, partition_count} = Rivulet.Kafka.Partition.partition_count(topic)

    topic
    |> children(partition_count |> range, child_module, extra_args)
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
end
