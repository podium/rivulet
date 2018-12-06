defmodule Rivulet.SQLSink.Writer.Manager do
  use GenServer

  alias Rivulet.Kafka.Partition

  def start_link(sup, count) do
    GenServer.start_link(__MODULE__, {sup, count})
  end

  def handle_batch(manager, partition, messages, consumer) do
    pid = GenServer.call(manager, {:get_pid, partition})
    Rivulet.SQLSink.Writer.handle_messages(pid, partition, messages, consumer)
  end

  def init({sup, count}) do
    {:ok, {sup, count}}
  end

  def handle_call({:get_pid, %Partition{} = partition} = _message_passed_to_process, _from, {sup, count} = state) do
    pid =
      partition
      |> id
      |> hash(count)
      |> find_pid_by_id(sup)

    {:reply, pid, state}
  end

  @spec id(Partition.t()) :: String.t()
  defp id(%Partition{topic: topic, partition: partition}) do
    "#{topic}:#{partition}"
  end

  @spec hash(String.t(), integer) :: integer
  defp hash(partition_id, count) do
    :erlang.phash2(partition_id, count) + 1
  end

  defp find_pid_by_id(hash, supervisor) do
    Rivulet.SQLSink.Supervisor.find_writer(supervisor, hash)
  end
end
