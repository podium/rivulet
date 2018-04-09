defmodule Rivulet.ElasticSearchSink.Writer.Manager do
  use GenServer

  alias Rivulet.Kafka.Partition

  def start_link(sup, count) do
    GenServer.start_link(__MODULE__, {sup, count})
  end

  @doc """
  NOTE: signature for GenServer.call/2 is call(process_identifier, message_to_pass)
  """
  def handle_batch(manager_pid, partition, messages) do
    writer_pid = GenServer.call(manager_pid, {:get_pid, partition})
    Rivulet.ElasticSearchSink.Writer.handle_messages(writer_pid, partition, messages)
  end

  def init({sup, count}) do
    {:ok, {sup, count}}
  end

  @doc """
  NOTE: state is a tuple consisting of {supervisor_process_pid, integer_for_count_of_writers}
  """
  def handle_call({:get_pid, %Partition{} = partition}, _from, {sup_pid, count} = state) do
    pid =
      partition
      |> id
      |> hash(count)
      |> find_pid_by_id(sup_pid)

    {:reply, pid, state}
  end

  defp id(%Partition{topic: topic, partition: partition}) do
    "#{topic}:#{partition}"
  end

  defp hash(partition_id, count) do
    :erlang.phash2(partition_id, count) + 1
  end

  @doc """
  NOTE: the find_writer on line 37 (Rivulet.ElasticSearchSink.Supervisor module)
  will first pattern match on the find_by_id/2 on line 48
  and then on line 41 and ultimately return the process identifier
  for some writer process that the supervisor has as a child.
  """
  def find_pid_by_id(hash, supervisor_pid) do
    Rivulet.ElasticSearchSink.Supervisor.find_writer(supervisor_pid, hash)
  end
end
