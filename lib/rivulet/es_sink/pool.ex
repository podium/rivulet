defmodule Rivulet.ElasticSearchSink.Pool do
  @moduledoc """
  A pool of processes for handling batches of Rivulet Message Sets, currently
  only supporting `Consumer.handle_batch/2`.

  Usage:

  1. Start in application start
  2. Call `Pool.process_messages(messages)` when you have messages to
  process.
  """
  use Supervisor
  alias FireHydrant.Pool.Batcher
  alias Rivulet.Kafka.Partition

  def start_link do
    Supervisor.start_link(__MODULE__, [])
  end

  def init(_) do
    import Supervisor.Spec

    children =
      [
        supervisor(FireHydrant.Pool.TaskSupervisor, []),
        worker(FireHydrant.Pool.Batcher, [])
      ]

    options = [strategy: :one_for_one]

    supervise(children, options)
  end

  def process_messages(messages, partition = %Partition{}) do
    Batcher.add_messages(messages, partition)
  end
end

defmodule FireHydrant.Pool.TaskSupervisor do
  @moduledoc false
  alias FireHydrant.Consumer
  alias Rivulet.Kafka.Partition

  def start_link do
    Task.Supervisor.start_link(name: __MODULE__)
  end

  def handle_messages(messages, partitions) do
    Task.Supervisor.start_child(__MODULE__, fn ->
      # here we need to: dump into ES, handle errors (in same module)
      # in separate module: send successful messages to RabbitMQ
      FireHydrant.Sinks.Elastic.bulk_index(messages)

      Enum.each(partitions, fn({%Partition{} = partition, offset}) ->
        Rivulet.Consumer.ack(Consumer.consumer_name(), partition, offset)
      end)
    end)
  end
end

defmodule FireHydrant.Pool.Batcher do
  @moduledoc """
  The batcher receives lists of Rivulet.Kafka.Consumer.Message structs. It
  batches these messages, preserving received order, and then spins up a task to
  process those messages. The number of such tasks that can be runningu
  concurrently is determined by the number of DB connections in Ecto's
  connection pool.
  """
  use GenServer

  alias FireHydrant.Pool.TaskSupervisor
  alias Rivulet.Kafka.Partition

  require Logger
  @type ignored :: term

  defmodule State do
    @moduledoc false
    defstruct [batches: []]

    @type t :: %__MODULE__{
      batches: [Batch.t]
    }
  end

  defmodule Batch do
    @moduledoc false
    defstruct [:messages, :partition, :message_count]

    def message_count(%__MODULE__{message_count: nil, messages: messages}) do
      length(messages)
    end

    def message_count(%__MODULE__{message_count: message_count}) do
      message_count
    end

    def message_count(batches) when is_list(batches) do
      Enum.reduce(batches, 0, fn(%__MODULE__{} = batch, sum) ->
        sum + message_count(batch)
      end)
    end
  end

  @spec start_link :: GenServer.on_start
  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc """
  Messages should be in order from lowest offset to highest
  """
  @spec add_messages(Rivulet.Consumer.messages, Partition.t) :: :accepted
  def add_messages(messages, partition = %Partition{}) do
    GenServer.call(__MODULE__, {:add_messages, messages, partition}, :timer.seconds(15))
  end

  def init(_) do
    state = %State{}
    {:ok, state}
  end

  @doc """
  This GenServer's state is a struct (%State{}) with a batches key on it. When more
  messages come in via add_messages/2 above, our handle_call/3 will be invoked
  and we'll create a new %Batch{} struct from the messages that are passed in.

  We call Enum.reduce iterating over each %Batch{} and adding the value contained
  within it's :message_count key to the accumulator. If that sum total is greater
  than the minimum batch size, we process the batch, overwise we save all the
  batches we've received to the state, and continue this process.
  """
  def handle_call({:add_messages, messages, partition = %Partition{}}, _from, state = %State{batches: batches}) do
    batch = %Batch{messages: messages, message_count: length(messages), partition: partition}
    batches = [batch | batches]

    min_batch_size = Application.get_env(:fire_hydrant, :min_batch_size)
    if Batch.message_count(batches) >= min_batch_size do
      handle_batches(batches)

      {:reply, :accepted, %State{}}
    else
      {:reply, :accepted, %State{state | batches: batches}, :timer.seconds(1)}
    end
  end

  def handle_info(:timeout, %State{batches: batches}) do
    handle_batches(batches)

    {:noreply, %State{}}
  end

  def handle_info(_, state) do
    {:noreply, state}
  end

  @spec handle_batches([Batch.t]) :: :ok
  defp handle_batches(batches) when is_list(batches) do
    partition_messages = preprocess_batches(batches)

    messages =
      partition_messages
      |> Enum.map(fn({_, messages}) -> messages end)
      |> List.flatten

    partitions =
      partition_messages
      |> Enum.map(fn
        ({_partition, []}) -> nil
        ({partition, messages}) -> {partition, List.last(messages).offset}
      end)
      |> Enum.filter(&(&1)) # Remove falsey values (when there are no messages
                            # to be processed for a partition)

    do_handle_batches(messages, partitions)
  end

  @spec preprocess_batches([Batch.t]) :: [{Partition.t, [Rivulet.Kafka.Consumer.Message.t]}]
  def preprocess_batches(batches) when is_list(batches) do
    batches
    |> Enum.group_by(fn(%Batch{partition: partition}) -> partition end)
    |> Enum.map(fn({partition, batches}) ->
         messages =
           batches
           |> Enum.map(fn %Batch{messages: messages} -> messages end)
           |> Enum.reduce([], fn(messages, acc) -> messages ++ acc end)
           |> Enum.sort(&(&1.offset <= &2.offset))

         {partition, messages}
       end)
  end

  defp do_handle_batches(messages, partitions) do
    max_children = 1

    running_children_count =
      TaskSupervisor
      |> Task.Supervisor.children
      |> length

    if running_children_count < max_children do
      Logger.info("Starting up Pooled Task")
      TaskSupervisor.handle_messages(messages, partitions)
    else
      Logger.info("Pool saturated - waiting to try again.")
      :timer.sleep(1) # milliseconds
      do_handle_batches(messages, partitions)
    end
  end
end
