defmodule Rivulet.Join.Batcher do
  @behaviour :gen_statem
  require Logger

  alias Rivulet.Join.ElasticSearch

  defmodule Data do
    defstruct [:handler, updates: [], ack_data: [], join_keys: []]
  end

  @empty_state :empty
  @filling_state :filling
  @flush_event :flush

  alias Rivulet.Kafka.Partition
  @spec batch_commands(pid, [ElasticSearch.batch], [String.t], Partition.topic, Partition.partition, non_neg_integer)
  :: :ok
  def batch_commands(batcher, cmds, join_keys, topic, partition, offset) do
    :gen_statem.call(batcher, {:add_batch, cmds, join_keys, {topic, partition, offset}})
  end

  def start_link(handler) do
    :gen_statem.start_link(__MODULE__, {handler}, [])
  end

  def callback_mode, do: [:handle_event_function, :state_enter]

  def init({handler}) do
    {:ok, @empty_state, %Data{handler: handler}}
  end

  def handle_event({:call, from}, {:add_batch, cmds, join_keys, {_topic, _partition, _offset} = ack_data}, @empty_state, %Data{} = data) do
    new_data = %Data{data | updates: [cmds | data.updates], ack_data: [ack_data | data.ack_data], join_keys: [join_keys | data.join_keys]}
    {:next_state, @filling_state, new_data, [{:reply, from, :accepted}]}
  end

  def handle_event(:enter, @empty_state, @filling_state, %Data{} = data) do
    {:next_state, @filling_state, data, [{:state_timeout, 500, @flush_event}]}
  end

  def handle_event(:enter, _any, @empty_state, %Data{}) do
    :keep_state_and_data
  end

  def handle_event({:call, from}, {:add_batch, cmds, join_keys, {_topic, _partition, _offset} = ack_data}, @filling_state, %Data{} = data) do
    new_data = %Data{data | updates: [cmds | data.updates], ack_data: [ack_data | data.ack_data], join_keys: [join_keys | data.join_keys]}
    {:keep_state, new_data, [{:reply, from, :accepted}]}
  end

  def handle_event(:state_timeout, :flush, @filling_state, %Data{} = data) do
    resp =
      data.updates
      |> Enum.reverse
      |> flush

    require IEx; IEx.pry
    
    Enum.each(resp["items"], fn(item) ->
      unless item["update"]["status"] < 300 do
        Logger.error("Elasticsearch updates failed. #{inspect item}")
      end
    end)

    join_keys =
      data.join_keys
      |> Enum.reverse
      |> List.flatten

    require IEx; IEx.pry

    Rivulet.Join.Handler.handle_resp(data.handler, join_keys, Enum.reverse(data.ack_data))

    require IEx; IEx.pry

    {:next_state, @empty_state, %Data{handler: data.handler}}
  end

  def handle_event(_type, event, _state, _data) do
    Logger.warn("Join Batcher received unknown event: #{inspect event}")
    :keep_state_and_data
  end

  #def handle_event({updates,})

  @spec flush([ElasticSearch.batch]) :: term
  defp flush(batch) do
    ElasticSearch.bulk_update(batch)
  end
end
