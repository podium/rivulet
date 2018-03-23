defmodule Rivulet.Join.Handler do
  use GenServer

  alias Rivulet.Join.ElasticSearch

  defmodule State do
    defstruct [:join_id, :transformers, :consumer]
  end

  def start(join_id, transformers, consumer) do
    GenServer.start(__MODULE__, [join_id, transformers, consumer])
  end

  def start_link(join_id, transformers, consumer) do
    GenServer.start_link(__MODULE__, [join_id, transformers, consumer])
  end

  def stop(ref) do
    GenServer.stop(ref)
  end

  def handle_resp(handler, join_keys, ack_data) do
    GenServer.call(handler, {:handle_resp, join_keys, ack_data}, 8_000)
  end

  def init([join_id, transformers, consumer]) do
    {:ok, %State{join_id: join_id, transformers: transformers, consumer: consumer}}
  end

  def handle_call({:handle_resp, join_keys, ack_data}, from,  %State{join_id: join_id, transformers: transformers, consumer: consumer} = state) do
    GenServer.reply(from, :ok)

    {time, val} = :timer.tc(fn ->
    res =
      join_id
      |> ElasticSearch.bulk_get_join_docs(join_keys)
      |> Map.get("responses")
      |> Enum.map(fn(%{"hits" => %{"hits" => hits}}) -> hits end)
      |> Enum.map(fn(hits) -> Enum.map(hits, fn(hit) -> hit["_source"]["document"] end) end)
      |> Enum.map(fn (docs) ->
        Enum.map(docs, fn (doc) -> doc |> Base.decode64! |> :erlang.binary_to_term end)
      end)

    Rivulet.Kafka.Join.Funcs.transforms(res, transformers)

    ack_data
    |> Enum.reduce(%{}, fn
      ({topic, partition, offset}, %{} = acks) ->
        Map.update(acks, {topic, partition}, offset, fn(prev_offset) ->
          if prev_offset > offset,
          do: prev_offset,
          else: offset
        end)
    end)
    |> Enum.each(fn({{topic, partition}, offset}) ->
      ack(consumer, topic, partition, offset)
    end)

    {:noreply, state}
    end)
    IO.inspect("handle took #{time} microseconds")
    val
  end

  def ack(consumer, topic, partition, offset) do
    :brod_group_subscriber.ack(consumer, topic, partition, offset)
  end

  def handle_info(_, state) do
    {:noreply, state}
  end
end
