defmodule Rivulet.Join.Handler do
  use GenServer

  alias Rivulet.Join.ElasticSearch
  alias Rivulet.Kafka.Partition

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

  def pry_those_things(payload) do
    require IEx; IEx.pry
    payload
  end

  def handle_call({:handle_resp, join_keys, ack_data}, from,  %State{join_id: join_id, transformers: transformers, consumer: consumer} = state) do
    GenServer.reply(from, :ok)

    res =
      join_id
      |> ElasticSearch.bulk_get_join_docs(join_keys)
      |> pry_those_things()
      |> Map.get("responses")
      |> Enum.map(fn(%{"hits" => %{"hits" => hits}}) -> hits end)
      |> Enum.map(fn(hits) -> Enum.map(hits, fn(hit) -> hit["_source"]["document"] end) end)
      |> Enum.map(fn (docs) ->
        Enum.map(docs, fn (doc) ->
          doc
          |> Base.decode64!
          |> :erlang.binary_to_term
        end)
      end)

    require IEx; IEx.pry

    Rivulet.Kafka.Join.Funcs.transforms(res, transformers)

    ack_data
    |> Enum.reduce(%{}, fn
      ({topic, partition, offset}, %{} = acks) ->
        require IEx; IEx.pry

        Map.update(acks, {topic, partition}, offset, fn(prev_offset) ->
          if prev_offset > offset,
          do: prev_offset,
          else: offset
        end)
    end)
    |> Enum.each(fn({{topic, partition}, offset}) ->
      require IEx; IEx.pry

      partition = %Partition{topic: topic, partition: partition}

      require IEx; IEx.pry

      Rivulet.Consumer.ack(consumer, partition, offset)
    end)

    {:noreply, state}
  end

  def handle_info(_, state) do
    {:noreply, state}
  end
end
