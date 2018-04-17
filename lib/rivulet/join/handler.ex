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

  def handle_call({:handle_resp, join_key_object_id_combo, ack_data}, from,  %State{join_id: join_id, transformers: transformers, consumer: consumer} = state) do
    GenServer.reply(from, :ok)

    just_join_keys =
      join_key_object_id_combo
      |> Enum.map(fn ({join_key, _}) -> join_key end)
      |> Enum.uniq

    lookup_map = create_lookup_map(join_key_object_id_combo, just_join_keys)

    source_docs =
      join_id
      |> ElasticSearch.bulk_get_join_docs(just_join_keys)
      |> Map.get("responses")
      |> Enum.map(fn(%{"hits" => %{"hits" => hits}}) -> hits end)
      |> Enum.map(fn(hits) -> Enum.map(hits, fn(hit) -> hit["_source"]["document"] end) end)

    zipped = just_join_keys
      |> Enum.zip(source_docs)
      |> Enum.map(fn ({key, _docs} = tup) ->
        last_most_responsible_object_id = Map.get(lookup_map, key)
        Tuple.append(tup, last_most_responsible_object_id)
      end)

    res =
      zipped
      |> Enum.map(fn ({join_key, docs, responsible}) ->
        decoded_docs = Enum.map(docs, fn (doc) ->
          doc
          |> Base.decode64!
          |> :erlang.binary_to_term
        end)

        {join_key, decoded_docs, responsible}
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
    |> Enum.each(fn({{topic, partition} = _key, offset = _value}) ->

      partition = %Partition{topic: topic, partition: partition}

      Rivulet.Consumer.ack(consumer, partition, offset)
    end)

    {:noreply, state}
  end

  @doc """
  NOTE:
  If 'combo' looks like this:
  [
    {"b2f9bfea-3ed6-5915-9e97-60589c0b2a27",
     "b2f9bfea-3ed6-5915-9e97-60589c0b2a27"},
    {"deccb141-6a3a-5127-8111-1d7936f985df",
     "deccb141-6a3a-5127-8111-1d7936f985df"},
    {"c0ee0235-5069-5773-b11e-280abca4bc20",
     "c0ee0235-5069-5773-b11e-280abca4bc20"},
    {"64ac0fd5-9f23-5d15-97c3-5d2c2086d763",
     "64ac0fd5-9f23-5d15-97c3-5d2c2086d763"},
    {"789686bf-fa4d-569a-9a9f-0fd66496fa48",
     "789686bf-fa4d-569a-9a9f-0fd66496fa48"},
    {"55471a1f-3f9a-55d2-90c3-8e04c1773617",
     "55471a1f-3f9a-55d2-90c3-8e04c1773617"},
    {"789686bf-fa4d-569a-9a9f-0fd66496fa48",
     "63ea1ac2-28d4-50ed-8396-055f9326b380"},
    {"c0ee0235-5069-5773-b11e-280abca4bc20",
     "f0315c54-b272-5e93-86f9-9897c2d860c5"},
    {"c0ee0235-5069-5773-b11e-280abca4bc20",
     "f0315c54-b272-5e93-86f9-9897c2d860c5"},
    {"b2f9bfea-3ed6-5915-9e97-60589c0b2a27",
     "49e32693-2ebf-5d61-8a52-c4a7086d7e19"},
    {"b2f9bfea-3ed6-5915-9e97-60589c0b2a27",
     "49e32693-2ebf-5d61-8a52-c4a7086d7e19"}
  ]

  'grouped' will look like this:
  %{
    "55471a1f-3f9a-55d2-90c3-8e04c1773617" => ["55471a1f-3f9a-55d2-90c3-8e04c1773617"],
    "64ac0fd5-9f23-5d15-97c3-5d2c2086d763" => ["64ac0fd5-9f23-5d15-97c3-5d2c2086d763"],
    "789686bf-fa4d-569a-9a9f-0fd66496fa48" => ["789686bf-fa4d-569a-9a9f-0fd66496fa48",
     "63ea1ac2-28d4-50ed-8396-055f9326b380"],
    "b2f9bfea-3ed6-5915-9e97-60589c0b2a27" => ["b2f9bfea-3ed6-5915-9e97-60589c0b2a27",
     "49e32693-2ebf-5d61-8a52-c4a7086d7e19",
     "49e32693-2ebf-5d61-8a52-c4a7086d7e19"],
    "c0ee0235-5069-5773-b11e-280abca4bc20" => ["c0ee0235-5069-5773-b11e-280abca4bc20",
     "f0315c54-b272-5e93-86f9-9897c2d860c5",
     "f0315c54-b272-5e93-86f9-9897c2d860c5"],
    "deccb141-6a3a-5127-8111-1d7936f985df" => ["deccb141-6a3a-5127-8111-1d7936f985df"]
  }

  'grouped' has the same number of keys as will be present in the 'join_keys' list
  passed in as second arg.
  """
  def create_lookup_map(combo, join_keys) do
    grouped = Enum.group_by(combo, &(elem(&1, 0)), &(elem(&1, 1)))

    Enum.reduce(join_keys, %{}, fn (join_key, acc) ->
      last_key = grouped
        |> Map.get(join_key)
        |> List.last

      to_return = Map.put(acc, join_key, last_key)
      to_return
    end)
  end

  def handle_info(_, state) do
    {:noreply, state}
  end
end
