defmodule Rivulet.Join.ElasticSearch do
  use HTTPoison.Base
  alias HTTPoison.Response
  alias Rivulet.JSON

  def process_url(path) do
    :fire_hydrant
    |> Application.get_env(:elasticsearch_uri)
    |> URI.parse
    |> URI.merge(URI.parse(path))
    |> URI.to_string
  end

  def process_request_headers(headers) do
    [{"Content-Type", "application/json"} | headers]
  end

  def process_response_body(body) do
    :jiffy.decode(body, [:return_maps])
  end

  @type join_key :: binary
  @type join_id :: binary
  @type object_id :: binary
  @type document :: Keyword.t | [{String.t, term}]
  @type uuid :: binary
  @type newline :: binary
  @type cmd :: binary
  @type data :: Poison.t
  @type batch :: [binary] #[cmd, newline, data, newline]

  @spec bulk_put_join_doc([{:put, join_key, object_id, document, uuid}], join_id) :: batch
  def bulk_put_join_doc(docs, join_id) when is_binary(join_id) do
    docs
    |> Enum.map(fn({:put, join_key, object_id, value, uuid}) ->
      cmd =
        %{"update" =>
          %{"_index" => index(join_id),
            "_type" => "join_documents",
            "_id" => object_id
          }
        }
      value =
        value
        |> :erlang.term_to_binary
        |> Base.encode64
      doc = %{"doc" => %{"join_key" => join_key, "document" => value}, "doc_as_upsert" => true}
      require IEx; IEx.pry

      {:ok, cmd} = JSON.encode(cmd)
      {:ok, doc} = JSON.encode(doc)

      {uuid, cmd, doc}
    end)
    |> Enum.reject(fn
      (nil) -> true
      (_) -> false
    end)
    |> List.flatten
    |> Enum.map(fn({_uuid, cmd, doc}) ->
        [cmd, "\n", doc, "\n"]
      end)
  end

  def bulk_update(batch) do
    case post("/_bulk?refresh=wait_for", batch) do
      {:error, _} = err -> raise "Calls to Elasticsearch are failing! #{inspect err}"
      {:ok, %Response{status_code: code, body: body}} when code >= 200 and code < 300 ->
        body
      {:ok, resp} ->
        raise "Calls to Elasticsearch are failing - #{inspect resp}"
    end
  end

  def create_join_index(index_name) do
    index_name = index(index_name)
    {:ok, json} =
      %{mappings:
        %{join_documents:
          %{properties:
            %{join_key: %{type: "keyword"},
              document: %{type: "binary"}}}}}
      |> JSON.encode
    put("/#{index_name}", json, [])
  end

  def put_join_doc(join_id, join_key, object_id, document) do
    params =
      document
      |> Map.update(:join_key, join_key, fn(_) -> join_key end)
      |> JSON.encode

    put("/#{index(join_id)}/join_documents/#{object_id}" , params)
  end

  def bulk_get_join_docs(join_id, join_keys) do
    body =
      Enum.map(join_keys, fn(join_key) ->
        {:ok, header} =
          JSON.encode(%{
            "index" => index(join_id),
          })

        {:ok, query} =
          JSON.encode(%{"query" => %{"match" => %{"join_key" => "#{join_key}"}}})

        [header, "\n", query, "\n"]
      end)

    case request(:get, "/join_documents/_msearch", body) do
      {:error, _} = err -> raise "Calls to Elasticsearch are failing! #{inspect err}"
      {:ok, %Response{status_code: code, body: body}} when code >= 200 and code < 300 ->
        body
      {:ok, resp} ->
        raise "Calls to Elasticsearch are failing - #{inspect resp}"
    end
  end

  def get_join_docs(join_id, join_key) do
    case get("/#{index(join_id)}/join_documents/_search?q=join_key:/#{join_key}/") do
      {:ok, %Response{body: body}} -> body
      {:error, _reason} = err -> err
    end
  end

  def index(join_id) do
    "rivulet-joinkey-#{join_id}"
  end
end
