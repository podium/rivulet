defmodule Rivulet.ElasticSearchSink.Database.ElasticSearchGenerator do
  alias Elastix.{Index, Mapping, Bulk}

  @type config :: Config.t
  @type url :: String.t
  @type index_name :: String.t
  @type type_name :: String.t
  @type mapping :: %{properties: %{}}
  @type exception :: any()
  @type action_type :: String.t
  @type record :: %{}
  @type index_record :: []
  @type upserted_record :: []
  @type query :: %{}

  @doc """
  Create an index.
  """
  @spec create_index(config) :: true | exception
  def create_index(config) do
    create_index(config.elastic_url, config.elastic_index)
  end
  defp create_index(url, index) do
    Logger.info "[Sinks.Elastic] Creating elasticsearch index: #{index}"

    url
    |> Index.create(index, %{})
    |> handle_es_response("create_index")
  end

  @doc """
  Update mappings on an index.
  """
  @spec create_mapping(config) :: true | exception
  def create_mapping(config) do
    create_mapping(
      config.elastic_url,
      config.elastic_index,
      config.elastic_type,
      config.elastic_mapping
    )
  end
  @spec create_mapping(url, index_name, type_name, mapping) :: true | exception
  defp create_mapping(url, index, type, mapping) do
    Logger.info "[Sinks.Elastic] creating elasticsearch mapping"

    url
    |> Mapping.put(index, type, mapping)
    |> handle_es_response("create_mapping")
  end

  # --------------------------------------------------------------
  # Handle the Elastix return
  # --------------------------------------------------------------

  # Errors
  @spec handle_es_response({:ok | any, %HTTPoison.Response{}}, action_type) :: true | exception
  defp handle_es_response({_, %HTTPoison.Response{body: %{"error" => %{"reason" => reason}}}}, action) do
    raise "Elasticsearch error: #{action} #{reason}"
  end

  # Success
  defp handle_es_response({:ok, %HTTPoison.Response{status_code: status_code}}, _) when status_code in 200..299 do
    true
  end

  # Catch-all error
  defp handle_es_response(_resp, action) do
    raise "Elasticsearch error: #{action}"
  end
end
