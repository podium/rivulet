defmodule Rivulet.ElasticSearchSink.Config do
  alias Rivulet.Kafka.Partition

  defstruct [
    :consumer_group,
    :elastic_index,
    :elastic_url,
    :elastic_type,
    :elastic_mapping,
    :topic,
    :callback_module
  ]

  @type t :: %__MODULE__{
    consumer_group: String.t,
    elastic_index: String.t,
    elastic_url: String.t,
    elastic_type: String.t,
    elastic_mapping: map,
    topic: Partition.topic,
    callback_module: any
  }

  @doc """
  NOTE: opts passed into from_sink_opts would look something
  like this:

    [
     elastic_url: "http://elasticsearch:9200",
     elastic_mapping: %{
       properties: %{
         _search: %{type: "text"},
         adjusted_score: %{type: "short"},
         conversation_uid: %{type: "keyword"},
         created_at: %{format: "date_optional_time", type: "date"},
         customer_name: %{copy_to: "_search", index: false, type: "text"},
         customer_phone: %{type: "text"},
         invitation_sent_at: %{format: "date_optional_time", type: "date"},
         location_address: %{fields: %{raw: %{type: "keyword"}}, type: "text"},
         location_name: %{fields: %{raw: %{type: "keyword"}}, type: "text"},
         location_uid: %{type: "keyword"},
         nps_comment: %{copy_to: "_search", index: "false", type: "text"},
         nps_score: %{type: "short"},
         organization_uid: %{type: "keyword"},
         response_received_at: %{format: "date_optional_time", type: "date"}
       }
     },
     otp_app: :snowden_sink,
     callback_module: SnowdenSink.ElasticSearchSink,
     elastic_index: "nps",
     elastic_type: "response",
     topic: "platform_nps_location_joins"
   ]
  """
  def from_sink_opts(opts) do
    callback_module = Keyword.get(opts, :callback_module)
    elastic_index = Keyword.get(opts, :elastic_index)
    elastic_url = Keyword.get(opts, :elastic_url, [])
    elastic_type = Keyword.fetch!(opts, :elastic_type)
    topic = Keyword.fetch!(opts, :topic)
    elastic_mapping = Keyword.get(opts, :elastic_mapping)

    %__MODULE__{
      consumer_group: "#{topic}-elasticsearch-sink",
      elastic_index: elastic_index,
      elastic_url: elastic_url,
      elastic_type: elastic_type,
      topic: topic,
      elastic_mapping: elastic_mapping,
      callback_module: callback_module
    }
  end
end
