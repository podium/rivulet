defmodule Rivulet.ElasticSearchSink.Config do
  alias Rivulet.Kafka.Partition

  defstruct [
    :consumer_group,
    :elastic_index,
    :elastic_url,
    :elastic_type,
    :elastic_mapping,
    :topic
  ]

  @type t :: %__MODULE__{
    consumer_group: String.t,
    elastic_index: String.t,
    elastic_url: String.t,
    elastic_type: String.t,
    elastic_mapping: map,
    topic: Partition.topic
  }

  def from_sink_opts(opts) do
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
      elastic_mapping: elastic_mapping
    }
  end
end
