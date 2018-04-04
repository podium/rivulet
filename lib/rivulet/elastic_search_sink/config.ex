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

  def from_sink_opts(opts) do
    callback_module = Keyword.get(opts, :caller_module)
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
