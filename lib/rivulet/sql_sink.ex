defmodule Rivulet.SQLSink do
  require Logger

  alias Rivulet.Kafka.Partition

  defdelegate start_link(opts), to: Rivulet.SQLSink.Supervisor

  defmodule Config do
    defstruct [
      :consumer_group,
      :delete_key_field,
      :primary_keys,
      :repo,
      :table_pattern,
      :topic,
      :unique_constraints,
      :decoding_strategy,
      whitelist: :all
    ]

    @type t :: %__MODULE__{
      consumer_group: String.t,
      delete_key_field: atom,
      primary_keys: [Table.column_name] | :sequence,
      repo: module,
      table_pattern: String.t,
      topic: Partition.topic,
      unique_constraints: [[Table.column_name]],
      decoding_strategy: atom,
      whitelist: :all | [Table.column_name]
    }

    def from_sink_opts(opts) do
      consumer_group = Keyword.fetch!(opts, :consumer_group)
      delete_key_field = Keyword.get(opts, :delete_key_field)
      primary_keys = Keyword.get(opts, :primary_keys, [])
      repo = Keyword.fetch!(opts, :repo)
      topic = Keyword.fetch!(opts, :topic)
      table_pattern = Keyword.get(opts, :table_pattern, topic)
      unique_constraints = Keyword.get(opts, :unique_constraints, [])
      decoding_strategy = Keyword.get(opts, :decoding_strategy, :json)
      whitelist = Keyword.get(opts, :whitelist, :all)

      %__MODULE__{
        consumer_group: consumer_group,
        delete_key_field: delete_key_field,
        primary_keys: primary_keys,
        repo: repo,
        topic: topic,
        table_pattern: table_pattern,
        unique_constraints: unique_constraints,
        decoding_strategy: decoding_strategy,
        whitelist: whitelist
      }
    end
  end
end
