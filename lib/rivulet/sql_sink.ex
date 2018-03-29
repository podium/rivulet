defmodule Rivulet.SQLSink do
  require Logger

  alias AvroEx.Schema
  alias Rivulet.SQLSink.Database.Table
  alias Rivulet.Kafka.Partition
  alias Rivulet.SQLSink.AvroConverter
  alias Rivulet.SQLSink.Database.SQLGenerator

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

      %__MODULE__{
        consumer_group: consumer_group,
        delete_key_field: delete_key_field,
        primary_keys: primary_keys,
        repo: repo,
        topic: topic,
        table_pattern: table_pattern,
        unique_constraints: unique_constraints
      }
    end
  end

  def ensure_table_created!(%Config{} = config) do
    subject = config.topic <> "-value"
    Logger.debug("Getting value schema for subject: #{subject}")
    {:ok, %Rivulet.Avro.Schema{} = schema} = Rivulet.Avro.schema_for_subject(subject)

    ensure_table_created!(config, schema.schema)
  end

  def ensure_table_created!(%Config{} = config, %AvroEx.Schema{} = schema) do
    Logger.debug("Ensuring table is created with config: #{inspect config}")

    #{:ok, results} =
      schema
      |> table_definition!(config)
      |> SQLGenerator.create_table
      |> SQLGenerator.execute(config.repo)

      #Enum.each(results, fn({:ok, _}) -> :ok end)
  end

  def table_definition!(schema, config) do
    case table_definition(schema, config) do
      %Table{} = table -> table
      err -> raise "Couldn't define table with config: #{inspect config}: Error: #{inspect err}"
    end
  end

  @spec table_definition(Schema.t | Schema.json_schema, Config.t)
  :: {:error, term}
  | Table.t
  def table_definition(avro_schema, config)
  when is_binary(avro_schema) do
    with {:ok, schema} <- AvroEx.parse_schema(avro_schema) do
      table_definition(schema, config)
    end
  end

  def table_definition(%Schema{} = schema, %Config{} = config) do
    case AvroConverter.definition(schema, config.table_pattern, config.topic) do
      {:error, :invalid_schema} -> {:error, :invalid_schema}
      {:error, :schema_not_insertable} -> {:error, :invalid_table_definition}
      %Table{} = table ->
        %Table{} =
          config.unique_constraints
          |> Enum.reduce(table, fn(unique_constraint, %Table{} = table) when is_list(unique_constraint) ->
            Table.unique_constraint(table, unique_constraint)
          end)
          |> Table.primary_keys(config.primary_keys)
    end
  end
end
