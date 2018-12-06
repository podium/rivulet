defmodule Rivulet.SQLSink.Database.Table do
  defstruct [:name, database: :postgres, column_definitions: [], primary_keys: [], unique_constraints: []]

  @supported_dbs [:postgres]

  @type name_pattern :: String.t

  @type table_name :: String.t
  @type column_name :: String.t
  @type column_type
    :: {:varchar, pos_integer}
    | :text
    | :date
    | :time
    | :integer
    | :utc_datetime
    | :long
    | :float
    | :double
    | :blob
    | :uuid
    | :boolean
    | {:nullable, t}
  @type column_definition :: {column_name, column_type}

  @type t :: %__MODULE__{
    column_definitions: [column_definition],
    primary_keys: [column_name],
    unique_constraints: [[column_name]],
    database: :postgres,
    name: table_name
  }

  def new(name) when is_binary(name) do
    %__MODULE__{name: name}
  end

  def with_column(%__MODULE__{} = table, name, type) do
    %__MODULE__{table | column_definitions: [{name, type} | table.column_definitions]}
  end

  def primary_keys(%__MODULE__{} = table, primary_keys)
  when is_list(primary_keys) or primary_keys == :sequence do
    %__MODULE__{table | primary_keys: primary_keys}
  end

  def reverse_columns(%__MODULE__{} = table) do
    %__MODULE__{table | column_definitions: Enum.reverse(table.column_definitions)}
  end

  def unique_constraint(%__MODULE__{} = table, unique_constraint) when is_list(unique_constraint) do
    %__MODULE__{table | unique_constraints: [unique_constraint | table.unique_constraints]}
  end

  def database(%__MODULE__{} = table, database) when database in @supported_dbs do
    %__MODULE__{table | database: database}
  end

  def sort_columns(%__MODULE__{column_definitions: columns} = table) do
    columns =
      columns
      |> Enum.reverse
      |> Enum.sort(fn({_namea, typea}, {_nameb, typeb}) ->
        sort_types(typea, typeb)
      end)

    %__MODULE__{table | column_definitions: columns}
  end

  @spec table_name(name_pattern, Partition.topic)
  :: table_name
  def table_name(pattern, topic) when is_binary(pattern) do
    pattern
    |> String.replace(~r/\$\$/, topic)
    |> String.replace(~r/\.|-/, "_")
  end

  def sort_types({:nullable, nullable}, b), do: sort_types(nullable, b)
  def sort_types(a, {:nullable, nullable}), do: sort_types(a, nullable)
  def sort_types(:boolean, _), do: true
  def sort_types(_, :boolean), do: false
  def sort_types(:integer, _), do: true
  def sort_types(_, :integer), do: false
  def sort_types(:date, _), do: true
  def sort_types(_, :date), do: false
  def sort_types(:time, _), do: true
  def sort_types(_, :time), do: false
  def sort_types(:utc_datetime, _), do: true
  def sort_types(_, :utc_datetime), do: false
  def sort_types(:long, _), do: true
  def sort_types(_, :long), do: false
  def sort_types(:float, _), do: true
  def sort_types(_, :float), do: false
  def sort_types(:double, _), do: true
  def sort_types(_, :double), do: false
  def sort_types(:uuid, _), do: true
  def sort_types(_, :uuid), do: false
  def sort_types({:varchar, size_a}, {:varchar, size_b}), do: size_a <= size_b
  def sort_types({:varchar, _}, _), do: true
  def sort_types(_, {:varchar, _}), do: false
  def sort_types(:text, _), do: true
  def sort_types(_, :text), do: false
  def sort_types(:blob, _), do: true
  def sort_types(_, :blob), do: false
end
