defmodule NullablePrimaryKeyException do
  defexception [:message]
end

defmodule Rivulet.SQLSink.Database.SQLGenerator do
  alias Rivulet.SQLSink.Database.{Postgres, Table}

  def execute(commands) when is_list(commands) do
    FireHydrant.Repo.transaction(fn ->
      commands
      |> List.flatten
      |> Enum.map(&execute/1)
    end)
  end

  def execute(sql) when is_binary(sql) do
    Ecto.Adapters.SQL.query(FireHydrant.Repo, sql)
  end

  def create_table(%Table{database: :postgres, primary_keys: :sequence} = table) do
    column_definitions =
      Enum.map(table.column_definitions, fn
        ({name, {:nullable, type}}) ->
          column_definition(name, :nullable, type, _primary_key = false)
        ({name, type}) ->
          column_definition(name, :not_nullable, type, _primary_key = false)
      end)

    unique_index_definitions =
      Enum.map(table.unique_constraints, fn(constraint) when is_list(constraint) ->
        index_name_columns = Enum.join(constraint, "_")
        index_columns = Enum.join(constraint, ",")
        """
        CREATE UNIQUE INDEX IF NOT EXISTS "#{table.name}_#{index_name_columns}_index" ON "#{table.name}"
        (#{index_columns});
        """
      end)

    create_sql =
      """
      CREATE TABLE IF NOT EXISTS "#{table.name}"
      (id BIGINT PRIMARY KEY,
      #{Enum.join(column_definitions, ",\n")});
      """

    sequence_sql =
      [
        """
        CREATE SEQUENCE #{table.name}_seq
        START WITH 1
        INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;
        """,
        """
        ALTER SEQUENCE #{table.name}_seq OWNED BY #{table.name}.id;
        """,
        """
        ALTER TABLE ONLY #{table.name} ALTER COLUMN id SET DEFAULT nextval('#{table.name}_seq'::regclass);
        """
      ]

    [create_sql, sequence_sql, unique_index_definitions]
  end

  def create_table(%Table{database: :postgres} = table) do
    column_definitions =
      Enum.map(table.column_definitions, fn
        ({name, {:nullable, type}}) ->
          column_definition(name, :nullable, type, name in table.primary_keys)
        ({name, type}) ->
          column_definition(name, :not_nullable, type, name in table.primary_keys)
      end)

    unique_index_definitions =
      Enum.map(table.unique_constraints, fn(constraint) when is_list(constraint) ->
        index_name_columns = Enum.join(constraint, "_")
        index_columns = Enum.join(constraint, ",")
        """
        CREATE UNIQUE INDEX IF NOT EXISTS "#{table.name}_#{index_name_columns}_index" ON "#{table.name}"
        (#{index_columns});
        """
      end)

    create_sql =
      """
      CREATE TABLE IF NOT EXISTS "#{table.name}"
      (#{Enum.join(column_definitions, ",\n")});
      """
    [create_sql | unique_index_definitions]
  end

  def column_definition(name, :not_nullable, type, _primary_key = true) do
    "#{name} #{Postgres.column_type(type)} PRIMARY KEY"
  end

  def column_definition(name, :nullable, type, _primary_key = false) do
    "#{name} #{Postgres.column_type(type)}"
  end

  def column_definition(name, :not_nullable, type, _primary_key = false) do
    "#{name} #{Postgres.column_type(type)} NOT NULL"
  end

  def column_definition(name, :nullable, type, _primary_key = true) do
    raise NullablePrimaryKeyException, message: "Primary key field #{name} (#{inspect type}) is nullable"
  end
end
