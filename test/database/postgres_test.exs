defmodule Rivulet.SQLSink.Database.Postgres.Test.Macros do
  defmacro __using__(_) do
    quote do
      import unquote(__MODULE__)
    end
  end

  defmacro translates(abstract_column_type, [to: db_specific_type]) do
    quote do
      test "#{inspect unquote(abstract_column_type)} translates to #{unquote(db_specific_type)}" do
        assert @test_module.column_type(unquote(abstract_column_type)) == unquote(db_specific_type)
      end
    end
  end
end

defmodule Rivulet.SQLSink.Database.Postgres.Test do
  use ExUnit.Case
  use __MODULE__.Macros

  @test_module Rivulet.SQLSink.Database.Postgres

  @types [
    {:text, "TEXT"},
    {:date, "DATE"},
    {:time, "TIME"},
    {:integer, "INT"},
    {:utc_datetime, "TIMESTAMP"},
    {:long, "BIGINT"},
    {:float, "REAL"},
    {:double, "DOUBLE PRECISION"},
    {:blob, "BYTEA"},
    {:uuid, "UUID"},
    {:boolean, "BOOLEAN"}
  ]

  describe "column_type" do
    for {abstract_type, specific_type} <- @types do
      @abstract_type abstract_type
      @specific_type specific_type

      test "translates #{inspect @abstract_type} to #{@specific_type}" do
        assert @test_module.column_type(@abstract_type) == @specific_type
      end

      test "translates nullable #{inspect @abstract_type} to #{@specific_type}" do
        assert @test_module.column_type({:nullable, @abstract_type}) == @specific_type
      end
    end
  end
end
