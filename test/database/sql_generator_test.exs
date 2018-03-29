defmodule Rivulet.SQLSink.Database.SQLGenerator.Test do
  use ExUnit.Case

  @test_module Rivulet.SQLSink.Database.SQLGenerator

  alias Rivulet.SQLSink.Database.Table

  describe "column_definition" do
    test "nullable primary key" do
      assert_raise NullablePrimaryKeyException, fn ->
        @test_module.column_definition("field1", :nullable, :integer, _primary_key = true)
      end
    end

    test "not nullable primary key" do
      assert "field1 INT PRIMARY KEY" =
        @test_module.column_definition("field1", :not_nullable, :integer, _primary_key = true)
    end

    test "nullable field" do
      assert "field1 INT" =
        @test_module.column_definition("field1", :nullable, :integer, _primary_key = false)
    end

    test "not nullable field" do
      assert "field1 INT NOT NULL" =
        @test_module.column_definition("field1", :not_nullable, :integer, _primary_key = false)
    end
  end

  describe "create_table" do
    test "handles sequence primary key correctly" do
      expected =
        [
          "CREATE TABLE IF NOT EXISTS \"abcs\"\n(id BIGINT PRIMARY KEY,\nfield1 INT NOT NULL);\n",
          ["CREATE SEQUENCE abcs_seq\nSTART WITH 1\nINCREMENT BY 1\nNO MINVALUE\nNO MAXVALUE\nCACHE 1;\n",
           "ALTER SEQUENCE abcs_seq OWNED BY abcs.id;\n",
           "ALTER TABLE ONLY abcs ALTER COLUMN id SET DEFAULT nextval('abcs_seq'::regclass);\n"],
          []
        ]

      table =
        %Table{
          database: :postgres,
          name: "abcs",
          primary_keys: :sequence,
          column_definitions: [
            {"field1", :integer}
          ]
        }

      assert ^expected = @test_module.create_table(table)
    end

    test "handles unique constraints correctly" do
      expected =
        [
          "CREATE TABLE IF NOT EXISTS \"abcs\"\n(id BIGINT PRIMARY KEY,\nfield1 INT NOT NULL);\n",
          ["CREATE SEQUENCE abcs_seq\nSTART WITH 1\nINCREMENT BY 1\nNO MINVALUE\nNO MAXVALUE\nCACHE 1;\n",
           "ALTER SEQUENCE abcs_seq OWNED BY abcs.id;\n",
           "ALTER TABLE ONLY abcs ALTER COLUMN id SET DEFAULT nextval('abcs_seq'::regclass);\n"],
          ["CREATE UNIQUE INDEX IF NOT EXISTS \"abcs_field1_index\" ON \"abcs\"\n(field1);\n"]
        ]

      table =
        %Table{
          database: :postgres,
          name: "abcs",
          primary_keys: :sequence,
          unique_constraints: [["field1"]],
          column_definitions: [
            {"field1", :integer}
          ]
        }

      assert ^expected = @test_module.create_table(table)
    end
  end
end
