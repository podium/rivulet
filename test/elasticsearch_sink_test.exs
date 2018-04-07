defmodule Rivulet.ElasticSearchSink.Test do
  use ExUnit.Case

  @test_module Rivulet.ElasticSearchSink

  setup do
    topic = "my-topic"
    config =
      %@test_module.Config{
        unique_constraints: [["a", "b"], ["a"]],
        primary_keys: ["c"],
        table_pattern: name_pattern,
        topic: topic
      }

    {:ok, %{schema: record, config: config}}
  end

  describe "ensure_index_and_mapping_created" do
    @tag :current
    test "does not create an ES index if one does already exist", context do
    end
  end
end
