defmodule Rivulet.ElasticSearchSink.Test do
  use ExUnit.Case

  @test_module Rivulet.ElasticSearchSink

  setup do
    record = nil
    topic = "my-topic"
    config =
      %@test_module.Config{
        unique_constraints: [["a", "b"], ["a"]],
        primary_keys: ["c"],
        table_pattern: name_pattern,
        topic: topic
      }

    {:ok, %{config: config}}
  end

  describe "" do
    @tag :current
    test "gracefully fails in the event an index has already been created", context do
    end
  end
end
