defmodule Rivulet.Kafka.Publisher.Test do
  use ExUnit.Case

  alias Rivulet.Kafka.Publish.Message

  @test_module Rivulet.Kafka.Publisher

  describe "group_messages/2" do
    test "removes nils" do
      assert %{} == @test_module.group_messages([nil, nil, nil])
    end

    test "groups messages with the same topic/partition number together (preserving order)" do
      msg1 = %Message{topic: "fake-topic", partition_strategy: 0, encoding_strategy: :raw, value: "abc"}
      msg2 = %Message{topic: "fake-topic", partition_strategy: 0, encoding_strategy: :raw, value: "def"}
      msg3 = %Message{topic: "fake-topic-2", partition_strategy: 1, encoding_strategy: :raw, value: "ghi"}

      assert %{
        {"fake-topic", 0} => [%Message{value: "abc"}, %Message{value: "def"}],
        {"fake-topic-2", 1} => [%Message{value: "ghi"}]
      } = @test_module.group_messages([msg1, msg2, msg3])
    end
  end
end
