defmodule Rivulet.Kafka.Stage.OffsetCommitter.Test do
  use ExUnit.Case
  use Rivulet.Mock

  @test_module Rivulet.Kafka.Stage.OffsetCommitter

  describe "highest offset" do
    test "returns the highest offset in a batch if the batch has an offset > initial" do
      actual =
        @test_module.highest_offset([%{offset: 1}, %{offset: 2}, %{offset: 3}], 2)

      assert actual == 3
    end

    test "returns the initial value if offsets are all lower" do
      actual =
        @test_module.highest_offset([%{offset: 1}, %{offset: 2}, %{offset: 3}], 4)

      assert actual == 4
    end

    test "returns the highest value if initial is nil" do
      actual =
        @test_module.highest_offset([%{offset: 1}], nil)

      assert actual == 1
    end
  end

  describe "handle_events" do
    test "returns highest offset if tracked offset is nil" do
      mock KafkaEx, :offset_commit, fn(_, _) -> :ok end do
        highest_offset = 10
        partition = %{topic: "fake-topic", partition: 0}
        initial_state = %@test_module.State{offset: nil, partition: partition}

        actual =
          @test_module.handle_events([%{offset: highest_offset}], :ignored, initial_state)

        assert {:noreply, _events, %@test_module.State{offset: ^highest_offset}} =
          actual
      end
    end

    test "returns highest offset if tracked offset is lower than highest" do
      mock KafkaEx, :offset_commit, fn(_, _) -> :ok end do
        highest_offset = 10
        partition = %{topic: "fake-topic", partition: 0}
        initial_state = %@test_module.State{offset: 9, partition: partition}

        actual =
          @test_module.handle_events([%{offset: highest_offset}], :ignored, initial_state)

        assert {:noreply, _events, %@test_module.State{offset: ^highest_offset}} =
          actual
      end
    end

    test "passes state through if highest offset is lower than tracked offset" do
      mock KafkaEx, :offset_commit, fn(_, _) -> :ok end do
        highest_offset = 10
        partition = %{topic: "fake-topic", partition: 0}
        initial_state = %@test_module.State{offset: 11, partition: partition}

        actual = @test_module.handle_events([%{offset: highest_offset}], :ignored, initial_state)

        assert {:noreply, _events, ^initial_state} =
          actual
      end
    end

    test "passes state through if tracked is nil and no events" do
      mock KafkaEx, :offset_commit, fn(_, _) -> :ok end do
        partition = %{topic: "fake-topic", partition: 0}
        initial_state = %@test_module.State{offset: nil, partition: partition}

        actual = @test_module.handle_events([], :ignored, initial_state)

        assert {:noreply, _events, ^initial_state} =
          actual
      end
    end
  end
end
