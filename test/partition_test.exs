defmodule Rivulet.Kafka.Partition.Test do
  use ExUnit.Case, async: false
  use Rivulet.Mock

  import ExUnit.CaptureLog

  @test_module Rivulet.Kafka.Partition

  @topic_name "fake-topic"
  @partitions [0, 1, 2]

  describe "partition count" do
    test "returns ok tuple if log found" do
      mock :brod, :get_partitions_count, fn(_, _) -> {:ok, 3} end do
        capture_log(fn ->
          assert {:ok, _} = @test_module.partition_count(@topic_name)
        end)
      end
    end

    test "returns the number of partitions if log found" do
      mock :brod, :get_partitions_count, fn(_, _) -> {:ok, 3} end do
        capture_log(fn ->
          assert {_, 3} = @test_module.partition_count(@topic_name)
        end)
      end
    end

    test "returns an error tuple if log not found" do
      mock :brod, :get_partitions_count, fn(_, _) -> {:error, "¯\_(ツ)_/¯"} end do
        capture_log(fn ->
          assert {:error, _, _} = @test_module.partition_count(@topic_name)
        end)
      end
    end

    test "returns an error message if log not found" do
      mock :brod, :get_partitions_count, fn(_, _) -> {:error, "¯\_(ツ)_/¯"} end do
        capture_log(fn ->
          assert {_, :topic_not_found, _} = @test_module.partition_count(@topic_name)
        end)
      end
    end

    test "returns topic name if log not found" do
      mock :brod, :get_partitions_count, fn(_, _) -> {:error, "¯\_(ツ)_/¯"} end do
        capture_log(fn ->
          assert {_, _, @topic_name} = @test_module.partition_count(@topic_name)
        end)
      end
    end
  end

  describe "random_partition" do
    test "returns ok tuple if log found" do
      mock :brod, :get_partitions_count, fn(_, _) -> {:ok, 3} end do
        capture_log(fn ->
          assert {:ok, _} = @test_module.random_partition(@topic_name)
        end)
      end
    end

    test "returns a random partition if log found" do
      mock :brod, :get_partitions_count, fn(_client, _topic) -> {:ok, 3} end do
        capture_log(fn ->
          {_, partition} = @test_module.random_partition(@topic_name)
          assert partition in @partitions
        end)
      end
    end

    test "returns an error tuple if log not found" do
      mock :brod, :get_partitions_count, fn(_, _) -> {:error, "¯\_(ツ)_/¯"} end do
        capture_log(fn ->
          assert {:error, _, _} = @test_module.random_partition(@topic_name)
        end)
      end
    end

    test "returns an error message if log not found" do
      mock :brod, :get_partitions_count, fn(_, _) -> {:error, "¯\_(ツ)_/¯"} end do
        capture_log(fn ->
          assert {_, :topic_not_found, _} = @test_module.random_partition(@topic_name)
        end)
      end
    end

    test "returns topic name if log not found" do
      mock :brod, :get_partitions_count, fn(_, _) -> {:error, "¯\_(ツ)_/¯"} end do
        capture_log(fn ->
          assert {_, _, @topic_name} = @test_module.random_partition(@topic_name)
        end)
      end
    end
  end

  describe "hashed_partition" do
    test "returns ok tuple if log found" do
      mock :brod, :get_partitions_count, fn(_, _) -> {:ok, 3} end do
        capture_log(fn ->
          assert {:ok, _} = @test_module.hashed_partition(@topic_name, "25")
        end)
      end
    end

    test "returns the same partition for the same key every time" do
      mock :brod, :get_partitions_count, fn(_, _) -> {:ok, 3} end do
        capture_log(fn ->
          {_, partition} = @test_module.hashed_partition(@topic_name, "25")

          for _ <- 0..100 do
            assert {_, ^partition} = @test_module.hashed_partition(@topic_name, "25")
          end
        end)
      end
    end

    test "returns an error tuple if log not found" do
      mock :brod, :get_partitions_count, fn(_, _) -> {:error, "¯\_(ツ)_/¯"} end do
        capture_log(fn ->
          assert {:error, _, _} = @test_module.hashed_partition(@topic_name, "25")
        end)
      end
    end

    test "returns an error message if log not found" do
      mock :brod, :get_partitions_count, fn(_, _) -> {:error, "¯\_(ツ)_/¯"} end do
        capture_log(fn ->
          assert {_, :topic_not_found, _} = @test_module.hashed_partition(@topic_name, "25")
        end)
      end
    end

    @tag :current
    test "returns topic name if log not found" do
      mock :brod, :get_partitions_count, fn(_, _) -> {:error, "¯\_(ツ)_/¯"} end do
        capture_log(fn ->
          assert {_, _, @topic_name} = @test_module.hashed_partition(@topic_name, "25")
        end)
      end
    end
  end
end
