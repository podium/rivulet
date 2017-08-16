defmodule Rivulet.Kafka.Partition.Test do
  use ExUnit.Case, async: false
  use Rivulet.Mock

  @test_module Rivulet.Kafka.Partition

  @topic_name "fake-topic"
  @partitions [0, 1, 2]

  describe "topic_metadata" do
    test "returns ok tuple if log found" do
      mock KafkaEx, :metadata, fn -> %{topic_metadatas: [%{topic: @topic_name}]} end do
        assert {:ok, _} = @test_module.topic_metadata(@topic_name)
      end
    end

    test "returns the topic struct" do
      topic = %{topic: @topic_name}
      mock KafkaEx, :metadata, fn -> %{topic_metadatas: [topic]} end do
        assert {_, ^topic} = @test_module.topic_metadata(@topic_name)
      end
    end
  end

  describe "partition count" do
    test "returns ok tuple if log found" do
      mock KafkaEx, :metadata, fn -> %{topic_metadatas: [%{topic: @topic_name, partition_metadatas: @partitions}]} end do
        assert {:ok, _} = @test_module.partition_count(@topic_name)
      end
    end

    test "returns the number of partitions if log found" do
      mock KafkaEx, :metadata, fn -> %{topic_metadatas: [%{topic: @topic_name, partition_metadatas: @partitions}]} end do
        assert {_, 3} = @test_module.partition_count(@topic_name)
      end
    end

    test "returns an error tuple if log not found" do
      mock KafkaEx, :metadata, fn -> %{topic_metadatas: []} end do
        assert {:error, _, _} = @test_module.partition_count(@topic_name)
      end
    end

    test "returns an error message if log not found" do
      mock KafkaEx, :metadata, fn -> %{topic_metadatas: []} end do
        assert {_, :topic_not_found, _} = @test_module.partition_count(@topic_name)
      end
    end

    test "returns topic name if log not found" do
      mock KafkaEx, :metadata, fn -> %{topic_metadatas: []} end do
        assert {_, _, @topic_name} = @test_module.partition_count(@topic_name)
      end
    end
  end

  describe "random_partition" do
    test "returns ok tuple if log found" do
      mock KafkaEx, :metadata, fn -> %{topic_metadatas: [%{topic: @topic_name, partition_metadatas: @partitions}]} end do
        assert {:ok, _} = @test_module.random_partition(@topic_name)
      end
    end

    test "returns a random partition if log found" do
      mock KafkaEx, :metadata, fn -> %{topic_metadatas: [%{topic: @topic_name, partition_metadatas: @partitions}]} end do
        {_, partition} = @test_module.random_partition(@topic_name)
        assert partition in @partitions
      end
    end

    test "returns an error tuple if log not found" do
      mock KafkaEx, :metadata, fn -> %{topic_metadatas: []} end do
        assert {:error, _, _} = @test_module.random_partition(@topic_name)
      end
    end

    test "returns an error message if log not found" do
      mock KafkaEx, :metadata, fn -> %{topic_metadatas: []} end do
        assert {_, :topic_not_found, _} = @test_module.random_partition(@topic_name)
      end
    end

    test "returns topic name if log not found" do
      mock KafkaEx, :metadata, fn -> %{topic_metadatas: []} end do
        assert {_, _, @topic_name} = @test_module.random_partition(@topic_name)
      end
    end
  end

  describe "hashed_partition" do
    test "returns ok tuple if log found" do
      mock KafkaEx, :metadata, fn -> %{topic_metadatas: [%{topic: @topic_name, partition_metadatas: @partitions}]} end do
        assert {:ok, _} = @test_module.hashed_partition(@topic_name, "25")
      end
    end

    test "returns the same partition for the same key every time" do
      mock KafkaEx, :metadata, fn -> %{topic_metadatas: [%{topic: @topic_name, partition_metadatas: @partitions}]} end do
          {_, partition} = @test_module.hashed_partition(@topic_name, "25")

        for _ <- 0..100 do
          assert {_, ^partition} = @test_module.hashed_partition(@topic_name, "25")
        end
      end
    end

    test "returns an error tuple if log not found" do
      mock KafkaEx, :metadata, fn -> %{topic_metadatas: []} end do
        assert {:error, _, _} = @test_module.hashed_partition(@topic_name, "25")
      end
    end

    test "returns an error message if log not found" do
      mock KafkaEx, :metadata, fn -> %{topic_metadatas: []} end do
        assert {_, :topic_not_found, _} = @test_module.hashed_partition(@topic_name, "25")
      end
    end

    test "returns topic name if log not found" do
      mock KafkaEx, :metadata, fn -> %{topic_metadatas: []} end do
        assert {_, _, @topic_name} = @test_module.hashed_partition(@topic_name, "25")
      end
    end
  end
end
