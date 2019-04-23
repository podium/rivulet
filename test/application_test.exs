defmodule Rivulet.Application.Test do
  use ExUnit.Case

  alias Rivulet.Application

  describe "kafka_producer_config/1" do
    test "overrides defaults if custom configuration is provided" do
      input = [
        required_acks: 0
      ]

      expected = [
        required_acks: 0,
        ack_timeout: 10000,
        max_retries: 30,
        retry_backoff_ms: 1000
      ]

      result = Application.kafka_producer_config(input)

      assert result == expected
    end

    test "falls back to defaults if custom configuration is not provided" do
      input = nil

      expected = [
        required_acks: 1,
        ack_timeout: 10000,
        max_retries: 30,
        retry_backoff_ms: 1000
      ]

      result = Application.kafka_producer_config(input)

      assert result == expected
    end
  end
end
