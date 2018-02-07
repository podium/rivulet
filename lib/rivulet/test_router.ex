defmodule Rivulet.TestRouter do
  use Rivulet.Kafka.Router,
    consumer_group: "test-rivulet"

  defsource "test-log" do
    transformer Rivulet.TestTransformer do
      publish_to "test-log-2", partition: :key
    end

    transformer Rivulet.TestTransformer2 do
      publish_to "test-log-2", partition: :key
    end
  end
end 
