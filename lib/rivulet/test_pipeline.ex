defmodule Rivulet.TestPipeline do
  use Supervisor

  def start_link(topic, partition) do
    Supervisor.start_link(__MODULE__, {topic, partition})
  end

  def init({topic, partition}) do
    children =
      [
        worker(Rivulet.Kafka.Producer, [topic, partition, 5]),
        worker(Rivulet.EventPrinter, [topic, partition])
      ]

    options = [strategy: :one_for_all]

    supervise(children, options)
  end
end
