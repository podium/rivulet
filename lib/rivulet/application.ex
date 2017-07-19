defmodule Rivulet.Application do
  use Application

  def start(_, _) do
    import Supervisor.Spec

    children = [
      supervisor(Registry, [:unique, Rivulet.Registry]),
      worker(Rivulet.Avro.Cache, []),
      worker(Rivulet.Pipeline, ["test-log", Rivulet.TestPipeline, TestPipeline, [0]])
    ]

    opts = [strategy: :one_for_one]

    Supervisor.start_link(children, opts)
  end
end
