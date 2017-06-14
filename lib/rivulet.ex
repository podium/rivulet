defmodule Rivulet do
  use Application

  def start(_, _) do
    import Supervisor.Spec

    children = [
      supervisor(Registry, [:unique, Rivulet.Registry])
      #worker(Rivulet.TestPipeline, ["rivulet-testing", 0])
    ]

    opts = [strategy: :one_for_one]

    Supervisor.start_link(children, opts)
  end
end
