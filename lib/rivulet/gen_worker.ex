defmodule Rivulet.GenWorker do
  @doc """
  Callback that should implement task business logic that must be securely processed.
  """
  @callback run(worker_args :: term()) :: worker_args :: term()

  @doc false
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts], location: :keep do
      @behaviour Rivulet.GenWorker
      @options opts

      alias Rivulet.GenWorker.State

      @doc """
      Start GenServer
      """
      defdelegate start_link(opts), to: Rivulet.GenWorker.Supervisor

      # def start_link(params \\ nil) do
      #   state = @options
      #     |> Keyword.put(:caller, __MODULE__)
      #     |> Keyword.put(:worker_args, params)
      #     |> State.init!()
      #
      #   GenServer.start_link(Rivulet.GenWorker.Server, state, name: __MODULE__)
      # end

      @doc false
      def run(_params) do
        raise "Behaviour function #{__MODULE__}.run/1 is not implemented!"
      end

      defoverridable [run: 1]
    end
  end
end
end
