defmodule Rivulet.Mixfile do
  use Mix.Project

  def project do
    [app: :rivulet,
     version: "0.1.0",
     elixir: "~> 1.3",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps(),
     aliases: aliases()]
  end

  def application do
    [applications: [:logger, :kafka_ex],
     mod: {Rivulet, []}]
  end

  defp aliases do
    ["compile": ["compile --warnings-as-errors"]]
  end

  defp deps do
    [{:kafka_ex, "~> 0.6.5"},
     {:gen_stage, "~> 0.11.0"},
     {:poison, "~> 3.1.0"}]
  end
end
