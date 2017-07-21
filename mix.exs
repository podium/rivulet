defmodule Rivulet.Mixfile do
  use Mix.Project

  def project do
    [app: :rivulet,
     version: "0.1.0",
     elixir: "~> 1.3",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps(),
     dialyzer: [plt_add_deps: :apps_direct],
     aliases: aliases()]
  end

  def application do
    [applications: [:mix, :logger, :kafka_ex, :eavro, :httpoison, :gen_stage, :poison, :hackney],
     mod: {Rivulet.Application, []}]
  end

  defp aliases do
    ["compile": ["compile --warnings-as-errors"],
     "test": ["test --no-start", "dialyzer --halt-exit-status"]]
  end

  defp deps do
    [{:kafka_ex, "~> 0.6.5"},
     {:dialyxir, "~> 0.5.0", only: [:dev, :test], runtime: false},
     {:eavro, git: "git@github.com:podium/eavro", ref: "fix-complex-primitives", manager: :rebar},
     {:httpoison, "~> 0.12.0"},
     {:gen_stage, "~> 0.11.0"},
     {:poison, "~> 3.1.0"}]
  end
end
