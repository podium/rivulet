defmodule Rivulet.Mixfile do
  use Mix.Project

  def project do
    [app: :rivulet,
     version: "0.1.0",
     elixir: "~> 1.4",
     elixirc_paths: elixirc_paths(Mix.env),
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps(),
     dialyzer: [plt_add_deps: :app_direct, plt_add_apps: [:kafka_ex, :avro_ex], ignore_warnings: "dialyzer.ignore-warnings"],
     aliases: aliases()]
  end

  def application do
    if Mix.env == :test do
      [applications: [:mix, :logger, :httpoison, :gen_stage, :poison, :hackney, :meck],
       mod: {Rivulet.Application, []}]
    else
      [applications: [:mix, :logger, :httpoison, :gen_stage, :poison, :hackney],
       mod: {Rivulet.Application, []}]
    end
  end

  defp aliases do
    ["compile": ["compile --warnings-as-errors"],
     "test": ["test --no-start", "dialyzer --halt-exit-status"]]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [{:dialyxir, "~> 0.5.0", only: [:dev, :test], runtime: false},
     {:avro_ex, "~> 0.1.0-beta.0"},
     {:gen_stage, "~> 0.11.0"},
     {:httpoison, ">= 0.12.0"},
     {:kafka_ex, "~> 0.8.0"},
     {:meck, "~> 0.8.7", only: [:test]},
     {:poison, "~> 2.2 or ~> 3.1.0"}]
  end
end
