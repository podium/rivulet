defmodule Rivulet.Avro.Cache.Test do
  use ExUnit.Case

  @test_module Rivulet.Avro.Cache

  setup do
    name = :"cache_#{:erlang.unique_integer}"
    {:ok, cache} = @test_module.start_link(name)

    {:ok, %{cache: cache, name: name}}
  end

  test "lets you put and get things", %{name: name} do
    name
    |> @test_module.put(1, :first)
    |> @test_module.put(2, :second)
    |> @test_module.put(3, :third)

    assert @test_module.get(name, 1) == :first
    assert @test_module.get(name, 2) == :second
    assert @test_module.get(name, 3) == :third
  end

  test "loses all data after stop", %{cache: cache, name: name} do
    name
    |> @test_module.put(1, :first)
    |> @test_module.put(2, :second)
    |> @test_module.put(3, :third)

    @test_module.stop(cache)

    refute @test_module.get(name, 1) == :first
    refute @test_module.get(name, 2) == :second
    refute @test_module.get(name, 3) == :third
  end

  test "returns :no_cache if cache has been stopped", %{cache: cache, name: name} do
    @test_module.stop(cache)
    assert @test_module.get(name, :any) == :no_cache
  end
end
