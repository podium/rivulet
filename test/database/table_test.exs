defmodule Rivulet.SQLSink.Database.Table.Test do
  use ExUnit.Case

  @test_module Rivulet.SQLSink.Database.Table

  @types [
    :boolean,
    :integer,
    :date,
    :time,
    :utc_datetime,
    :long,
    :float,
    :double,
    :uuid,
    {:varchar, 254},
    {:varchar, 255},
    :text,
    :blob
  ]

  for left <- @types do
    for right <- @types do
      @left left
      @right right
      @winner Enum.find(@types, &(&1 == @left or &1 == @right))

      test "#{inspect @left} vs #{inspect @right} - should be #{inspect @winner}" do
        assert (@winner == @left) == @test_module.sort_types(@left, @right)
      end
    end
  end
end
