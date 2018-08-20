defmodule Rivulet.JSON.Test do
  use ExUnit.Case

  @test_module  Rivulet.JSON

  describe "encode/1" do
    test "if no json_handler is set it should default to jiffy" do
      assert Rivulet.JSON.handler == :jiffy
    end
  end

end
