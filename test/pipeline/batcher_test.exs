defmodule Rivulet.Pipeline.Batcher.Test do
  use ExUnit.Case

  @test_module Rivulet.Pipeline.Batcher

  describe "State :idle - event does not meet threshold" do
    setup do
      state =
        %{
          threshold: 4,
          events: [1,2,3],
          state: :idle
        }

      {:ok, state}
    end

    test "transitions to :filling",
    %{threshold: threshold, events: events, state: state} do
      msg = {:events, events, length(events)}
      data = %@test_module.Data{max: threshold, count: 0}

      assert {:next_state, :filling, _, _} =
        @test_module.handle_event({:call, :from}, msg, state, data)
    end

    test "replies with :continue",
    %{threshold: threshold, events: events, state: state} do
      msg = {:events, events, length(events)}
      data = %@test_module.Data{max: threshold, count: 0}
      from = :from

      {_, _, _, actions} =
        @test_module.handle_event({:call, from}, msg, state, data)

      assert {:reply, from, :continue} in actions
    end

    test "creates state timeout for :filling",
    %{threshold: threshold, events: events, state: state} do
      msg = {:events, events, length(events)}
      timeout = :timeout
      data = %@test_module.Data{max: threshold, count: 0, timeout: timeout}
      from = :from

      {_, _, _, actions} =
        @test_module.handle_event({:call, from}, msg, state, data)

      assert {:state_timeout, timeout, :timeout} in actions
    end

    test "appends new events to cached events",
    %{threshold: threshold, events: events, state: state} do
      msg = {:events, events, length(events)}
      data = %@test_module.Data{max: threshold + 1, count: 1, events: [4]}
      from = :from

      assert {_, _, %@test_module.Data{events: [4, 1, 2, 3], count: 4}, _} =
        @test_module.handle_event({:call, from}, msg, state, data)
    end
  end

  describe "State :idle - event does meet threshold" do
    setup do
      state =
        %{
          threshold: 3,
          events: [1,2,3],
          state: :idle
        }

      {:ok, state}
    end

    test "transitions to :full if events meets threshold",
    %{threshold: threshold, events: events, state: state} do
      msg = {:events, events, length(events)}
      data = %@test_module.Data{max: threshold, count: 0}

      assert {:next_state, :full, _, _} =
        @test_module.handle_event({:call, :from}, msg, state, data)
    end

    test "appends new events to cached events",
    %{threshold: threshold, events: events, state: state} do
      msg = {:events, events, length(events)}
      data = %@test_module.Data{max: threshold + 1, count: 1, events: [4]}
      from = :from

      assert {_, _, %@test_module.Data{events: [4, 1, 2, 3], count: 4}, _} =
        @test_module.handle_event({:call, from}, msg, state, data)
    end

    test "triggers an internal action for filled-ness",
    %{threshold: threshold, events: events, state: state} do
      msg = {:events, events, length(events)}
      data = %@test_module.Data{max: threshold + 1, count: 1, events: [4]}
      from = :from

      assert {_, _, _, actions} =
        @test_module.handle_event({:call, from}, msg, state, data)

      assert {:next_event, :internal, :full} in actions
    end
  end

  describe "State :filling - event does not meet threshold" do
    setup do
      state =
        %{
          threshold: 10,
          events: [1,2,3],
          state: :filling
        }

      {:ok, state}
    end

    test "transitions to :filling",
    %{threshold: threshold, events: events, state: state} do
      msg = {:events, events, length(events)}
      data = %@test_module.Data{max: threshold, count: 1, events: [1]}

      assert {:next_state, :filling, _, _} =
        @test_module.handle_event({:call, :from}, msg, state, data)
    end

    test "replies with :continue",
    %{threshold: threshold, events: events, state: state} do
      msg = {:events, events, length(events)}
      data = %@test_module.Data{max: threshold, count: 1, events: [1]}
      from = :from

      {_, _, _, actions} =
        @test_module.handle_event({:call, from}, msg, state, data)

      assert {:reply, from, :continue} in actions
    end

    test "appends new events to cached events",
    %{threshold: threshold, events: events, state: state} do
      msg = {:events, events, length(events)}
      data = %@test_module.Data{max: threshold + 1, count: 1, events: [4]}
      from = :from

      assert {_, _, %@test_module.Data{events: [4, 1, 2, 3], count: 4}, _} =
        @test_module.handle_event({:call, from}, msg, state, data)
    end
  end

  describe "State :filling - event does meet threshold" do
    setup do
      state =
        %{
          threshold: 3,
          events: [1,2,3],
          state: :filling
        }

      {:ok, state}
    end

    test "transitions to :full if events meets threshold",
    %{threshold: threshold, events: events, state: state} do
      msg = {:events, events, length(events)}
      data = %@test_module.Data{max: threshold, count: 1, events: [1]}

      assert {:next_state, :full, _, _} =
        @test_module.handle_event({:call, :from}, msg, state, data)
    end

    test "appends new events to cached events",
    %{threshold: threshold, events: events, state: state} do
      msg = {:events, events, length(events)}
      data = %@test_module.Data{max: threshold + 1, count: 1, events: [4]}
      from = :from

      assert {_, _, %@test_module.Data{events: [4, 1, 2, 3], count: 4}, _} =
        @test_module.handle_event({:call, from}, msg, state, data)
    end

    test "triggers an internal action for filled-ness",
    %{threshold: threshold, events: events, state: state} do
      msg = {:events, events, length(events)}
      data = %@test_module.Data{max: threshold + 1, count: 1, events: [4]}
      from = :from

      assert {_, _, _, actions} =
        @test_module.handle_event({:call, from}, msg, state, data)

      assert {:next_event, :internal, :full} in actions
    end
  end

  describe ":filling state timeout" do
    test "transitions to :idle and sends events to notify process" do
      events = [1, 2, 3]
      data = %@test_module.Data{from: :from, events: events, count: 3, notify: self()}

      assert {:next_state, :idle, _} =
        @test_module.handle_event(:state_timeout, :timeout, :filling, data)

      assert_receive ^events, "Did not receive events"
    end
  end

  describe "interface" do
    test "works as expected" do
      events = [1, 2, 3]
      {:ok, pid} = Rivulet.Pipeline.Batcher.start_link(4, :timer.minutes(1))

      assert Rivulet.Pipeline.Batcher.events(pid, events) == :continue

      expected_events = [1,2,3,1,2,3]

      assert {:events, ^expected_events} = Rivulet.Pipeline.Batcher.events(pid, events)
    end
  end
end
