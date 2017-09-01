defmodule Rivulet.Pipeline.Batcher do
  @moduledoc false

  @behaviour :gen_statem

  defmodule Data do
    defstruct [:from, :max, :timeout, :notify, count: 0, events: []]
  end

  # Public API

  def start_link(max, timeout \\ 500, notify \\ self()) do # 500 ms default
    :gen_statem.start_link(__MODULE__, {max, timeout, notify}, [])
  end

  def events(batcher, events) when is_list(events) do
    count = length(events)
    :gen_statem.call(batcher, {:events, events, count})
  end

  # Callback Functions

  def init({max, timeout, notify}) do
    data = %Data{max: max, timeout: timeout, notify: notify}
    {:ok, :idle, data}
  end

  def callback_mode, do: :handle_event_function

  def handle_event({:call, from}, {:events, events, count}, :idle, %Data{max: max, count: current_count} = data)
  when is_list(events) and count >= 0  and count + current_count < max do
    data = cache_events(data, events, count)
    {:next_state, :filling, data, [{:reply, from, :continue}, {:state_timeout, data.timeout, :timeout}]}
  end

  def handle_event({:call, from}, {:events, events, count}, :idle, %Data{max: max, count: current_count} = data)
  when is_list(events) and count >= 0  and count + current_count >= max do
    data = cache_events(data, events, count)
    {:next_state, :full, %Data{data | from: from}, [{:next_event, :internal, :full}]}
  end

  def handle_event({:call, from}, {:events, events, count}, :filling, %Data{max: max, count: current_count} = data)
  when is_list(events) and count >= 0  and count + current_count < max do
    data = cache_events(data, events, count)
    {:next_state, :filling, data, [{:reply, from, :continue}]}
  end

  def handle_event({:call, from}, {:events, events, count}, :filling, %Data{max: max, count: current_count} = data)
  when is_list(events) and count >= 0  and count + current_count >= max do
    data = cache_events(data, events, count)
    {:next_state, :full, %Data{data | from: from}, [{:next_event, :internal, :full}]}
  end

  def handle_event(:internal, :full, :full, %Data{from: from} = data) do
    events = data.events
    data = %Data{data | events: [], count: 0, from: nil}

    actions = [
      {:reply, from, {:events, events}}
    ]

    {:next_state, :idle, data, actions}
  end

  def handle_event(:state_timeout, :timeout, :filling, %Data{notify: notify} = data) do
    events = data.events
    data = %Data{data | events: [], count: 0}

    send(notify, {:events, events})

    {:next_state, :idle, data}
  end

  def handle_event(:cast, :idle, :full, %Data{} = data) do
    {:next_state, :idle, data}
  end

  def handle_event(:enter, :filling, :filling, %Data{} = data) do
    {:next_state, :filling, data}
  end

  def handle_event(:enter, _, :filling, %Data{} = data) do
    {:next_state, :filling, data, [{:state_timeout, data.timeout, :timeout}]}
  end

  def handle_event(:enter, _, state, %Data{} = data) do
    {:next_state, state, data}
  end

  def terminate(_reason, _state, _data), do: :ok

  # Private Functions

  defp cache_events(%Data{events: cached_events, count: current_count} = data, events, count)  do
    cached_events = cached_events ++ events
    %Data{data | events: cached_events, count: current_count + count}
  end
end
