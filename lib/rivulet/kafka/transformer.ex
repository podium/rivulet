defmodule Rivulet.Transformer do
  defmacro __using__(_) do
    quote do
      alias unquote(__MODULE__)
    end
  end

  alias Rivulet.Kafka.Publisher.Message

  @type key :: binary
  @type value :: binary

  @callback handle_message(Message.t)
  :: nil | {key, value} | [{key, value} | nil]
end
