defmodule Rivulet do
  @spec client_name!() :: term | no_return
  def client_name!() do
    client_name = Application.get_env(:rivulet, :client_name)

    unless client_name do
      raise "Application.get_env(:rivulet, :client_name) not configured"
    end

    client_name
  end

  @spec client_name() :: nil | pid
  def client_name do
    Application.get_env(:rivulet, :client_name)
  end
end
