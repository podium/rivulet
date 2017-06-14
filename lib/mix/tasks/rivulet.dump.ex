defmodule Mix.Tasks.Rivulet.Dump do
  use Mix.Task

  @shortdoc "plz 2 halp?"
  def run(_) do
    Application.ensure_all_started(:kafka_ex)

    events = [
      %{
        "type" => "CreateThing",
        "version" => 1,
        "data" => %{
          "name" => "Paul Blanco's Good Car Company 1",
        }
      },
      %{
        "type" => "CreateThing",
        "version" => 1,
        "data" => %{
          "name" => "Paul Blanco's Good Car Company 2",
        }
      },
      %{
        "type" => "CreateThing",
        "version" => 1,
        "data" => %{
          "name" => "Paul Blanco's Good Car Company 3",
        }
      },
      %{
        "type" => "CreateThing",
        "version" => 1,
        "data" => %{
          "name" => "Paul Blanco's Good Car Company 4",
        }
      },
      %{
        "type" => "CreateThing",
        "version" => 1,
        "data" => %{
          "name" => "Paul Blanco's Good Car Company 5",
        }
      }
    ]

    Enum.map(events, fn(event) ->
      KafkaEx.produce("rivulet-testing", 0, event |> Poison.encode!)
    end)
  end
end
