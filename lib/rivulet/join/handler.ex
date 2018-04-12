defmodule Rivulet.Join.Handler do
  use GenServer

  alias Rivulet.Join.ElasticSearch
  alias Rivulet.Kafka.Partition

  defmodule State do
    defstruct [:join_id, :transformers, :consumer]
  end

  def start(join_id, transformers, consumer) do
    GenServer.start(__MODULE__, [join_id, transformers, consumer])
  end

  def start_link(join_id, transformers, consumer) do
    GenServer.start_link(__MODULE__, [join_id, transformers, consumer])
  end

  def stop(ref) do
    GenServer.stop(ref)
  end

  def handle_resp(handler, join_keys, ack_data) do
    GenServer.call(handler, {:handle_resp, join_keys, ack_data}, 8_000)
  end

  def init([join_id, transformers, consumer]) do
    {:ok, %State{join_id: join_id, transformers: transformers, consumer: consumer}}
  end

  def pry_those_things(payload) do
    # IO.inspect(payload, label: "payload")
    # require IEx; IEx.pry
    payload
  end

  @doc """
  param1: {:handle_resp, ["5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5"], ?}
  param2: 8_000
  param3: %State{join_id: "nps-location-join", transformers: , consumer:}

  return from bulk_get_join_docs:

  res: Same as 'join_docs' from Funcs.transform/2

  thing_again:
    {"platform_nps_joins", 2, 1}
    {"platform_nps_joins", 6, 1}

  each_thing:
    {{"platform_nps_joins", 2}, 1}
    {{"platform_nps_joins", 6}, 1}

  partition:
    %Rivulet.Kafka.Partition{partition: 2, topic: "platform_nps_joins"}
    %Rivulet.Kafka.Partition{partition: 6, topic: "platform_nps_joins"}
  """
  def handle_call({:handle_resp, join_keys, ack_data}, from,  %State{join_id: join_id, transformers: transformers, consumer: consumer} = state) do
    GenServer.reply(from, :ok)

    res =
      join_id
      |> ElasticSearch.bulk_get_join_docs(join_keys)
      |> IO.inspect(label: "return from bulk_get_join_docs")
      # |> pry_those_things()
      |> Map.get("responses")
      |> Enum.map(fn(%{"hits" => %{"hits" => hits}}) -> hits end)
      # [[
      #     "g3QAAAAKbQAAAAdhZGRyZXNzbQAAAC0zMTkwIEF1dG8gQ2VudGVyIENpciwgU3RvY2t0b24sIENBIDk1MjEyLCBVU0FtAAAACGFyY2hpdmVkZAAFZmFsc2VtAAAACmNyZWF0ZWRfYXRtAAAAGzIwMTgtMDItMjRUMjI6NTU6NDIuNzA5Mjc0Wm0AAAAQbGFzdF9tb2RpZmllZF9hdG0AAAAbMjAxOC0wNC0xMVQyMTo1NTo0Mi43MDk4MThabQAAAARuYW1lbQAAAB5QYXVsIEJsYW5jbydzIEdvb2QgQ2FyIENvbXBhbnltAAAAEG9yZ2FuaXphdGlvbl91aWRtAAAAJGY0YWM0YmNiLWUyNzEtNWE5Mi04ZTQzLTFkNjc2YTg4MjFmYW0AAAANcG9kaXVtX251bWJlcm0AAAAMKzEzODUzMzYwMDYwbQAAABd0aW1lem9uZV9pZGVudGlmaWVyX3VpZGQAA25pbG0AAAADdWlkbQAAACQ1ZmQwM2JmOC05Y2Q2LTUyMGEtYjJlMy05MDg0Yjc4Y2IwYzVtAAAACnVwZGF0ZWRfYXRtAAAAGzIwMTgtMDQtMTFUMjE6NTU6NDIuNzM2MDUzWg==", "g3QAAAALbQAAAA5hZGp1c3RlZF9zY29yZWFkbQAAAApjcmVhdGVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADWN1c3RvbWVyX25hbWVtAAAABkRhblR3b20AAAAOY3VzdG9tZXJfcGhvbmVtAAAADCsxODUwNTg1NzYxN20AAAASaW52aXRhdGlvbl9zZW50X2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADGxvY2F0aW9uX3VpZG0AAAAQX9A7+JzWUgqy45CEt4ywxW0AAAALbnBzX2NvbW1lbnRtAAAACkNvbW1lbnRUd29tAAAAEm5wc19pbnZpdGF0aW9uX3VpZG0AAAAQwO4CNVBpV3OxHigKvKS8IG0AAAAQbnBzX3Jlc3BvbnNlX3VpZG0AAAAQ8DFcVLJyXpOG+ZiXwthgxW0AAAAJbnBzX3Njb3JlYQptAAAAFHJlc3BvbnNlX3JlY2VpdmVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVEM=", "g3QAAAALbQAAAA5hZGp1c3RlZF9zY29yZWFkbQAAAApjcmVhdGVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADWN1c3RvbWVyX25hbWVtAAAAA0Rhbm0AAAAOY3VzdG9tZXJfcGhvbmVtAAAADCsxODUwNTg1NzYxNm0AAAASaW52aXRhdGlvbl9zZW50X2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADGxvY2F0aW9uX3VpZG0AAAAQX9A7+JzWUgqy45CEt4ywxW0AAAALbnBzX2NvbW1lbnRtAAAAB0NvbW1lbnRtAAAAEm5wc19pbnZpdGF0aW9uX3VpZG0AAAAQeJaGv/pNVpqanw/WZJb6SG0AAAAQbnBzX3Jlc3BvbnNlX3VpZG0AAAAQY+oawijUUO2DlgVfkyazgG0AAAAJbnBzX3Njb3JlYQptAAAAFHJlc3BvbnNlX3JlY2VpdmVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVEM="
      #   ],[
      #     "g3QAAAAKbQAAAAdhZGRyZXNzbQAAAC0zMTkwIEF1dG8gQ2VudGVyIENpciwgU3RvY2t0b24sIENBIDk1MjEyLCBVU0FtAAAACGFyY2hpdmVkZAAFZmFsc2VtAAAACmNyZWF0ZWRfYXRtAAAAGzIwMTgtMDItMjRUMjI6NTU6NDIuNzA5Mjc0Wm0AAAAQbGFzdF9tb2RpZmllZF9hdG0AAAAbMjAxOC0wNC0xMVQyMTo1NTo0Mi43MDk4MThabQAAAARuYW1lbQAAAB5QYXVsIEJsYW5jbydzIEdvb2QgQ2FyIENvbXBhbnltAAAAEG9yZ2FuaXphdGlvbl91aWRtAAAAJGY0YWM0YmNiLWUyNzEtNWE5Mi04ZTQzLTFkNjc2YTg4MjFmYW0AAAANcG9kaXVtX251bWJlcm0AAAAMKzEzODUzMzYwMDYwbQAAABd0aW1lem9uZV9pZGVudGlmaWVyX3VpZGQAA25pbG0AAAADdWlkbQAAACQ1ZmQwM2JmOC05Y2Q2LTUyMGEtYjJlMy05MDg0Yjc4Y2IwYzVtAAAACnVwZGF0ZWRfYXRtAAAAGzIwMTgtMDQtMTFUMjE6NTU6NDIuNzM2MDUzWg==", "g3QAAAALbQAAAA5hZGp1c3RlZF9zY29yZWFkbQAAAApjcmVhdGVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADWN1c3RvbWVyX25hbWVtAAAABkRhblR3b20AAAAOY3VzdG9tZXJfcGhvbmVtAAAADCsxODUwNTg1NzYxN20AAAASaW52aXRhdGlvbl9zZW50X2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADGxvY2F0aW9uX3VpZG0AAAAQX9A7+JzWUgqy45CEt4ywxW0AAAALbnBzX2NvbW1lbnRtAAAACkNvbW1lbnRUd29tAAAAEm5wc19pbnZpdGF0aW9uX3VpZG0AAAAQwO4CNVBpV3OxHigKvKS8IG0AAAAQbnBzX3Jlc3BvbnNlX3VpZG0AAAAQ8DFcVLJyXpOG+ZiXwthgxW0AAAAJbnBzX3Njb3JlYQptAAAAFHJlc3BvbnNlX3JlY2VpdmVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVEM=", "g3QAAAALbQAAAA5hZGp1c3RlZF9zY29yZWFkbQAAAApjcmVhdGVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADWN1c3RvbWVyX25hbWVtAAAAA0Rhbm0AAAAOY3VzdG9tZXJfcGhvbmVtAAAADCsxODUwNTg1NzYxNm0AAAASaW52aXRhdGlvbl9zZW50X2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADGxvY2F0aW9uX3VpZG0AAAAQX9A7+JzWUgqy45CEt4ywxW0AAAALbnBzX2NvbW1lbnRtAAAAB0NvbW1lbnRtAAAAEm5wc19pbnZpdGF0aW9uX3VpZG0AAAAQeJaGv/pNVpqanw/WZJb6SG0AAAAQbnBzX3Jlc3BvbnNlX3VpZG0AAAAQY+oawijUUO2DlgVfkyazgG0AAAAJbnBzX3Njb3JlYQptAAAAFHJlc3BvbnNlX3JlY2VpdmVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVEM="
      #   ],[
      #     "g3QAAAAKbQAAAAdhZGRyZXNzbQAAAC0zMTkwIEF1dG8gQ2VudGVyIENpciwgU3RvY2t0b24sIENBIDk1MjEyLCBVU0FtAAAACGFyY2hpdmVkZAAFZmFsc2VtAAAACmNyZWF0ZWRfYXRtAAAAGzIwMTgtMDItMjRUMjI6NTU6NDIuNzA5Mjc0Wm0AAAAQbGFzdF9tb2RpZmllZF9hdG0AAAAbMjAxOC0wNC0xMVQyMTo1NTo0Mi43MDk4MThabQAAAARuYW1lbQAAAB5QYXVsIEJsYW5jbydzIEdvb2QgQ2FyIENvbXBhbnltAAAAEG9yZ2FuaXphdGlvbl91aWRtAAAAJGY0YWM0YmNiLWUyNzEtNWE5Mi04ZTQzLTFkNjc2YTg4MjFmYW0AAAANcG9kaXVtX251bWJlcm0AAAAMKzEzODUzMzYwMDYwbQAAABd0aW1lem9uZV9pZGVudGlmaWVyX3VpZGQAA25pbG0AAAADdWlkbQAAACQ1ZmQwM2JmOC05Y2Q2LTUyMGEtYjJlMy05MDg0Yjc4Y2IwYzVtAAAACnVwZGF0ZWRfYXRtAAAAGzIwMTgtMDQtMTFUMjE6NTU6NDIuNzM2MDUzWg==", "g3QAAAALbQAAAA5hZGp1c3RlZF9zY29yZWFkbQAAAApjcmVhdGVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADWN1c3RvbWVyX25hbWVtAAAABkRhblR3b20AAAAOY3VzdG9tZXJfcGhvbmVtAAAADCsxODUwNTg1NzYxN20AAAASaW52aXRhdGlvbl9zZW50X2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADGxvY2F0aW9uX3VpZG0AAAAQX9A7+JzWUgqy45CEt4ywxW0AAAALbnBzX2NvbW1lbnRtAAAACkNvbW1lbnRUd29tAAAAEm5wc19pbnZpdGF0aW9uX3VpZG0AAAAQwO4CNVBpV3OxHigKvKS8IG0AAAAQbnBzX3Jlc3BvbnNlX3VpZG0AAAAQ8DFcVLJyXpOG+ZiXwthgxW0AAAAJbnBzX3Njb3JlYQptAAAAFHJlc3BvbnNlX3JlY2VpdmVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVEM=", "g3QAAAALbQAAAA5hZGp1c3RlZF9zY29yZWFkbQAAAApjcmVhdGVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADWN1c3RvbWVyX25hbWVtAAAAA0Rhbm0AAAAOY3VzdG9tZXJfcGhvbmVtAAAADCsxODUwNTg1NzYxNm0AAAASaW52aXRhdGlvbl9zZW50X2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADGxvY2F0aW9uX3VpZG0AAAAQX9A7+JzWUgqy45CEt4ywxW0AAAALbnBzX2NvbW1lbnRtAAAAB0NvbW1lbnRtAAAAEm5wc19pbnZpdGF0aW9uX3VpZG0AAAAQeJaGv/pNVpqanw/WZJb6SG0AAAAQbnBzX3Jlc3BvbnNlX3VpZG0AAAAQY+oawijUUO2DlgVfkyazgG0AAAAJbnBzX3Njb3JlYQptAAAAFHJlc3BvbnNlX3JlY2VpdmVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVEM="
      #   ],[
      #     "g3QAAAAKbQAAAAdhZGRyZXNzbQAAAC0zMTkwIEF1dG8gQ2VudGVyIENpciwgU3RvY2t0b24sIENBIDk1MjEyLCBVU0FtAAAACGFyY2hpdmVkZAAFZmFsc2VtAAAACmNyZWF0ZWRfYXRtAAAAGzIwMTgtMDItMjRUMjI6NTU6NDIuNzA5Mjc0Wm0AAAAQbGFzdF9tb2RpZmllZF9hdG0AAAAbMjAxOC0wNC0xMVQyMTo1NTo0Mi43MDk4MThabQAAAARuYW1lbQAAAB5QYXVsIEJsYW5jbydzIEdvb2QgQ2FyIENvbXBhbnltAAAAEG9yZ2FuaXphdGlvbl91aWRtAAAAJGY0YWM0YmNiLWUyNzEtNWE5Mi04ZTQzLTFkNjc2YTg4MjFmYW0AAAANcG9kaXVtX251bWJlcm0AAAAMKzEzODUzMzYwMDYwbQAAABd0aW1lem9uZV9pZGVudGlmaWVyX3VpZGQAA25pbG0AAAADdWlkbQAAACQ1ZmQwM2JmOC05Y2Q2LTUyMGEtYjJlMy05MDg0Yjc4Y2IwYzVtAAAACnVwZGF0ZWRfYXRtAAAAGzIwMTgtMDQtMTFUMjE6NTU6NDIuNzM2MDUzWg==", "g3QAAAALbQAAAA5hZGp1c3RlZF9zY29yZWFkbQAAAApjcmVhdGVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADWN1c3RvbWVyX25hbWVtAAAABkRhblR3b20AAAAOY3VzdG9tZXJfcGhvbmVtAAAADCsxODUwNTg1NzYxN20AAAASaW52aXRhdGlvbl9zZW50X2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADGxvY2F0aW9uX3VpZG0AAAAQX9A7+JzWUgqy45CEt4ywxW0AAAALbnBzX2NvbW1lbnRtAAAACkNvbW1lbnRUd29tAAAAEm5wc19pbnZpdGF0aW9uX3VpZG0AAAAQwO4CNVBpV3OxHigKvKS8IG0AAAAQbnBzX3Jlc3BvbnNlX3VpZG0AAAAQ8DFcVLJyXpOG+ZiXwthgxW0AAAAJbnBzX3Njb3JlYQptAAAAFHJlc3BvbnNlX3JlY2VpdmVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVEM=", "g3QAAAALbQAAAA5hZGp1c3RlZF9zY29yZWFkbQAAAApjcmVhdGVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADWN1c3RvbWVyX25hbWVtAAAAA0Rhbm0AAAAOY3VzdG9tZXJfcGhvbmVtAAAADCsxODUwNTg1NzYxNm0AAAASaW52aXRhdGlvbl9zZW50X2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADGxvY2F0aW9uX3VpZG0AAAAQX9A7+JzWUgqy45CEt4ywxW0AAAALbnBzX2NvbW1lbnRtAAAAB0NvbW1lbnRtAAAAEm5wc19pbnZpdGF0aW9uX3VpZG0AAAAQeJaGv/pNVpqanw/WZJb6SG0AAAAQbnBzX3Jlc3BvbnNlX3VpZG0AAAAQY+oawijUUO2DlgVfkyazgG0AAAAJbnBzX3Njb3JlYQptAAAAFHJlc3BvbnNlX3JlY2VpdmVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVEM="
      #   ]]
      |> Enum.map(fn(hits) ->
        # [
        #   "g3QAAAAKbQAAAAdhZGRyZXNzbQAAAC0zMTkwIEF1dG8gQ2VudGVyIENpciwgU3RvY2t0b24sIENBIDk1MjEyLCBVU0FtAAAACGFyY2hpdmVkZAAFZmFsc2VtAAAACmNyZWF0ZWRfYXRtAAAAGzIwMTgtMDItMjRUMjI6NTU6NDIuNzA5Mjc0Wm0AAAAQbGFzdF9tb2RpZmllZF9hdG0AAAAbMjAxOC0wNC0xMVQyMTo1NTo0Mi43MDk4MThabQAAAARuYW1lbQAAAB5QYXVsIEJsYW5jbydzIEdvb2QgQ2FyIENvbXBhbnltAAAAEG9yZ2FuaXphdGlvbl91aWRtAAAAJGY0YWM0YmNiLWUyNzEtNWE5Mi04ZTQzLTFkNjc2YTg4MjFmYW0AAAANcG9kaXVtX251bWJlcm0AAAAMKzEzODUzMzYwMDYwbQAAABd0aW1lem9uZV9pZGVudGlmaWVyX3VpZGQAA25pbG0AAAADdWlkbQAAACQ1ZmQwM2JmOC05Y2Q2LTUyMGEtYjJlMy05MDg0Yjc4Y2IwYzVtAAAACnVwZGF0ZWRfYXRtAAAAGzIwMTgtMDQtMTFUMjE6NTU6NDIuNzM2MDUzWg==", "g3QAAAALbQAAAA5hZGp1c3RlZF9zY29yZWFkbQAAAApjcmVhdGVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADWN1c3RvbWVyX25hbWVtAAAABkRhblR3b20AAAAOY3VzdG9tZXJfcGhvbmVtAAAADCsxODUwNTg1NzYxN20AAAASaW52aXRhdGlvbl9zZW50X2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADGxvY2F0aW9uX3VpZG0AAAAQX9A7+JzWUgqy45CEt4ywxW0AAAALbnBzX2NvbW1lbnRtAAAACkNvbW1lbnRUd29tAAAAEm5wc19pbnZpdGF0aW9uX3VpZG0AAAAQwO4CNVBpV3OxHigKvKS8IG0AAAAQbnBzX3Jlc3BvbnNlX3VpZG0AAAAQ8DFcVLJyXpOG+ZiXwthgxW0AAAAJbnBzX3Njb3JlYQptAAAAFHJlc3BvbnNlX3JlY2VpdmVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVEM=", "g3QAAAALbQAAAA5hZGp1c3RlZF9zY29yZWFkbQAAAApjcmVhdGVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADWN1c3RvbWVyX25hbWVtAAAAA0Rhbm0AAAAOY3VzdG9tZXJfcGhvbmVtAAAADCsxODUwNTg1NzYxNm0AAAASaW52aXRhdGlvbl9zZW50X2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADGxvY2F0aW9uX3VpZG0AAAAQX9A7+JzWUgqy45CEt4ywxW0AAAALbnBzX2NvbW1lbnRtAAAAB0NvbW1lbnRtAAAAEm5wc19pbnZpdGF0aW9uX3VpZG0AAAAQeJaGv/pNVpqanw/WZJb6SG0AAAAQbnBzX3Jlc3BvbnNlX3VpZG0AAAAQY+oawijUUO2DlgVfkyazgG0AAAAJbnBzX3Njb3JlYQptAAAAFHJlc3BvbnNlX3JlY2VpdmVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVEM="
        # ]
        Enum.map(hits, fn(hit) ->
          hit["_source"]["document"]
        end)
      end)
      |> Enum.map(fn (docs) ->

        # [
        #   "g3QAAAAKbQAAAAdhZGRyZXNzbQAAAC0zMTkwIEF1dG8gQ2VudGVyIENpciwgU3RvY2t0b24sIENBIDk1MjEyLCBVU0FtAAAACGFyY2hpdmVkZAAFZmFsc2VtAAAACmNyZWF0ZWRfYXRtAAAAGzIwMTgtMDItMjRUMjI6NTU6NDIuNzA5Mjc0Wm0AAAAQbGFzdF9tb2RpZmllZF9hdG0AAAAbMjAxOC0wNC0xMVQyMTo1NTo0Mi43MDk4MThabQAAAARuYW1lbQAAAB5QYXVsIEJsYW5jbydzIEdvb2QgQ2FyIENvbXBhbnltAAAAEG9yZ2FuaXphdGlvbl91aWRtAAAAJGY0YWM0YmNiLWUyNzEtNWE5Mi04ZTQzLTFkNjc2YTg4MjFmYW0AAAANcG9kaXVtX251bWJlcm0AAAAMKzEzODUzMzYwMDYwbQAAABd0aW1lem9uZV9pZGVudGlmaWVyX3VpZGQAA25pbG0AAAADdWlkbQAAACQ1ZmQwM2JmOC05Y2Q2LTUyMGEtYjJlMy05MDg0Yjc4Y2IwYzVtAAAACnVwZGF0ZWRfYXRtAAAAGzIwMTgtMDQtMTFUMjE6NTU6NDIuNzM2MDUzWg==", "g3QAAAALbQAAAA5hZGp1c3RlZF9zY29yZWFkbQAAAApjcmVhdGVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADWN1c3RvbWVyX25hbWVtAAAABkRhblR3b20AAAAOY3VzdG9tZXJfcGhvbmVtAAAADCsxODUwNTg1NzYxN20AAAASaW52aXRhdGlvbl9zZW50X2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADGxvY2F0aW9uX3VpZG0AAAAQX9A7+JzWUgqy45CEt4ywxW0AAAALbnBzX2NvbW1lbnRtAAAACkNvbW1lbnRUd29tAAAAEm5wc19pbnZpdGF0aW9uX3VpZG0AAAAQwO4CNVBpV3OxHigKvKS8IG0AAAAQbnBzX3Jlc3BvbnNlX3VpZG0AAAAQ8DFcVLJyXpOG+ZiXwthgxW0AAAAJbnBzX3Njb3JlYQptAAAAFHJlc3BvbnNlX3JlY2VpdmVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVEM=", "g3QAAAALbQAAAA5hZGp1c3RlZF9zY29yZWFkbQAAAApjcmVhdGVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADWN1c3RvbWVyX25hbWVtAAAAA0Rhbm0AAAAOY3VzdG9tZXJfcGhvbmVtAAAADCsxODUwNTg1NzYxNm0AAAASaW52aXRhdGlvbl9zZW50X2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADGxvY2F0aW9uX3VpZG0AAAAQX9A7+JzWUgqy45CEt4ywxW0AAAALbnBzX2NvbW1lbnRtAAAAB0NvbW1lbnRtAAAAEm5wc19pbnZpdGF0aW9uX3VpZG0AAAAQeJaGv/pNVpqanw/WZJb6SG0AAAAQbnBzX3Jlc3BvbnNlX3VpZG0AAAAQY+oawijUUO2DlgVfkyazgG0AAAAJbnBzX3Njb3JlYQptAAAAFHJlc3BvbnNlX3JlY2VpdmVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVEM="
        # ]
        Enum.map(docs, fn (doc) ->
          # "g3QAAAALbQAAAA5hZGp1c3RlZF9zY29yZWFkbQAAAApjcmVhdGVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADWN1c3RvbWVyX25hbWVtAAAAA0Rhbm0AAAAOY3VzdG9tZXJfcGhvbmVtAAAADCsxODUwNTg1NzYxNm0AAAASaW52aXRhdGlvbl9zZW50X2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVENtAAAADGxvY2F0aW9uX3VpZG0AAAAQX9A7+JzWUgqy45CEt4ywxW0AAAALbnBzX2NvbW1lbnRtAAAAB0NvbW1lbnRtAAAAEm5wc19pbnZpdGF0aW9uX3VpZG0AAAAQeJaGv/pNVpqanw/WZJb6SG0AAAAQbnBzX3Jlc3BvbnNlX3VpZG0AAAAQY+oawijUUO2DlgVfkyazgG0AAAAJbnBzX3Njb3JlYQptAAAAFHJlc3BvbnNlX3JlY2VpdmVkX2F0dAAAAA1kAApfX3N0cnVjdF9fZAAPRWxpeGlyLkRhdGVUaW1lZAAIY2FsZW5kYXJkABNFbGl4aXIuQ2FsZW5kYXIuSVNPZAADZGF5YQtkAARob3VyYRVkAAttaWNyb3NlY29uZGgCYgAMVEBhBmQABm1pbnV0ZWE2ZAAFbW9udGhhBGQABnNlY29uZGE2ZAAKc3RkX29mZnNldGEAZAAJdGltZV96b25lbQAAAAdFdGMvVVRDZAAKdXRjX29mZnNldGEAZAAEeWVhcmIAAAfiZAAJem9uZV9hYmJybQAAAANVVEM="
          doc
          |> Base.decode64!
          |> :erlang.binary_to_term
        end)
      end)

    IO.inspect(res, label: "res")
    # require IEx; IEx.pry

    Rivulet.Kafka.Join.Funcs.transforms(res, transformers)

    ack_data
    |> Enum.reduce(%{}, fn
      ({topic, partition, offset} = thing_again, %{} = acks) ->
        IO.inspect(thing_again, label: "thing_again")
        # require IEx; IEx.pry

        Map.update(acks, {topic, partition}, offset, fn(prev_offset) ->
          if prev_offset > offset,
          do: prev_offset,
          else: offset
        end)
    end)
    |> Enum.each(fn({{topic, partition}, offset} = each_thing) ->
      # require IEx; IEx.pry
      IO.inspect(each_thing, label: "each_thing")

      partition = %Partition{topic: topic, partition: partition}

      IO.inspect(partition, label: "partition")
      # require IEx; IEx.pry

      Rivulet.Consumer.ack(consumer, partition, offset)
    end)

    {:noreply, state}
  end

  def handle_info(_, state) do
    {:noreply, state}
  end
end
