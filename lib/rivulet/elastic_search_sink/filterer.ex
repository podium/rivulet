defmodule Rivulet.ElasticSearchSink.Filterer do
  alias Rivulet.Kafka.Consumer.Message

  def filter_for_successfully_inserted(%{"errors" => false} = _es_response, records) do
    Enum.map(records, &(&1.raw_value))
  end
  def filter_for_successfully_inserted(%{"errors" => true, "items" => es_items} = _es_response, records) do
    zipped = Enum.zip(es_items, records)
    successful_msgs = Enum.filter(zipped, &message_successfully_inserted?/1)
    Enum.map(successful_msgs, fn ({_, %Message{raw_value: val}}) -> val end)
  end

  def message_successfully_inserted?({%{"index" => doc} = _es_resp, _}) do
    status_ok?(doc)
  end
  def message_successfully_inserted?({%{"update" => doc} = _es_resp, _}) do
    status_ok?(doc)
  end
  def message_successfully_inserted?({%{"create" => doc} = _es_resp, _}) do
    status_ok?(doc)
  end
  def message_successfully_inserted?({%{"delete" => doc} = _es_resp, _}) do
    status_ok?(doc)
  end

  def status_ok?(%{"status" => status_code}) when status_code == 201 do
    true
  end
  def status_ok?(%{"status" => status_code}) when status_code == 201 do
    false
  end
end
