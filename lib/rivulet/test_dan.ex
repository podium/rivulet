defmodule Rivulet.TestDan do
  use Rivulet.ElasticSearchSink, otp_app: :rivulet

  def init(_, config) do
    {:ok, Keyword.put(config, :elastic_url, "http://elasticsearch:9200")}
  end
end
