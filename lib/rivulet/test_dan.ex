defmodule Rivulet.TestDan do
  use Rivulet.ElasticSearchSink, otp_app: :rivulet

  require Logger

  @elastic_mapping %{
    properties: %{
      invitation_sent_at: %{
        type: "date",
        format: "date_optional_time"
      },
      response_received_at: %{
        type: "date",
        format: "date_optional_time"
      },
      nps_comment: %{
        type: "text",
        copy_to: "_search",
        index: "false"
      },
      nps_score: %{
        type: "short"
      },
      adjusted_score: %{
        type: "short"
      },
      customer_name: %{
        type: "text",
        copy_to: "_search",
        index: false
      },
      customer_phone: %{
        type: "text"
      },
      location_name: %{
        type: "text",
        fields: %{
          raw: %{
            type: "keyword"
          }
        }
      },
      location_address: %{
        type: "text",
        fields: %{
          raw: %{
            type: "keyword"
          }
        }
      },
      conversation_uid: %{
        type: "keyword"
      },
      location_uid: %{
        type: "keyword"
      },
      organization_uid: %{
        type: "keyword"
      },
      created_at: %{
        type: "date",
        format: "date_optional_time"
      },
      _search: %{
        type: "text"
      }
    }
  }

  def init(_, config) do
    with {:ok, config_with_mapping} <- configure_mapping(config),
     do: {:ok, Keyword.put(config_with_mapping, :elastic_url, "http://localhost:9200")}
  end

  defp configure_mapping(config) do
    {:ok, Keyword.put(config, :elastic_mapping, @elastic_mapping)}
  end

  def on_complete(_successfully_inserted_messages) do
    Logger.info("on_complete callback fired for _successfully_inserted_messages")
  end
end
