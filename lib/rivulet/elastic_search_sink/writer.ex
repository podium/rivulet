defmodule Rivulet.ElasticSearchSink.Writer do
  @moduledoc """
  This module is an abstraction that represents the functionality
  each separate Writer process (i.e., each process that will do some database
  operations within BlackMamba, etc.) will need to successfully write to
  a Postgres database.
  """
  require Logger
  alias Rivulet.JSON
  alias Rivulet.Kafka.Partition
  alias Rivulet.Kafka.Consumer.Message
  alias Rivulet.ElasticSearchSink.{Config, Filterer}
  alias Elastix.Bulk

  @type index_name :: String.t
  @type type_name :: String.t
  @type mapping :: %{properties: %{}}
  @type exception :: any()
  @type action_type :: String.t
  @type record :: %{}
  @type index_record :: []
  @type upserted_record :: []
  @type query :: %{}

  def start_link(%Config{} = config) do
    GenServer.start_link(__MODULE__, {config})
  end

  def init({config}) do
    {:ok, config}
  end

  @doc """
  The pid here is the identifier for this particular Writer process
  """
  def handle_messages(pid, %Partition{} = partition, messages) do
    GenServer.cast(pid, {:handle_messages, partition, messages})
  end

  @doc """
  messages: [
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 2426213254,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 0,
    raw_key: "80a27c8e-8bad-5799-8f21-26b1a3ad0e6e",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":9,\"nps_response_uid\":\"c3c22ff9-4dfe-5a40-95dc-02f7ec857350\",\"nps_invitation_uid\":\"80a27c8e-8bad-5799-8f21-26b1a3ad0e6e\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.297000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.297000Z\",\"adjusted_score\":100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 2426213254,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 1,
    raw_key: "80a27c8e-8bad-5799-8f21-26b1a3ad0e6e",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":9,\"nps_response_uid\":\"c3c22ff9-4dfe-5a40-95dc-02f7ec857350\",\"nps_invitation_uid\":\"80a27c8e-8bad-5799-8f21-26b1a3ad0e6e\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.297000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.297000Z\",\"adjusted_score\":100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 2426213254,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 2,
    raw_key: "80a27c8e-8bad-5799-8f21-26b1a3ad0e6e",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":9,\"nps_response_uid\":\"c3c22ff9-4dfe-5a40-95dc-02f7ec857350\",\"nps_invitation_uid\":\"80a27c8e-8bad-5799-8f21-26b1a3ad0e6e\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.297000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.297000Z\",\"adjusted_score\":100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 2426213254,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 3,
    raw_key: "80a27c8e-8bad-5799-8f21-26b1a3ad0e6e",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":9,\"nps_response_uid\":\"c3c22ff9-4dfe-5a40-95dc-02f7ec857350\",\"nps_invitation_uid\":\"80a27c8e-8bad-5799-8f21-26b1a3ad0e6e\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.297000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.297000Z\",\"adjusted_score\":100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 2426213254,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 4,
    raw_key: "80a27c8e-8bad-5799-8f21-26b1a3ad0e6e",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":9,\"nps_response_uid\":\"c3c22ff9-4dfe-5a40-95dc-02f7ec857350\",\"nps_invitation_uid\":\"80a27c8e-8bad-5799-8f21-26b1a3ad0e6e\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.297000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.297000Z\",\"adjusted_score\":100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 2426213254,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 5,
    raw_key: "80a27c8e-8bad-5799-8f21-26b1a3ad0e6e",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":9,\"nps_response_uid\":\"c3c22ff9-4dfe-5a40-95dc-02f7ec857350\",\"nps_invitation_uid\":\"80a27c8e-8bad-5799-8f21-26b1a3ad0e6e\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.297000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.297000Z\",\"adjusted_score\":100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 2426213254,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 6,
    raw_key: "80a27c8e-8bad-5799-8f21-26b1a3ad0e6e",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":9,\"nps_response_uid\":\"c3c22ff9-4dfe-5a40-95dc-02f7ec857350\",\"nps_invitation_uid\":\"80a27c8e-8bad-5799-8f21-26b1a3ad0e6e\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.297000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.297000Z\",\"adjusted_score\":100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 2426213254,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 7,
    raw_key: "80a27c8e-8bad-5799-8f21-26b1a3ad0e6e",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":9,\"nps_response_uid\":\"c3c22ff9-4dfe-5a40-95dc-02f7ec857350\",\"nps_invitation_uid\":\"80a27c8e-8bad-5799-8f21-26b1a3ad0e6e\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.297000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.297000Z\",\"adjusted_score\":100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 2426213254,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 8,
    raw_key: "80a27c8e-8bad-5799-8f21-26b1a3ad0e6e",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":9,\"nps_response_uid\":\"c3c22ff9-4dfe-5a40-95dc-02f7ec857350\",\"nps_invitation_uid\":\"80a27c8e-8bad-5799-8f21-26b1a3ad0e6e\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.297000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.297000Z\",\"adjusted_score\":100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 2426213254,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 9,
    raw_key: "80a27c8e-8bad-5799-8f21-26b1a3ad0e6e",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":9,\"nps_response_uid\":\"c3c22ff9-4dfe-5a40-95dc-02f7ec857350\",\"nps_invitation_uid\":\"80a27c8e-8bad-5799-8f21-26b1a3ad0e6e\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.297000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.297000Z\",\"adjusted_score\":100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 2426213254,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 10,
    raw_key: "80a27c8e-8bad-5799-8f21-26b1a3ad0e6e",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":9,\"nps_response_uid\":\"c3c22ff9-4dfe-5a40-95dc-02f7ec857350\",\"nps_invitation_uid\":\"80a27c8e-8bad-5799-8f21-26b1a3ad0e6e\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.297000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.297000Z\",\"adjusted_score\":100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 2426213254,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 11,
    raw_key: "80a27c8e-8bad-5799-8f21-26b1a3ad0e6e",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":9,\"nps_response_uid\":\"c3c22ff9-4dfe-5a40-95dc-02f7ec857350\",\"nps_invitation_uid\":\"80a27c8e-8bad-5799-8f21-26b1a3ad0e6e\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.297000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.297000Z\",\"adjusted_score\":100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 2426213254,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 12,
    raw_key: "80a27c8e-8bad-5799-8f21-26b1a3ad0e6e",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":9,\"nps_response_uid\":\"c3c22ff9-4dfe-5a40-95dc-02f7ec857350\",\"nps_invitation_uid\":\"80a27c8e-8bad-5799-8f21-26b1a3ad0e6e\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.297000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.297000Z\",\"adjusted_score\":100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 2426213254,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 13,
    raw_key: "80a27c8e-8bad-5799-8f21-26b1a3ad0e6e",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":9,\"nps_response_uid\":\"c3c22ff9-4dfe-5a40-95dc-02f7ec857350\",\"nps_invitation_uid\":\"80a27c8e-8bad-5799-8f21-26b1a3ad0e6e\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.297000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.297000Z\",\"adjusted_score\":100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 2426213254,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 14,
    raw_key: "80a27c8e-8bad-5799-8f21-26b1a3ad0e6e",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":9,\"nps_response_uid\":\"c3c22ff9-4dfe-5a40-95dc-02f7ec857350\",\"nps_invitation_uid\":\"80a27c8e-8bad-5799-8f21-26b1a3ad0e6e\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.297000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.297000Z\",\"adjusted_score\":100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 2426213254,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 15,
    raw_key: "80a27c8e-8bad-5799-8f21-26b1a3ad0e6e",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":9,\"nps_response_uid\":\"c3c22ff9-4dfe-5a40-95dc-02f7ec857350\",\"nps_invitation_uid\":\"80a27c8e-8bad-5799-8f21-26b1a3ad0e6e\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.297000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.297000Z\",\"adjusted_score\":100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 2426213254,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 16,
    raw_key: "80a27c8e-8bad-5799-8f21-26b1a3ad0e6e",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":9,\"nps_response_uid\":\"c3c22ff9-4dfe-5a40-95dc-02f7ec857350\",\"nps_invitation_uid\":\"80a27c8e-8bad-5799-8f21-26b1a3ad0e6e\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.297000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.297000Z\",\"adjusted_score\":100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 2426213254,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 17,
    raw_key: "80a27c8e-8bad-5799-8f21-26b1a3ad0e6e",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":9,\"nps_response_uid\":\"c3c22ff9-4dfe-5a40-95dc-02f7ec857350\",\"nps_invitation_uid\":\"80a27c8e-8bad-5799-8f21-26b1a3ad0e6e\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.297000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.297000Z\",\"adjusted_score\":100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 2426213254,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 18,
    raw_key: "80a27c8e-8bad-5799-8f21-26b1a3ad0e6e",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":9,\"nps_response_uid\":\"c3c22ff9-4dfe-5a40-95dc-02f7ec857350\",\"nps_invitation_uid\":\"80a27c8e-8bad-5799-8f21-26b1a3ad0e6e\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.297000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.297000Z\",\"adjusted_score\":100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 2426213254,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 19,
    raw_key: "80a27c8e-8bad-5799-8f21-26b1a3ad0e6e",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":9,\"nps_response_uid\":\"c3c22ff9-4dfe-5a40-95dc-02f7ec857350\",\"nps_invitation_uid\":\"80a27c8e-8bad-5799-8f21-26b1a3ad0e6e\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.297000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.297000Z\",\"adjusted_score\":100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 2426213254,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 20,
    raw_key: "80a27c8e-8bad-5799-8f21-26b1a3ad0e6e",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":9,\"nps_response_uid\":\"c3c22ff9-4dfe-5a40-95dc-02f7ec857350\",\"nps_invitation_uid\":\"80a27c8e-8bad-5799-8f21-26b1a3ad0e6e\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.297000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.297000Z\",\"adjusted_score\":100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 2426213254,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 21,
    raw_key: "80a27c8e-8bad-5799-8f21-26b1a3ad0e6e",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":9,\"nps_response_uid\":\"c3c22ff9-4dfe-5a40-95dc-02f7ec857350\",\"nps_invitation_uid\":\"80a27c8e-8bad-5799-8f21-26b1a3ad0e6e\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.297000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.297000Z\",\"adjusted_score\":100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 2426213254,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 22,
    raw_key: "80a27c8e-8bad-5799-8f21-26b1a3ad0e6e",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":9,\"nps_response_uid\":\"c3c22ff9-4dfe-5a40-95dc-02f7ec857350\",\"nps_invitation_uid\":\"80a27c8e-8bad-5799-8f21-26b1a3ad0e6e\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.297000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.297000Z\",\"adjusted_score\":100}",
    value_schema: nil
  }
]

partition: %Rivulet.Kafka.Partition{partition: 8, topic: "platform_nps_location_joins"}
20:01:52.519 [warn] :brod_client [#PID<0.824.0>] :"snowden-sink-host1" got unexpected cast: {:ack, "platform_nps_location_joins", 7, 64}
messages: [
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 0,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 1,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 2,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 3,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 4,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 5,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 6,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 7,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 8,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 9,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 10,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 11,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 12,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 13,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 14,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 15,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 16,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 17,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 18,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 19,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 20,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 21,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 22,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 23,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 24,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 25,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 26,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 27,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 28,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 29,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 30,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 31,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 32,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 33,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 34,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 35,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 36,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 37,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 38,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 39,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 40,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    value_schema: nil
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 41,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    raw_value: "{\"response_received_at\":\"2018-04-06T15:12:32.461000Z\",\"organization_uid\":\"f4ac4bcb-e271-5a92-8e43-1d676a8821fa\",\"nps_score\":2,\"nps_response_uid\":\"a6ae2318-a69f-5b0f-a29e-4181445ca355\",\"nps_invitation_uid\":\"23cab2a9-0582-5aa6-b77e-4c24b4ee1951\",\"nps_comment\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"location_uid\":\"5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5\",\"location_name\":\"Paul Blanco's Good Car Company\",\"location_address\":\"3190 Auto Center Cir, Stockton, CA 95212, USA\",\"invitation_sent_at\":\"2018-04-06T15:12:32.304000Z\",\"customer_phone\":\"+18012556085\",\"customer_name\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":\"2018-04-06T15:12:32.304000Z\",\"adjusted_score\":-100}",
    ...
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 42,
    raw_key: "23cab2a9-0582-5aa6-b77e-4c24b4ee1951",
    ...
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    offset: 43,
    ...
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    key_schema: nil,
    ...
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    decoded_value: nil,
    ...
  },
  %Rivulet.Kafka.Consumer.Message{
    attributes: 0,
    crc: 812621839,
    decoded_key: nil,
    ...
  },
  %Rivulet.Kafka.Consumer.Message{attributes: 0, crc: 812621839, ...},
  %Rivulet.Kafka.Consumer.Message{attributes: 0, ...},
  %Rivulet.Kafka.Consumer.Message{...},
  ...
]
  """
  def handle_cast({:handle_messages, partition, messages}, %Config{} = state) do
    IO.inspect(partition, label: "partition")
    IO.inspect(messages, label: "messages")
    Logger.debug("Handling Messages by dumping")

    offset = messages |> List.last |> Map.get(:offset)

    Logger.debug("Should get to #{partition.topic}:#{partition.partition} - #{offset}")

    successfully_inserted =
      messages
      |> bulk_index_decoded_messages(state)
      |> Filterer.filter_for_successfully_inserted(messages)

    state.callback_module.on_complete(successfully_inserted)

    Rivulet.Consumer.ack(Rivulet.client_name!, partition, offset)

    {:noreply, state}
  end

  def only_latest_per_key(messages) when is_list(messages) do
    messages
    |> Enum.group_by(&(&1.raw_key))
    |> Enum.map(fn({_key, messages}) -> List.last(messages) end)
    |> List.flatten
  end

  def bulk_index_decoded_messages(messages, state) when is_list(messages) do
    messages
    |> only_latest_per_key
    |> bulk_index(state)
    |> handle_es_response()
  end

  def bulk_index(records, %Config{} = state) do
    records = format_bulk_records(state.elastic_index, state.elastic_type, records)
    raw_data = encode_bulk_records(records)

    Bulk.post_raw(state.elastic_url, raw_data, index: state.elastic_index, type: state.elastic_type)
  end

  def handle_es_response({:ok, %HTTPoison.Response{body: body}}), do: body
  def handle_es_response({:error, %HTTPoison.Response{}}) do
    %{}
  end

  defp encode_bulk_records(lines) do
    Enum.map(lines, &encode_single_record/1)
  end

  defp encode_single_record(line) do
    with {:ok, encoded} <- JSON.encode(line),
     do: encoded <> "\n"
  end

  @spec format_bulk_records(index_name, type_name, [record]) :: [index_record]
  defp format_bulk_records(index, type, records) do
    records
    |> List.wrap()
    |> Enum.flat_map(&update_line(index, type, &1))
  end

  @spec update_line(index_name, type_name, any) :: upserted_record
  defp update_line(index, type, %Message{} = message) do
    document_id = message.raw_key
    {:ok, record} = JSON.decode(message.raw_value)

    formatted_record = format_for_upsert(record)

    [
      %{"update" => %{
          "_index" => index,
          "_type" => type,
          "_id" => document_id
          }
      },
      formatted_record
    ]
  end

  defp format_for_upsert(record) do
    %{"doc" => record, "doc_as_upsert" => true}
  end
end
