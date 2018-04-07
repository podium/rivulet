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

  @doc """
  res looks like:
  [[
    %{
      "address" => "3190 Auto Center Cir, Stockton, CA 95212, USA",
      "archived" => false,
      "created_at" => "2018-02-20T21:35:11.274635Z",
      "last_modified_at" => "2018-04-07T20:35:11.275092Z",
      "name" => "VonRueden-Jaskolski",
      "organization_uid" => "fecd66c7-b92a-5e9e-9438-d4d41797f5d5",
      "podium_number" => "+13853360060",
      "timezone_identifier_uid" => nil,
      "uid" => "3171fee2-795b-5e79-a965-5128761d1319",
      "updated_at" => "2018-04-07T20:35:11.292541Z"
    }
  ],
  [
    %{
      "adjusted_score" => 0,
      "created_at" => #DateTime<2018-04-07 20:32:49.651000Z>,
      "customer_name" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "customer_phone" => "+18012556085",
      "invitation_sent_at" => #DateTime<2018-04-07 20:32:49.539000Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "nps_comment" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "nps_invitation_uid" => <<103, 174, 254, 255, 65, 224, 94, 142, 146, 142,
        224, 182, 193, 184, 234, 152>>,
      "nps_response_uid" => <<159, 75, 73, 42, 229, 210, 93, 116, 176, 193, 170,
        96, 203, 92, 154, 211>>,
      "nps_score" => 7,
      "response_received_at" => #DateTime<2018-04-07 20:32:50.093000Z>
    },
    %{
      "adjusted_score" => -100,
      "created_at" => #DateTime<2018-04-07 20:32:50.103000Z>,
      "customer_name" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "customer_phone" => "+18012556085",
      "invitation_sent_at" => #DateTime<2018-04-07 20:32:50.103000Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "nps_comment" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "nps_invitation_uid" => <<18, 185, 184, 191, 73, 174, 93, 138, 129, 9,
        169, 20, 145, 231, 73, 42>>,
      "nps_response_uid" => <<99, 221, 178, 123, 104, 92, 85, 5, 173, 219, 11,
        110, 255, 74, 173, 150>>,
      "nps_score" => 4,
      "response_received_at" => #DateTime<2018-04-07 20:32:50.110000Z>
    },
    %{
      "adjusted_score" => 0,
      "created_at" => #DateTime<2018-04-07 20:32:50.103000Z>,
      "customer_name" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "customer_phone" => "+18012556085",
      "invitation_sent_at" => #DateTime<2018-04-07 20:32:50.103000Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "nps_comment" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "nps_invitation_uid" => <<40, 86, 170, 21, 199, 65, 83, 26, 172, 61, 133,
        98, 172, 98, 241, 187>>,
      "nps_response_uid" => <<93, 56, 254, 55, 188, 180, 85, 63, 154, 100, 104,
        57, 99, 247, 219, 98>>,
      "nps_score" => 7,
      "response_received_at" => #DateTime<2018-04-07 20:32:50.109000Z>
    },
    %{
      "adjusted_score" => 100,
      "created_at" => #DateTime<2018-04-07 20:32:50.103000Z>,
      "customer_name" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "customer_phone" => "+18012556085",
      "invitation_sent_at" => #DateTime<2018-04-07 20:32:50.103000Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "nps_comment" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "nps_invitation_uid" => <<228, 36, 172, 107, 160, 17, 86, 209, 174, 41,
        146, 44, 25, 136, 80, 36>>,
      "nps_response_uid" => <<232, 233, 100, 129, 0, 251, 82, 62, 146, 131, 166,
        161, 63, 22, 100, 57>>,
      "nps_score" => 10,
      "response_received_at" => #DateTime<2018-04-07 20:32:50.109000Z>
    },
    %{
      "adjusted_score" => -100,
      "created_at" => #DateTime<2018-04-07 20:32:49.651000Z>,
      "customer_name" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "customer_phone" => "+18012556085",
      "invitation_sent_at" => #DateTime<2018-04-07 20:32:49.539000Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "nps_comment" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "nps_invitation_uid" => <<53, 165, 166, 147, 169, 102, 91, 112, 143, 66,
        28, 12, 89, 205, 90, 238>>,
      "nps_response_uid" => <<23, 10, 106, 82, 82, 208, 93, 186, 151, 120, 72,
        58, 53, 49, 251, 142>>,
      "nps_score" => 5,
      "response_received_at" => #DateTime<2018-04-07 20:32:50.093000Z>
    },
    %{
      "adjusted_score" => -100,
      "created_at" => #DateTime<2018-04-07 20:32:50.117000Z>,
      "customer_name" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "customer_phone" => "+18012556085",
      "invitation_sent_at" => #DateTime<2018-04-07 20:32:50.117000Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "nps_comment" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "nps_invitation_uid" => <<61, 82, 21, 159, 234, 28, 82, 107, 136, 189,
        217, 140, 207, 174, 128, 201>>,
      "nps_response_uid" => <<165, 18, 50, 236, 121, 22, 83, 170, 137, 76, 237,
        181, 236, 224, 149, 183>>,
      "nps_score" => 3,
      "response_received_at" => #DateTime<2018-04-07 20:32:50.123000Z>
    },
    %{
      "adjusted_score" => 100,
      "created_at" => #DateTime<2018-04-07 20:32:50.102000Z>,
      "customer_name" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "customer_phone" => "+18012556085",
      "invitation_sent_at" => #DateTime<2018-04-07 20:32:50.102000Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "nps_comment" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "nps_invitation_uid" => <<70, 240, 168, 6, 139, 139, 83, 24, 172, 18, 150,
        206, 210, 12, 26, 251>>,
      "nps_response_uid" => <<158, 240, 198, 47, 100, 35, 84, 182, 135, 63, 172,
        220, 232, 95, 22, 216>>,
      "nps_score" => 9,
      "response_received_at" => #DateTime<2018-04-07 20:32:50.107000Z>
    },
    %{
      "adjusted_score" => -100,
      "created_at" => #DateTime<2018-04-07 20:32:50.117000Z>,
      "customer_name" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "customer_phone" => "+18012556085",
      "invitation_sent_at" => #DateTime<2018-04-07 20:32:50.117000Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "nps_comment" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "nps_invitation_uid" => <<79, 21, 85, 197, 9, 171, 92, 173, 171, 240, 54,
        124, 100, 57, 132, 57>>,
      "nps_response_uid" => <<5, 20, 74, 54, 23, 64, 82, 99, 189, 205, 235, 236,
        68, 251, 142, 237>>,
      "nps_score" => 3,
      "response_received_at" => #DateTime<2018-04-07 20:32:50.121000Z>
    },
    %{
      "address" => "3190 Auto Center Cir, Stockton, CA 95212, USA",
      "archived" => false,
      "created_at" => "2018-02-20T21:35:09.896194Z",
      "last_modified_at" => "2018-04-07T20:35:09.896802Z",
      "name" => "Paul Blanco's Good Car Company",
      "organization_uid" => "f4ac4bcb-e271-5a92-8e43-1d676a8821fa",
      "podium_number" => "+13853360060",
      "timezone_identifier_uid" => nil,
      "uid" => "5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5",
      "updated_at" => "2018-04-07T20:35:09.924731Z"
    },
    %{
      "adjusted_score" => 100,
      "created_at" => #DateTime<2018-04-07 20:32:49.651000Z>,
      "customer_name" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "customer_phone" => "+18012556085",
      "invitation_sent_at" => #DateTime<2018-04-07 20:32:49.651000Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "nps_comment" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "nps_invitation_uid" => <<175, 185, 179, 233, 108, 40, 92, 176, 155, 108,
        106, 110, 195, 224, 105, 99>>,
      "nps_response_uid" => <<240, 49, 92, 84, 178, 114, 94, 147, 134, 249, 152,
        151, 194, 216, 96, 197>>,
      "nps_score" => 8,
      "response_received_at" => #DateTime<2018-04-07 20:32:50.093000Z>
    }
  ],[
    %{
      "adjusted_score" => 0,
      "created_at" => #DateTime<2018-04-07 20:32:49.651000Z>,
      "customer_name" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "customer_phone" => "+18012556085",
      "invitation_sent_at" => #DateTime<2018-04-07 20:32:49.539000Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "nps_comment" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "nps_invitation_uid" => <<103, 174, 254, 255, 65, 224, 94, 142, 146, 142,
        224, 182, 193, 184, 234, 152>>,
      "nps_response_uid" => <<159, 75, 73, 42, 229, 210, 93, 116, 176, 193, 170,
        96, 203, 92, 154, 211>>,
      "nps_score" => 7,
      "response_received_at" => #DateTime<2018-04-07 20:32:50.093000Z>
    },
    %{
      "adjusted_score" => -100,
      "created_at" => #DateTime<2018-04-07 20:32:50.103000Z>,
      "customer_name" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "customer_phone" => "+18012556085",
      "invitation_sent_at" => #DateTime<2018-04-07 20:32:50.103000Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "nps_comment" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "nps_invitation_uid" => <<18, 185, 184, 191, 73, 174, 93, 138, 129, 9,
        169, 20, 145, 231, 73, 42>>,
      "nps_response_uid" => <<99, 221, 178, 123, 104, 92, 85, 5, 173, 219, 11,
        110, 255, 74, 173, 150>>,
      "nps_score" => 4,
      "response_received_at" => #DateTime<2018-04-07 20:32:50.110000Z>
    },
    %{
      "adjusted_score" => 0,
      "created_at" => #DateTime<2018-04-07 20:32:50.103000Z>,
      "customer_name" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "customer_phone" => "+18012556085",
      "invitation_sent_at" => #DateTime<2018-04-07 20:32:50.103000Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "nps_comment" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "nps_invitation_uid" => <<40, 86, 170, 21, 199, 65, 83, 26, 172, 61, 133,
        98, 172, 98, 241, 187>>,
      "nps_response_uid" => <<93, 56, 254, 55, 188, 180, 85, 63, 154, 100, 104,
        57, 99, 247, 219, 98>>,
      "nps_score" => 7,
      "response_received_at" => #DateTime<2018-04-07 20:32:50.109000Z>
    },
    %{
      "adjusted_score" => 100,
      "created_at" => #DateTime<2018-04-07 20:32:50.103000Z>,
      "customer_name" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "customer_phone" => "+18012556085",
      "invitation_sent_at" => #DateTime<2018-04-07 20:32:50.103000Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "nps_comment" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "nps_invitation_uid" => <<228, 36, 172, 107, 160, 17, 86, 209, 174, 41,
        146, 44, 25, 136, 80, 36>>,
      "nps_response_uid" => <<232, 233, 100, 129, 0, 251, 82, 62, 146, 131, 166,
        161, 63, 22, 100, 57>>,
      "nps_score" => 10,
      "response_received_at" => #DateTime<2018-04-07 20:32:50.109000Z>
    },
    %{
      "adjusted_score" => -100,
      "created_at" => #DateTime<2018-04-07 20:32:49.651000Z>,
      "customer_name" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "customer_phone" => "+18012556085",
      "invitation_sent_at" => #DateTime<2018-04-07 20:32:49.539000Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "nps_comment" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "nps_invitation_uid" => <<53, 165, 166, 147, 169, 102, 91, 112, 143, 66,
        28, 12, 89, 205, 90, 238>>,
      "nps_response_uid" => <<23, 10, 106, 82, 82, 208, 93, 186, 151, 120, 72,
        58, 53, 49, 251, 142>>,
      "nps_score" => 5,
      "response_received_at" => #DateTime<2018-04-07 20:32:50.093000Z>
    },
    %{
      "adjusted_score" => -100,
      "created_at" => #DateTime<2018-04-07 20:32:50.117000Z>,
      "customer_name" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "customer_phone" => "+18012556085",
      "invitation_sent_at" => #DateTime<2018-04-07 20:32:50.117000Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "nps_comment" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "nps_invitation_uid" => <<61, 82, 21, 159, 234, 28, 82, 107, 136, 189,
        217, 140, 207, 174, 128, 201>>,
      "nps_response_uid" => <<165, 18, 50, 236, 121, 22, 83, 170, 137, 76, 237,
        181, 236, 224, 149, 183>>,
      "nps_score" => 3,
      "response_received_at" => #DateTime<2018-04-07 20:32:50.123000Z>
    },
    %{
      "adjusted_score" => 100,
      "created_at" => #DateTime<2018-04-07 20:32:50.102000Z>,
      "customer_name" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "customer_phone" => "+18012556085",
      "invitation_sent_at" => #DateTime<2018-04-07 20:32:50.102000Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "nps_comment" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "nps_invitation_uid" => <<70, 240, 168, 6, 139, 139, 83, 24, 172, 18, 150,
        206, 210, 12, 26, 251>>,
      "nps_response_uid" => <<158, 240, 198, 47, 100, 35, 84, 182, 135, 63, 172,
        220, 232, 95, 22, 216>>,
      "nps_score" => 9,
      "response_received_at" => #DateTime<2018-04-07 20:32:50.107000Z>
    },
    %{
      "adjusted_score" => -100,
      "created_at" => #DateTime<2018-04-07 20:32:50.117000Z>,
      "customer_name" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "customer_phone" => "+18012556085",
      "invitation_sent_at" => #DateTime<2018-04-07 20:32:50.117000Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "nps_comment" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "nps_invitation_uid" => <<79, 21, 85, 197, 9, 171, 92, 173, 171, 240, 54,
        124, 100, 57, 132, 57>>,
      "nps_response_uid" => <<5, 20, 74, 54, 23, 64, 82, 99, 189, 205, 235, 236,
        68, 251, 142, 237>>,
      "nps_score" => 3,
      "response_received_at" => #DateTime<2018-04-07 20:32:50.121000Z>
    },
    %{
      "address" => "3190 Auto Center Cir, Stockton, CA 95212, USA",
      "archived" => false,
      "created_at" => "2018-02-20T21:35:09.896194Z",
      "last_modified_at" => "2018-04-07T20:35:09.896802Z",
      "name" => "Paul Blanco's Good Car Company",
      "organization_uid" => "f4ac4bcb-e271-5a92-8e43-1d676a8821fa",
      "podium_number" => "+13853360060",
      "timezone_identifier_uid" => nil,
      "uid" => "5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5",
      "updated_at" => "2018-04-07T20:35:09.924731Z"
    },
    %{
      "adjusted_score" => 100,
      "created_at" => #DateTime<2018-04-07 20:32:49.651000Z>,
      "customer_name" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "customer_phone" => "+18012556085",
      "invitation_sent_at" => #DateTime<2018-04-07 20:32:49.651000Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "nps_comment" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "nps_invitation_uid" => <<175, 185, 179, 233, 108, 40, 92, 176, 155, 108,
        106, 110, 195, 224, 105, 99>>,
      "nps_response_uid" => <<240, 49, 92, 84, 178, 114, 94, 147, 134, 249, 152,
        151, 194, 216, 96, 197>>,
      "nps_score" => 8,
      "response_received_at" => #DateTime<2018-04-07 20:32:50.093000Z>
    }
  ],
  [
    %{
      "adjusted_score" => 0,
      "created_at" => #DateTime<2018-04-07 20:32:49.651000Z>,
      "customer_name" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "customer_phone" => "+18012556085",
      "invitation_sent_at" => #DateTime<2018-04-07 20:32:49.539000Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "nps_comment" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "nps_invitation_uid" => <<103, 174, 254, 255, 65, 224, 94, 142, 146, 142,
        224, 182, 193, 184, 234, 152>>,
      "nps_response_uid" => <<159, 75, 73, 42, 229, 210, 93, 116, 176, 193, 170,
        96, 203, 92, 154, 211>>,
      "nps_score" => 7,
      "response_received_at" => #DateTime<2018-04-07 20:32:50.093000Z>
    },
    %{
      "adjusted_score" => -100,
      "created_at" => #DateTime<2018-04-07 20:32:50.103000Z>,
      "customer_name" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "customer_phone" => "+18012556085",
      "invitation_sent_at" => #DateTime<2018-04-07 20:32:50.103000Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "nps_comment" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "nps_invitation_uid" => <<18, 185, 184, 191, 73, 174, 93, 138, 129, 9,
        169, 20, 145, 231, 73, 42>>,
      "nps_response_uid" => <<99, 221, 178, 123, 104, 92, 85, 5, 173, 219, 11,
        110, 255, 74, 173, 150>>,
      "nps_score" => 4,
      "response_received_at" => #DateTime<2018-04-07 20:32:50.110000Z>
    },
    %{
      "adjusted_score" => 0,
      "created_at" => #DateTime<2018-04-07 20:32:50.103000Z>,
      "customer_name" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "customer_phone" => "+18012556085",
      "invitation_sent_at" => #DateTime<2018-04-07 20:32:50.103000Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "nps_comment" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "nps_invitation_uid" => <<40, 86, 170, 21, 199, 65, 83, 26, 172, 61, 133,
        98, 172, 98, 241, 187>>,
      "nps_response_uid" => <<93, 56, 254, 55, 188, 180, 85, 63, 154, 100, 104,
        57, 99, 247, 219, 98>>,
      "nps_score" => 7,
      "response_received_at" => #DateTime<2018-04-07 20:32:50.109000Z>
    },
    %{
      "adjusted_score" => 100,
      "created_at" => #DateTime<2018-04-07 20:32:50.103000Z>,
      "customer_name" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "customer_phone" => "+18012556085",
      "invitation_sent_at" => #DateTime<2018-04-07 20:32:50.103000Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "nps_comment" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "nps_invitation_uid" => <<228, 36, 172, 107, 160, 17, 86, 209, 174, 41,
        146, 44, 25, 136, 80, 36>>,
      "nps_response_uid" => <<232, 233, 100, 129, 0, 251, 82, 62, 146, 131, 166,
        161, 63, 22, 100, 57>>,
      "nps_score" => 10,
      "response_received_at" => #DateTime<2018-04-07 20:32:50.109000Z>
    },
    %{
      "adjusted_score" => -100,
      "created_at" => #DateTime<2018-04-07 20:32:49.651000Z>,
      "customer_name" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "customer_phone" => "+18012556085",
      "invitation_sent_at" => #DateTime<2018-04-07 20:32:49.539000Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "nps_comment" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "nps_invitation_uid" => <<53, 165, 166, 147, 169, 102, 91, 112, 143, 66,
        28, 12, 89, 205, 90, 238>>,
      "nps_response_uid" => <<23, 10, 106, 82, 82, 208, 93, 186, 151, 120, 72,
        58, 53, 49, 251, 142>>,
      "nps_score" => 5,
      "response_received_at" => #DateTime<2018-04-07 20:32:50.093000Z>
    },
    %{
      "adjusted_score" => -100,
      "created_at" => #DateTime<2018-04-07 20:32:50.117000Z>,
      "customer_name" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "customer_phone" => "+18012556085",
      "invitation_sent_at" => #DateTime<2018-04-07 20:32:50.117000Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "nps_comment" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "nps_invitation_uid" => <<61, 82, 21, 159, 234, 28, 82, 107, 136, 189,
        217, 140, 207, 174, 128, 201>>,
      "nps_response_uid" => <<165, 18, 50, 236, 121, 22, 83, 170, 137, 76, 237,
        181, 236, 224, 149, 183>>,
      "nps_score" => 3,
      "response_received_at" => #DateTime<2018-04-07 20:32:50.123000Z>
    },
    %{
      "adjusted_score" => 100,
      "created_at" => #DateTime<2018-04-07 20:32:50.102000Z>,
      "customer_name" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "customer_phone" => "+18012556085",
      "invitation_sent_at" => #DateTime<2018-04-07 20:32:50.102000Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "nps_comment" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "nps_invitation_uid" => <<70, 240, 168, 6, 139, 139, 83, 24, 172, 18, 150,
        206, 210, 12, 26, 251>>,
      "nps_response_uid" => <<158, 240, 198, 47, 100, 35, 84, 182, 135, 63, 172,
        220, 232, 95, 22, 216>>,
      "nps_score" => 9,
      "response_received_at" => #DateTime<2018-04-07 20:32:50.107000Z>
    },
    %{
      "adjusted_score" => -100,
      "created_at" => #DateTime<2018-04-07 20:32:50.117000Z>,
      "customer_name" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "customer_phone" => "+18012556085",
      "invitation_sent_at" => #DateTime<2018-04-07 20:32:50.117000Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "nps_comment" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "nps_invitation_uid" => <<79, 21, 85, 197, 9, 171, 92, 173, 171, 240, 54,
        124, 100, 57, 132, 57>>,
      "nps_response_uid" => <<5, 20, 74, 54, 23, 64, 82, 99, 189, 205, 235, 236,
        68, 251, 142, 237>>,
      "nps_score" => 3,
      "response_received_at" => #DateTime<2018-04-07 20:32:50.121000Z>
    },
    %{
      "address" => "3190 Auto Center Cir, Stockton, CA 95212, USA",
      "archived" => false,
      "created_at" => "2018-02-20T21:35:09.896194Z",
      "last_modified_at" => "2018-04-07T20:35:09.896802Z",
      "name" => "Paul Blanco's Good Car Company",
      "organization_uid" => "f4ac4bcb-e271-5a92-8e43-1d676a8821fa",
      "podium_number" => "+13853360060",
      "timezone_identifier_uid" => nil,
      "uid" => "5fd03bf8-9cd6-520a-b2e3-9084b78cb0c5",
      "updated_at" => "2018-04-07T20:35:09.924731Z"
    },
    %{
      "adjusted_score" => 100,
      "created_at" => #DateTime<2018-04-07 20:32:49.651000Z>,
      "customer_name" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "customer_phone" => "+18012556085",
      "invitation_sent_at" => #DateTime<2018-04-07 20:32:49.651000Z>,
      "location_uid" => <<95, 208, 59, 248, 156, 214, 82, 10, 178, 227, 144,
        132, 183, 140, 176, 197>>,
      "nps_comment" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "nps_invitation_uid" => <<175, 185, 179, 233, 108, 40, 92, 176, 155, 108,
        106, 110, 195, 224, 105, 99>>,
      "nps_response_uid" => <<240, 49, 92, 84, 178, 114, 94, 147, 134, 249, 152,
        151, 194, 216, 96, 197>>,
      "nps_score" => 8,
      "response_received_at" => #DateTime<2018-04-07 20:32:50.093000Z>
    }
  ]]
  """
  def handle_call({:handle_resp, join_keys, ack_data}, from,  %State{join_id: join_id, transformers: transformers, consumer: consumer} = state) do
    GenServer.reply(from, :ok)

    IO.inspect(join_keys, label: "join_keys")

    res =
      join_id
      |> IO.inspect(label: "join_id")
      |> ElasticSearch.bulk_get_join_docs(join_keys)
      |> Map.get("responses")
      |> Enum.map(fn(%{"hits" => %{"hits" => hits}}) -> hits end)
      |> Enum.map(fn(hits) -> Enum.map(hits, fn(hit) -> hit["_source"]["document"] end) end)
      |> Enum.map(fn (docs) ->
        IO.inspect(docs, label: "docs")
        Enum.map(docs, fn (doc) ->
          doc
          |> Base.decode64!
          |> :erlang.binary_to_term
          |> IO.inspect(label: "binary_to_term")
        end)
      end)

    Rivulet.Kafka.Join.Funcs.transforms(res, transformers)

    ack_data
    |> Enum.reduce(%{}, fn
      ({topic, partition, offset}, %{} = acks) ->
        Map.update(acks, {topic, partition}, offset, fn(prev_offset) ->
          if prev_offset > offset,
          do: prev_offset,
          else: offset
        end)
    end)
    |> Enum.each(fn({{topic, partition}, offset}) ->
      partition = %Partition{topic: topic, partition: partition}
      Rivulet.Consumer.ack(consumer, partition, offset)
    end)

    {:noreply, state}
  end

  def handle_info(_, state) do
    {:noreply, state}
  end
end
