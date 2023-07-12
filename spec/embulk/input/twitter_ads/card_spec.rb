require 'embulk/input/twitter_ads/card'
require 'webmock/rspec'
require "oauth"

RSpec.describe Embulk::Input::TwitterAds::Card do
  let(:access_token) do
    consumer = OAuth::Consumer.new('consumer_key', 'consumer_secret', site: "https://ads-api.twitter.com", scheme: :header)
    OAuth::AccessToken.from_hash(consumer, oauth_token: 'oauth_token', oauth_token_secret: 'oauth_token_secret')
  end
  let(:logger) { double(Object) }

  describe 'fetch_pages' do
    subject do
      Embulk::Input::TwitterAds::Card.fetch_pages(api_version: '12',
                                                  access_token: access_token,
                                                  account_id: 'account_id',
                                                  logger: logger,
                                                  entity_start_date: nil,
                                                  entity_end_date: nil,
                                                  entity_timezone: nil,
                                                  columns: [{ 'name' => 'id' }])
    end

    before do
      stub_request(:get, "https://ads-api.twitter.com/12/accounts/account_id/cards?include_legacy_cards=true").
        to_return(status: 200, body: {"next_cursor"=>nil, "data"=> [{"id"=>"0", "created_at"=>"2020-11-03T22:43:04Z"}]}.to_json)
    end

    it { is_expected.to eq([["0"]]) }
  end

  describe 'request' do
    subject do
      Embulk::Input::TwitterAds::Card.request(api_version: '12', access_token: access_token, account_id: 'account_id', logger: logger)
    end

    context 'success' do
      before do
        page0_body = {
          "next_cursor"=>"1",
          "data"=>
          [
            {
              "name"=>"0",
              "components"=> [
                {"media_key"=>"0","media_metadata"=>{"0"=>{"type"=>"IMAGE","url"=>"https=>//example.com","width"=>0,"height"=>0}},"type"=>"MEDIA"},
                {"title"=>"0","destination"=>{"url"=>"0","type"=>"WEBSITE"},"type"=>"DETAILS"}
              ],
              "id"=>"0",
              "created_at"=>"2020-11-03T22:43:04Z",
              "card_uri"=>"card://0",
              "updated_at"=>"2021-08-26T19:26:15Z",
              "deleted"=>false,
              "card_type"=>"IMAGE_WEBSITE"
            }
          ]
        }
        stub_request(:get, "https://ads-api.twitter.com/12/accounts/account_id/cards?include_legacy_cards=true").
          to_return(status: 200, body: page0_body.to_json)

        page1_body = {
          "next_cursor"=>nil,
          "data"=>
          [
            {
              "name"=>"1",
              "components"=> [
                {"media_key"=>"1","media_metadata"=>{"1"=>{"type"=>"IMAGE","url"=>"https=>//example.com","width"=>1,"height"=>1}},"type"=>"MEDIA"},
                {"title"=>"1","destination"=>{"url"=>"1","type"=>"WEBSITE"},"type"=>"DETAILS"}
              ],
              "id"=>"1",
              "created_at"=>"2020-11-03T22:43:04Z",
              "card_uri"=>"card://01",
              "updated_at"=>"2021-08-26T19:26:15Z",
              "deleted"=>false,
              "card_type"=>"IMAGE_WEBSITE"
            }
          ]
        }
        stub_request(:get, "https://ads-api.twitter.com/12/accounts/account_id/cards?include_legacy_cards=true&cursor=1").
          to_return(status: 200, body: page1_body.to_json)
      end

      it do
        expecteds = [
          {
            "name"=>"0",
            "components"=>[
              {"media_key"=>"0","media_metadata"=>{"0"=>{"type"=>"IMAGE","url"=>"https=>//example.com","width"=>0,"height"=>0}},"type"=>"MEDIA"},
              {"title"=>"0","destination"=>{"url"=>"0","type"=>"WEBSITE"},"type"=>"DETAILS"}
            ],
            "id"=>"0",
            "created_at"=>"2020-11-03T22:43:04Z",
            "card_uri"=>"card://0",
            "updated_at"=>"2021-08-26T19:26:15Z",
            "deleted"=>false,
            "card_type"=>"IMAGE_WEBSITE"
          },
          {
            "name"=>"1",
            "components"=> [
              {"media_key"=>"1","media_metadata"=>{"1"=>{"type"=>"IMAGE","url"=>"https=>//example.com","width"=>1,"height"=>1}},"type"=>"MEDIA"},
              {"title"=>"1","destination"=>{"url"=>"1","type"=>"WEBSITE"},"type"=>"DETAILS"}
            ],
            "id"=>"1",
            "created_at"=>"2020-11-03T22:43:04Z",
            "card_uri"=>"card://01",
            "updated_at"=>"2021-08-26T19:26:15Z",
            "deleted"=>false,
            "card_type"=>"IMAGE_WEBSITE"
          }
        ]
        is_expected.to eq(expecteds)
      end
    end

    context 'client error except rate limit' do
      before do
        allow(logger).to receive(:error).once
        stub_request(:get, "https://ads-api.twitter.com/12/accounts/account_id/cards?include_legacy_cards=true").
          to_return(status: 400, body: '')
      end

      it { expect { subject }.to raise_error(Embulk::Input::TwitterAds::API::BadRequest) }
    end

    context 'server error' do
      before do
        allow(logger).to receive(:info).exactly(5).times
        allow(logger).to receive(:warn).exactly(5).times
        allow(logger).to receive(:error).exactly(7).times
        allow(Embulk::Input::TwitterAds::Card).to receive(:sleep).exactly(5).times
        stub_request(:get, "https://ads-api.twitter.com/12/accounts/account_id/cards?include_legacy_cards=true").
          to_return(status: 500, body: '')
      end

      it { expect { subject }.to raise_error(Embulk::Input::TwitterAds::API::ServerError) }
    end
  end

  describe 'convert_to_pages' do
    subject do
      Embulk::Input::TwitterAds::Card.convert_to_pages(data: data,
                                                       entity_start_date: entity_start_date,
                                                       entity_end_date: entity_end_date,
                                                       entity_timezone: entity_timezone,
                                                       columns: columns)
    end

    let(:entity_start_date) { nil }
    let(:entity_end_date) { nil }
    let(:entity_timezone) { nil }
    let(:columns) { [{ 'name' => 'id' }, { 'name' => 'created_at' }] }
    let(:data) do
      [
        { 'id' => 1, 'created_at' => "2020-01-01T00:00:00Z" },
        { 'id' => 2, 'created_at' => "2020-01-02T00:00:00Z" },
        { 'id' => 3, 'created_at' => "2020-01-03T00:00:00Z" },
        { 'id' => 4, 'created_at' => "2020-01-04T00:00:00Z" }]
    end

    context 'order by columns' do
      let(:columns) { [{ 'name' => 'created_at' }, { 'name' => 'id' }] }

      it { is_expected.to eq([["2020-01-01T00:00:00Z", 1], ["2020-01-02T00:00:00Z", 2], ["2020-01-03T00:00:00Z", 3], ["2020-01-04T00:00:00Z", 4]]) }
    end

    context 'filter by entity' do
      let(:entity_start_date) { '2020-01-02' }
      let(:entity_end_date) { '2020-01-03' }

      context 'utc' do
        let(:entity_timezone) { 'UTC' }

        it { is_expected.to eq([[2, "2020-01-02T00:00:00Z"], [3, "2020-01-03T00:00:00Z"]]) }
      end

      context 'utc' do
        let(:entity_timezone) { 'Asia/Tokyo' }

        it { is_expected.to eq([[2, "2020-01-02T00:00:00Z"]]) }
      end
    end
  end
end
