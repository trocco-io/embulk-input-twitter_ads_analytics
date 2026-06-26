require 'webmock/rspec'
require 'oauth'

# Stub minimal Embulk framework so the plugin can be loaded without JRuby/Embulk runtime
module Embulk
  class InputPlugin
    def self.transaction(*); end
    def initialize(task, schema, index, page_builder)
      @task = task
      init
    end
    def init; end
  end

  module Plugin
    def self.register_input(*); end
  end

  def self.logger
    @logger ||= Logger.new(nil)
  end
end

require 'embulk/input/twitter_ads_analytics'

RSpec.describe Embulk::Input::TwitterAdsAnalytics do
  let(:account_id) { 'test_account' }
  let(:job_id) { 'test_job_123' }

  let(:plugin) do
    task = double('task', :[] => nil)
    allow(task).to receive(:[]).with('account_id').and_return(account_id)
    allow(task).to receive(:[]).with('timezone').and_return('UTC')
    instance = described_class.allocate
    instance.instance_variable_set(:@account_id, account_id)
    instance
  end

  let(:access_token) do
    consumer = OAuth::Consumer.new('key', 'secret', site: 'https://ads-api.twitter.com', scheme: :header)
    OAuth::AccessToken.from_hash(consumer, oauth_token: 'token', oauth_token_secret: 'token_secret')
  end

  let(:job_status_url) do
    encoded = URI.encode_www_form_component(job_id)
    "https://ads-api.twitter.com/12/stats/jobs/accounts/#{account_id}?job_ids=#{encoded}"
  end

  describe '#poll_job_status' do
    before do
      allow(Embulk.logger).to receive(:info)
      allow(Embulk.logger).to receive(:warn)
      allow(Embulk.logger).to receive(:error)
      allow(plugin).to receive(:sleep)
    end

    context 'when response data is missing the data key' do
      before do
        stub_request(:get, job_status_url)
          .to_return(status: 200, body: { 'request' => {} }.to_json)
      end

      it 'raises an error immediately' do
        expect { plugin.poll_job_status(access_token, job_id) }
          .to raise_error(StandardError, /Invalid response data/)
      end
    end

    context 'when response data array is empty initially then returns SUCCESS' do
      before do
        success_body = {
          'data' => [{ 'id' => job_id, 'status' => 'SUCCESS', 'url' => 'https://example.com/result' }]
        }.to_json

        stub_request(:get, job_status_url)
          .to_return(
            { status: 200, body: { 'data' => [] }.to_json },
            { status: 200, body: success_body }
          )
      end

      it 'continues polling and eventually returns job data' do
        result = plugin.poll_job_status(access_token, job_id)
        expect(result['status']).to eq('SUCCESS')
      end

      it 'logs a waiting message on empty response' do
        plugin.poll_job_status(access_token, job_id)
        expect(Embulk.logger).to have_received(:info).with(/not yet available/)
      end
    end

    context 'when response data array remains empty until max_polling_attempts' do
      before do
        stub_request(:get, job_status_url)
          .to_return(status: 200, body: { 'data' => [] }.to_json)
      end

      it 'raises a timeout error referencing the empty-data cause' do
        expect { plugin.poll_job_status(access_token, job_id) }
          .to raise_error(StandardError, /timed out: data remained empty after/)
      end
    end

    context 'when job status is SUCCESS' do
      let(:job_data) { { 'id' => job_id, 'status' => 'SUCCESS', 'url' => 'https://example.com/result' } }

      before do
        stub_request(:get, job_status_url)
          .to_return(status: 200, body: { 'data' => [job_data] }.to_json)
      end

      it 'returns job data' do
        result = plugin.poll_job_status(access_token, job_id)
        expect(result['status']).to eq('SUCCESS')
      end
    end

    context 'when job status is FAILED' do
      before do
        stub_request(:get, job_status_url)
          .to_return(status: 200, body: { 'data' => [{ 'id' => job_id, 'status' => 'FAILED' }] }.to_json)
      end

      it 'raises an error' do
        expect { plugin.poll_job_status(access_token, job_id) }
          .to raise_error(StandardError, /failed/)
      end
    end
  end
end
