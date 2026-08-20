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

  describe '.guess' do
    def guessed_column_names(entity:, metric_groups:)
      config = double('config')
      allow(config).to receive(:param).with('entity', :string).and_return(entity)
      allow(config).to receive(:param).with('metric_groups', :array).and_return(metric_groups)
      described_class.guess(config)['columns'].map { |column| column[:name] }
    end

    context 'when ENGAGEMENT is requested for an entity that reports clicks' do
      let(:column_names) { guessed_column_names(entity: 'CAMPAIGN', metric_groups: ['ENGAGEMENT']) }

      it 'offers link_clicks, which replaced url_clicks in the API response' do
        expect(column_names).to include('link_clicks')
      end

      it 'keeps offering url_clicks, which is still documented' do
        expect(column_names).to include('url_clicks')
      end

      it 'appends link_clicks last, so no pre-existing column changes position' do
        expect(column_names.last).to eq('link_clicks')
      end
    end

    %w[ACCOUNT FUNDING_INSTRUMENT].each do |entity|
      context "when ENGAGEMENT is requested for #{entity}, which does not report clicks" do
        let(:column_names) { guessed_column_names(entity: entity, metric_groups: ['ENGAGEMENT']) }

        it 'does not offer link_clicks' do
          expect(column_names).not_to include('link_clicks')
        end

        it 'does not offer url_clicks' do
          expect(column_names).not_to include('url_clicks')
        end
      end
    end

    context 'when ENGAGEMENT is not requested' do
      let(:column_names) { guessed_column_names(entity: 'CAMPAIGN', metric_groups: ['BILLING']) }

      it 'does not offer link_clicks' do
        expect(column_names).not_to include('link_clicks')
      end
    end
  end

  describe '#run' do
    # date and campaign_id are handled by branches above the metrics lookup, so they must never be
    # reported as missing metrics.
    let(:columns) do
      [
        { 'name' => 'date', 'type' => 'timestamp' },
        { 'name' => 'campaign_id', 'type' => 'string' },
        { 'name' => 'clicks', 'type' => 'long' },
        { 'name' => 'url_clicks', 'type' => 'long' },
        { 'name' => 'link_clicks', 'type' => 'long' },
      ]
    end

    let(:page_builder) { double('page_builder', add: nil, finish: nil) }
    let(:warnings) { [] }

    let(:plugin) do
      instance = described_class.allocate
      {
        '@account_id' => account_id,
        '@entity' => 'CAMPAIGN',
        '@metric_groups' => ['ENGAGEMENT'],
        '@granularity' => 'DAY',
        '@placement' => 'ALL_ON_TWITTER',
        '@start_date' => '2026-08-01',
        '@end_date' => '2026-08-01',
        '@timezone' => 'UTC',
        '@columns' => columns,
        '@request_entities_limit' => 1000,
      }.each { |name, value| instance.instance_variable_set(name, value) }
      instance
    end

    # One stats item per given metrics hash, each belonging to its own campaign.
    def stub_api(*metrics_per_item)
      allow(plugin).to receive(:page_builder).and_return(page_builder)
      allow(plugin).to receive(:get_access_token).and_return(access_token)
      allow(plugin).to receive(:request_entities).and_return(
        Array.new(metrics_per_item.length) { |i| { 'id' => "campaign_#{i}" } }
      )
      allow(plugin).to receive(:request_stats).and_return(
        metrics_per_item.each_with_index.map do |metrics, i|
          { 'id' => "campaign_#{i}", 'id_data' => [{ 'metrics' => metrics }] }
        end
      )
    end

    def reported_missing_metrics
      expect(warnings.length).to eq(1)
      warnings.first[/filled with NULL: (.+?)\./, 1].split(', ')
    end

    before do
      Time.zone = 'UTC'
      allow(Embulk.logger).to receive(:info)
      allow(Embulk.logger).to receive(:warn) { |message| warnings << message }
    end

    context 'when a configured metric is absent from the response' do
      before { stub_api({ 'clicks' => [10], 'link_clicks' => [7] }) }

      it 'warns naming only the absent metric' do
        plugin.run
        expect(reported_missing_metrics).to contain_exactly('url_clicks')
      end

      it 'still writes the same row, with NULL for the absent metric' do
        plugin.run
        expect(page_builder).to have_received(:add)
          .with([Time.zone.parse('2026-08-01'), 'campaign_0', 10, nil, 7])
      end
    end

    context 'when every configured metric is present' do
      before { stub_api({ 'clicks' => [10], 'url_clicks' => [3], 'link_clicks' => [7] }) }

      it 'does not warn' do
        plugin.run
        expect(warnings).to be_empty
      end
    end

    context 'when a metric key is present but null' do
      before { stub_api({ 'clicks' => [10], 'url_clicks' => nil, 'link_clicks' => [7] }) }

      it 'does not warn, because the metric was returned and simply has no value' do
        plugin.run
        expect(warnings).to be_empty
      end

      it 'writes NULL for it, as before' do
        plugin.run
        expect(page_builder).to have_received(:add)
          .with([Time.zone.parse('2026-08-01'), 'campaign_0', 10, nil, 7])
      end
    end

    context 'when a metric is absent for one entity but returned for another' do
      before do
        stub_api(
          { 'clicks' => [10], 'link_clicks' => [7] },
          { 'clicks' => [20], 'url_clicks' => [5], 'link_clicks' => [9] },
        )
      end

      it 'does not warn, because X omits keys for entities with no activity' do
        plugin.run
        expect(warnings).to be_empty
      end
    end

    context 'when no stats are returned at all' do
      before { stub_api }

      it 'does not warn, because nothing was requested of the API' do
        plugin.run
        expect(warnings).to be_empty
      end
    end
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
