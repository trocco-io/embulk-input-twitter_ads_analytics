require "oauth"
require "active_support"
require "active_support/core_ext/date"
require "active_support/core_ext/time"
require "active_support/core_ext/numeric"

module Embulk
  module Input

    class TwitterAdsAnalytics < InputPlugin
      Plugin.register_input("twitter_ads_analytics", self)

      def self.transaction(config, &control)
        # configuration code:
        task = {
          "consumer_key" => config.param("consumer_key", :string),
          "consumer_secret" => config.param("consumer_secret", :string),
          "oauth_token" => config.param("oauth_token", :string),
          "oauth_token_secret" => config.param("oauth_token_secret", :string),
          "account_id" => config.param("account_id", :string),
          "entity" => config.param("entity", :string).upcase,
          "metric_groups" => config.param("metric_groups", :array).map(&:upcase),
          "granularity" => config.param("granularity", :string).upcase,
          "placement" => config.param("placement", :string).upcase,
          "start_date" => config.param("start_date", :string),
          "end_date" => config.param("end_date", :string),
          "timezone" => config.param("timezone", :string),
          "async" => config.param("timezone", :bool),
          "columns" => config.param("columns", :array),
        }

        columns = []
        task["columns"].each_with_index do |column, i|
          columns << Column.new(i, column["name"], column["type"].to_sym, column["format"])
        end

        resume(task, columns, 1, &control)
      end

      def self.resume(task, columns, count, &control)
        task_reports = yield(task, columns, count)

        next_config_diff = {}
        return next_config_diff
      end

      def self.guess(config)
        entity = config.param("entity", :string).upcase
        metric_groups = config.param("metric_groups", :array).map(&:upcase)
        columns = [
          {name: "date", type: "timestamp", format: "%Y-%m-%d"},
        ]
        columns += [
          {name: "account_id", type: "string"},
          {name: "account_name", type: "string"},
        ] if entity == "ACCOUNT"
        columns += [
          {name: "campaign_id", type: "string"},
          {name: "campaign_name", type: "string"},
        ] if entity == "CAMPAIGN"
        columns += [
          {name: "line_item_id", type: "string"},
          {name: "line_item_name", type: "string"},
        ] if entity == "LINE_ITEM"
        columns += [
          {name: "funding_instrument_id", type: "string"},
          {name: "description", type: "string"},
        ] if entity == "FUNDING_INSTRUMENT"
        columns += [
          {name: "engagements", type: "long"},
          {name: "impressions", type: "long"},
          {name: "retweets", type: "long"},
          {name: "replies", type: "long"},
          {name: "likes", type: "long"},
          {name: "follows", type: "long"},
          {name: "card_engagements", type: "long"},
          {name: "clicks", type: "long"},
          {name: "app_clicks", type: "long"},
          {name: "url_clicks", type: "long"},
          {name: "qualified_impressions", type: "long"},
        ] if metric_groups.include?("ENGAGEMENT") && (entity != "ACCOUNT" && entity != "FUNDING_INSTRUMENT")
        columns += [
          {name: "engagements", type: "long"},
          {name: "impressions", type: "long"},
          {name: "retweets", type: "long"},
          {name: "replies", type: "long"},
          {name: "likes", type: "long"},
          {name: "follows", type: "long"},
        ] if metric_groups.include?("ENGAGEMENT") && (entity == "ACCOUNT" || entity == "FUNDING_INSTRUMENT")
        columns += [
          {name: "billed_engagements", type: "long"},
          {name: "billed_charge_local_micro", type: "long"},
        ] if metric_groups.include?("BILLING")
        columns += [
          {name: "video_total_views", type: "long"},
          {name: "video_views_25", type: "long"},
          {name: "video_views_50", type: "long"},
          {name: "video_views_75", type: "long"},
          {name: "video_views_100", type: "long"},
          {name: "video_cta_clicks", type: "long"},
          {name: "video_content_starts", type: "long"},
          {name: "video_mrc_views", type: "long"},
          {name: "video_3s100pct_views", type: "long"},
        ] if metric_groups.include?("VIDEO")
        columns += [
          {name: "media_views", type: "long"},
          {name: "media_engagements", type: "long"},
        ] if metric_groups.include?("MEDIA")
        return {"columns" => columns}
      end

      def init
        # initialization code:
        @consumer_key = task["consumer_key"]
        @consumer_secret = task["consumer_secret"]
        @oauth_token = task["oauth_token"]
        @oauth_token_secret = task["oauth_token_secret"]
        @account_id = task["account_id"]
        @entity = task["entity"]
        @metric_groups = task["metric_groups"]
        @granularity = task["granularity"]
        @placement = task["placement"]
        @start_date = task["start_date"]
        @end_date = task["end_date"]
        @timezone = task["timezone"]
        @async = task["async"]
        @columns = task["columns"]

        Time.zone = @timezone
      end

      def run
        access_token = get_access_token
        entities = request_entities(access_token)
        stats = []
        entities.each_slice(10) do |chunked_entities|
          chunked_times.each do |chunked_time|
            response = request_stats(access_token, chunked_entities.map{ |entity| entity["id"] }, chunked_time)
            response.each do |row|
              row["start_date"] = chunked_time[:start_date]
              row["end_date"] = chunked_time[:end_date]
            end
            stats += response
          end
        end
        stats.each do |item|
          metrics = item["id_data"][0]["metrics"]
          (Date.parse(item["start_date"])..Date.parse(item["end_date"])).each_with_index do |date, i|
            page = []
            @columns.each do |column|
              if ["account_id", "campaign_id", "line_item_id", "funding_instrument_id"].include?(column["name"])
                page << item["id"]
              elsif column["name"] == "date"
                page << Time.zone.parse(date.to_s)
              elsif ["account_name", "campaign_name", "line_item_name"].include?(column["name"])
                page << entities.find { |entity| entity["id"] == item["id"] }["name"]
              elsif column["name"] == "description"
                page << entities.find { |entity| entity["id"] == item["id"] }["description"]
              else
                unless metrics[column["name"]]
                  page << nil
                else
                  page << metrics[column["name"]][i]
                end
              end
            end
            page_builder.add(page)
          end
        end
        page_builder.finish

        task_report = {}
        return task_report
      end

      def get_access_token
        consumer = OAuth::Consumer.new(@consumer_key, @consumer_secret, site: "https://ads-api.twitter.com", scheme: :header)
        OAuth::AccessToken.from_hash(consumer, oauth_token: @oauth_token, oauth_token_secret: @oauth_token_secret)
      end
  
      def request_entities(access_token)
        url = "https://ads-api.twitter.com/7/accounts/#{@account_id}/#{entity_plural(@entity).downcase}"
        url = "https://ads-api.twitter.com/7/accounts/#{@account_id}" if @entity == "ACCOUNT"
        response = access_token.request(:get, url)
        if response.code != "200"
          Embulk.logger.error "#{response.body}"
          raise
        end
        return [JSON.parse(response.body)["data"]] if @entity == "ACCOUNT"
        JSON.parse(response.body)["data"]
      end

      def request_stats(access_token, entity_ids, chunked_time)
        params = {
          entity: @entity,
          entity_ids: entity_ids.join(","),
          metric_groups: @metric_groups.join(","),
          start_time: chunked_time[:start_time],
          end_time: chunked_time[:end_time],
          placement: @placement,
          granularity: @granularity,
        }
        response = access_token.request(:get, "https://ads-api.twitter.com/7/stats/accounts/#{@account_id}?#{URI.encode_www_form(params)}")
        if response.code != "200"
          Embulk.logger.error "#{response.body}"
          raise
        end
        JSON.parse(response.body)["data"]
      end

      def chunked_times
        (Date.parse(@start_date)..Date.parse(@end_date)).each_slice(7).map do |chunked|
          {
            start_date: chunked.first.to_s,
            end_date: chunked.last.to_s,
            start_time: Time.zone.parse(chunked.first.to_s).strftime("%FT%T%z"),
            end_time: Time.zone.parse(chunked.last.to_s).tomorrow.strftime("%FT%T%z"),
          }
        end
      end

      def entity_plural(entity)
        case entity
        when "CAMPAIGN"
          "CAMPAIGNS"
        when "LINE_ITEM"
          "LINE_ITEMS"
        when "PROMOTED_TWEET"
          "PROMOTED_TWEETS"
        when "ACCOUNT"
          "ACCOUNTS"
        when "FUNDING_INSTRUMENT"
          "FUNDING_INSTRUMENTS"
        end
      end
    end
  end
end
