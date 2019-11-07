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
          "access_token" => config.param("access_token", :string),
          "access_token_secret" => config.param("access_token_secret", :string),
          "account_id" => config.param("account_id", :string),
          "entity" => config.param("entity", :string),
          "metric_groups" => config.param("metric_groups", :array),
          "granularity" => config.param("granularity", :string),
          "placement" => config.param("placement", :string),
          "start_date" => config.param("start_date", :string),
          "end_date" => config.param("end_date", :string),
          "timezone" => config.param("timezone", :string),
          "async" => config.param("timezone", :bool),
          "fields" => config.param("fields", :array),
        }

        columns = []
        task["fields"].each_with_index do |field, i|
          columns << Column.new(i, field["name"], field["type"].to_sym)
        end

        resume(task, columns, 1, &control)
      end

      def self.resume(task, columns, count, &control)
        task_reports = yield(task, columns, count)

        next_config_diff = {}
        return next_config_diff
      end

      def self.guess(config)
        entity = config.param("entity", :string)
        metric_groups = config.param("metric_groups", :array)
        fields = [
          {name: "date", type: "string"},
          {name: "id", type: "string"},
          {name: "name", type: "string"},
        ]
        fields += [
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
        ] if metric_groups.include?("ENGAGEMENT") && entity != "ACCOUNT"
        fields += [
          {name: "engagements", type: "long"},
          {name: "impressions", type: "long"},
          {name: "retweets", type: "long"},
          {name: "replies", type: "long"},
          {name: "likes", type: "long"},
          {name: "follows", type: "long"},
        ] if metric_groups.include?("ENGAGEMENT") && entity == "ACCOUNT"
        fields += [
          {name: "billed_engagements", type: "long"},
          {name: "billed_charge_local_micro", type: "long"},
        ] if metric_groups.include?("BILLING")
        fields += [
          {name: "media_views", type: "long"},
          {name: "media_engagements", type: "long"},
        ] if metric_groups.include?("MEDIA")
        return {"fields" => fields}
      end

      def init
        # initialization code:
        @consumer_key = task["consumer_key"]
        @consumer_secret = task["consumer_secret"]
        @access_token = task["access_token"]
        @access_token_secret = task["access_token_secret"]
        @account_id = task["account_id"]
        @entity = task["entity"]
        @metric_groups = task["metric_groups"]
        @granularity = task["granularity"]
        @placement = task["placement"]
        @start_date = task["start_date"]
        @end_date = task["end_date"]
        @timezone = task["timezone"]
        @async = task["async"]
        @fields = task["fields"]

        Time.zone = @timezone
        @start_time = Time.zone.parse(@start_date).strftime("%FT%T%z")
        @end_time = Time.zone.parse(@end_date).tomorrow.strftime("%FT%T%z")
      end

      def run
        token = get_token
        entities = request_entities(token)
        stats = request_stats(token, entities.map{ |entity| entity["id"] })
        stats.each do |item|
          metrics = item["id_data"][0]["metrics"]
          (Date.parse(@start_date)..Date.parse(@end_date)).each_with_index do |date, i|
            page = []
            @fields.each do |field|
              if field["name"] == "id"
                page << item["id"]
              elsif field["name"] == "date"
                page << date.to_s
              elsif field["name"] == "name"
                page << entities.find { |entity| entity["id"] == item["id"] }["name"]
              else
                unless metrics[field["name"]]
                  page << nil
                else
                  page << metrics[field["name"]][i]
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

      def get_token
        consumer = OAuth::Consumer.new(@consumer_key, @consumer_secret, site: "https://ads-api.twitter.com", scheme: :header)
        OAuth::AccessToken.from_hash(consumer, oauth_token: @access_token, oauth_token_secret: @access_token_secret)
      end
  
      def request_entities(token)
        response = token.request(:get, "https://ads-api.twitter.com/6/accounts/#{@account_id}/#{entity_plural(@entity).downcase}")
        JSON.parse(response.body)["data"]
      end

      def request_stats(token, entity_ids)
        params = {
          entity: @entity.upcase,
          entity_ids: entity_ids.join(","),
          metric_groups: @metric_groups.join(","),
          start_time: @start_time,
          end_time: @end_time,
          placement: @placement.upcase,
          granularity: @granularity.upcase,
        }
        response = token.request(:get, "https://ads-api.twitter.com/6/stats/accounts/#{@account_id}?#{URI.encode_www_form(params)}")
        JSON.parse(response.body)["data"]
      end

      def entity_plural(entity)
        case entity
        when "CAMPAIGN"
          "CAMPAIGNS"
        when "LINE_ITEM"
          "LINE_ITEMS"
        when "PROMOTED_TWEET"
          "PROMOTED_TWEETS"
        when "MEDIA_CREATIVE"
          "MEDIA_CREATIVES"
        when "ACCOUNT"
          "ACCOUNTS"
        when "FUNDING_INSTRUMENT"
          "FUNDING_INSTRUMENTS"
        end
      end
    end
  end
end
