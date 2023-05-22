require "oauth"
require "active_support"
require "active_support/core_ext/date"
require "active_support/core_ext/time"
require "active_support/core_ext/numeric"

module Embulk
  module Input
    class TwitterAdsAnalytics < InputPlugin
      Plugin.register_input("twitter_ads_analytics", self)

      NUMBER_OF_RETRIES = 5
      MAX_SLEEP_SEC_NUMBER = 1200

      ADS_API_VERSION = 12

      # Error codes and responses
      # @see https://developer.twitter.com/en/docs/twitter-ads-api/response-codes
      # Client Errors (4XX)
      class ClientError < StandardError; end
      class BadRequest < ClientError; end
      class NotAuthorized < ClientError; end
      class Forbidden < ClientError; end
      class NotFound < ClientError; end
      class RateLimit < ClientError; end
      # Server Errors (5XX)
      class ServerError < StandardError; end
      class ServiceUnavailable < ServerError; end

      ERRORS = {
        "400" => BadRequest,
        "401" => NotAuthorized,
        "403" => Forbidden,
        "404" => NotFound,
        "429" => RateLimit,
        "500" => ServerError,
        "503" => ServiceUnavailable
      }.freeze

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
          "request_entities_limit" => config.param("request_entities_limit", :integer, default: 1000),
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
          {name: "campaign_id", type: "string"},
        ] if entity == "LINE_ITEM"
        columns += [
          {name: "promoted_tweet_id", type: "string"},
          {name: "line_item_id", type: "string"},
        ] if entity == "PROMOTED_TWEET"
        columns += [
          {name: "media_creative_id", type: "string"},
          {name: "line_item_id", type: "string"},
        ] if entity == "MEDIA_CREATIVE"
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
          {name: "video_3s100pct_views", type: "long"},
          {name: "video_6s_views", type: "long"},
          {name: "video_15s_views", type: "long"},
        ] if metric_groups.include?("VIDEO")
        columns += [
          {name: "media_views", type: "long"},
          {name: "media_engagements", type: "long"},
        ] if metric_groups.include?("MEDIA")
        columns += [
          {name: "conversion_purchases", type: "json"},
          {name: "conversion_sign_ups", type: "json"},
          {name: "conversion_site_visits", type: "json"},
          {name: "conversion_downloads", type: "json"},
          {name: "conversion_custom", type: "json"},
        ] if metric_groups.include?("WEB_CONVERSION")
        columns += [
          {name: "mobile_conversion_spent_credits", type: "json"},
          {name: "mobile_conversion_installs", type: "json"},
          {name: "mobile_conversion_content_views", type: "json"},
          {name: "mobile_conversion_add_to_wishlists", type: "json"},
          {name: "mobile_conversion_checkouts_initiated", type: "json"},
          {name: "mobile_conversion_reservations", type: "json"},
          {name: "mobile_conversion_tutorials_completed", type: "json"},
          {name: "mobile_conversion_achievements_unlocked", type: "json"},
          {name: "mobile_conversion_searches", type: "json"},
          {name: "mobile_conversion_add_to_carts", type: "json"},
          {name: "mobile_conversion_payment_info_additions", type: "json"},
          {name: "mobile_conversion_re_engages", type: "json"},
          {name: "mobile_conversion_shares", type: "json"},
          {name: "mobile_conversion_rates", type: "json"},
          {name: "mobile_conversion_logins", type: "json"},
          {name: "mobile_conversion_updates", type: "json"},
          {name: "mobile_conversion_levels_achieved", type: "json"},
          {name: "mobile_conversion_invites", type: "json"},
          {name: "mobile_conversion_key_page_views", type: "json"},
        ] if metric_groups.include?("MOBILE_CONVERSION")
        columns += [
          {name: "mobile_conversion_lifetime_value_purchases", type: "json"},
          {name: "mobile_conversion_lifetime_value_sign_ups", type: "json"},
          {name: "mobile_conversion_lifetime_value_updates", type: "json"},
          {name: "mobile_conversion_lifetime_value_tutorials_completed", type: "json"},
          {name: "mobile_conversion_lifetime_value_reservations", type: "json"},
          {name: "mobile_conversion_lifetime_value_add_to_carts", type: "json"},
          {name: "mobile_conversion_lifetime_value_add_to_wishlists", type: "json"},
          {name: "mobile_conversion_lifetime_value_checkouts_initiated", type: "json"},
          {name: "mobile_conversion_lifetime_value_levels_achieved", type: "json"},
          {name: "mobile_conversion_lifetime_value_achievements_unlocked", type: "json"},
          {name: "mobile_conversion_lifetime_value_shares", type: "json"},
          {name: "mobile_conversion_lifetime_value_invites", type: "json"},
          {name: "mobile_conversion_lifetime_value_payment_info_additions", type: "json"},
          {name: "mobile_conversion_lifetime_value_spent_credits", type: "json"},
          {name: "mobile_conversion_lifetime_value_rates", type: "json"},
        ] if metric_groups.include?("LIFE_TIME_VALUE_MOBILE_CONVERSION")
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
        @request_entities_limit = task["request_entities_limit"]

        Time.zone = @timezone
      end

      def run
        access_token = get_access_token
        entities = request_entities(access_token)
        stats = []
        entities.each_slice(10) do |chunked_entities|
          chunked_times.each do |chunked_time|
            response = request_stats(access_token, chunked_entities.map { |entity| entity["id"] }, chunked_time)
            line_item_campaign_id = {}
            if @entity == "LINE_ITEM"
              line_item_campaign_id = chunked_entities.map {|entity| [entity["id"], entity["campaign_id"]]}.to_h
            end
            entity_line_item_id = {}
            if has_line_item_id?(@entity)
              entity_line_item_id = chunked_entities.map {|entity| [entity["id"], entity["line_item_id"]]}.to_h
            end
            response.each do |row|
              row["start_date"] = chunked_time[:start_date]
              row["end_date"] = chunked_time[:end_date]
              row["campaign_id"] = line_item_campaign_id[row["id"]] if @entity == "LINE_ITEM"
              row["line_item_id"] = entity_line_item_id[row["id"]] if has_line_item_id?(@entity)
            end
            stats += response
          end
        end
        stats.each do |item|
          metrics = item["id_data"][0]["metrics"]
          (Date.parse(item["start_date"])..Date.parse(item["end_date"])).each_with_index do |date, i|
            page = []
            @columns.each do |column|
              if @entity == "LINE_ITEM" && column["name"] == "campaign_id"
                page << item["campaign_id"]
              elsif has_line_item_id?(@entity) && column["name"] == "line_item_id"
                page << item["line_item_id"]
              elsif [
                "account_id",
                "campaign_id",
                "line_item_id",
                "promoted_tweet_id",
                "media_creative_id",
                "funding_instrument_id",
              ].include?(column["name"])
                page << item["id"]
              elsif column["name"] == "date"
                page << Time.zone.parse(date.to_s)
              elsif ["account_name", "campaign_name", "line_item_name"].include?(column["name"])
                page << entities.find { |entity| entity["id"] == item["id"] }["name"]
              elsif column["name"] == "description"
                page << entities.find { |entity| entity["id"] == item["id"] }["description"]
              else
                if !metrics[column["name"]]
                  page << nil
                elsif column["type"] == "json"
                  page << metrics[column["name"]]
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

      def request_entities_one_page(access_token, cursor)
        retries = 0
        begin
          arg = {count: @request_entities_limit}
          arg[:cursor] = cursor if cursor
          query = arg.to_query
          url = "https://ads-api.twitter.com/#{ADS_API_VERSION}/accounts/#{@account_id}/#{entity_plural(@entity).downcase}?#{query}"
          url = "https://ads-api.twitter.com/#{ADS_API_VERSION}/accounts/#{@account_id}?#{query}" if @entity == "ACCOUNT"
          response = access_token.request(:get, url)
          if ERRORS["#{response.code}"].present?
            Embulk.logger.error "#{response.body}"
            raise ERRORS["#{response.code}"]
          end
          response_json = JSON.parse(response.body)
          response_data = response_json["data"]

          {
            data: @entity == "ACCOUNT" ? [response_data] : response_data,
            next_cursor: response_json["next_cursor"]
          }
        rescue RateLimit, ServerError => e
          if retries < NUMBER_OF_RETRIES
            retries += 1
            sleep_sec = get_sleep_sec(response: response, retries: retries)
            if sleep_sec > MAX_SLEEP_SEC_NUMBER
              raise e
            end
            Embulk.logger.info "waiting for retry #{sleep_sec} seconds"
            sleep sleep_sec
            Embulk.logger.warn("retry #{retries}, #{e.message}")
            retry
          else
            Embulk.logger.error("exceeds the upper limit retry, #{e.message}")
            raise e
          end
        end
      end

      def request_entities(access_token)
        cursor = nil
        data = []
        loop do
          response = request_entities_one_page(access_token, cursor)
          data += response[:data]
          cursor = response[:next_cursor]
          break unless cursor
        end
        data
      end

      def request_stats(access_token, entity_ids, chunked_time)
        retries = 0
        begin
          params = {
            entity: @entity,
            entity_ids: entity_ids.join(","),
            metric_groups: @metric_groups.join(","),
            start_time: chunked_time[:start_time],
            end_time: chunked_time[:end_time],
            placement: @placement,
            granularity: @granularity,
          }
          response = access_token.request(:get, "https://ads-api.twitter.com/#{ADS_API_VERSION}/stats/accounts/#{@account_id}?#{URI.encode_www_form(params)}")
          if ERRORS["#{response.code}"].present?
            Embulk.logger.error "#{response.body}"
            raise ERRORS["#{response.code}"]
          end
          JSON.parse(response.body)["data"]
        rescue RateLimit, ServerError => e
          if retries < NUMBER_OF_RETRIES
            retries += 1
            sleep_sec = get_sleep_sec(response: response, retries: retries)
            if sleep_sec > MAX_SLEEP_SEC_NUMBER
              raise e
            end
            Embulk.logger.info "waiting for retry #{sleep_sec} seconds"
            sleep sleep_sec
            Embulk.logger.warn("retry #{retries}, #{e.message}")
            retry
          else
            Embulk.logger.error("exceeds the upper limit retry, #{e.message}")
            raise e
          end
        end
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
        when "MEDIA_CREATIVE"
          "MEDIA_CREATIVES"
        when "ACCOUNT"
          "ACCOUNTS"
        when "FUNDING_INSTRUMENT"
          "FUNDING_INSTRUMENTS"
        end
      end

      private

      def has_line_item_id?(entity)
        entity == "PROMOTED_TWEET" || entity == "MEDIA_CREATIVE"
      end

      def get_sleep_sec(response:, retries:)
        rate_limit_reset_timestamp = get_rate_limit_reset_timestamp(response: response)
        if rate_limit_reset_timestamp.present?
          return [rate_limit_reset_timestamp.to_i - Time.now.to_i, 0].max
        else
          return retries.second
        end
      end

      # https://developer.twitter.com/ja/docs/twitter-ads-api/rate-limiting
      def get_rate_limit_reset_timestamp(response:)
        # It is stored in the header in the format key => [value].
        # @example x-rate-limit-reset"=>["1638316658"]
        response.header.to_hash.fetch("x-account-rate-limit-reset", [])[0] || response.header.to_hash.fetch("x-rate-limit-reset", [])[0]
      end
    end
  end
end
