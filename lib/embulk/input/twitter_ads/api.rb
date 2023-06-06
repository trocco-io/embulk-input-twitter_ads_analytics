require "active_support"
require "active_support/core_ext/numeric"

module Embulk
  module Input
    module TwitterAds
      module API
        NUMBER_OF_RETRIES = 5
        MAX_SLEEP_SEC_NUMBER = 1200

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

        def with_retry(logger:)
          retries = 0
          begin
            response = yield
            if ERRORS["#{response.code}"].present?
              logger.error "#{response.body}"
              raise ERRORS["#{response.code}"]
            end
            return response
          rescue RateLimit, ServerError => e
            if retries < NUMBER_OF_RETRIES
              retries += 1
              sleep_sec = get_sleep_sec(response: response, retries: retries)
              if sleep_sec > MAX_SLEEP_SEC_NUMBER
                raise e
              end
              logger.info "waiting for retry #{sleep_sec} seconds"
              sleep sleep_sec
              logger.warn("retry #{retries}, #{e.message}")
              retry
            else
              logger.error("exceeds the upper limit retry, #{e.message}")
              raise e
            end
          end
        end

        private
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
end
