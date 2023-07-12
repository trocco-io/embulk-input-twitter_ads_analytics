require 'json'
require_relative 'api'
require_relative 'util'

module Embulk
  module Input
    module TwitterAds
      class Card
        class << self
          include API

          def columns
            [
              { name: "name", type: "string" },
              { name: "components", type: "json" },
              { name: "id", type: "string" },
              { name: "created_at", type: "string" },
              { name: "card_uri", type: "string" },
              { name: "updated_at", type: "string" },
              { name: "deleted", type: "boolean" },
              { name: "card_type", type: "string" },
            ]
          end

          def fetch_pages(api_version:, access_token:, account_id:, logger:, entity_start_date:, entity_end_date:, entity_timezone:, columns:)
            data = Card.request(api_version: api_version,
                                access_token: access_token,
                                account_id: account_id,
                                logger: logger)
            Card.convert_to_pages(data: data,
                                  entity_start_date: entity_start_date,
                                  entity_end_date: entity_end_date,
                                  entity_timezone: entity_timezone,
                                  columns: columns)
          end

          def request(api_version:, access_token:, account_id:, logger:)
            cursor = nil
            data = []
            loop do
              response = with_retry(logger: logger) do
                args = { include_legacy_cards: true }
                args[:cursor] = cursor if cursor
                url = "https://ads-api.twitter.com/#{api_version}/accounts/#{account_id}/cards?#{args.to_query}"
                access_token.request(:get, url)
              end
              response_json = JSON.parse(response.body)
              cursor = response_json['next_cursor']
              data += response_json["data"]
              return data unless cursor
            end
          end

          def convert_to_pages(data:, entity_start_date:, entity_end_date:, entity_timezone:, columns:)
            column_keys = columns.map { |x| x['name'] }
            filted_data = Util.filter_entities_by_time_string(data, entity_start_date, entity_end_date, entity_timezone)
            filted_data.map { |d| column_keys.map { |column| d[column] } }
          end
        end
      end
    end
  end
end
