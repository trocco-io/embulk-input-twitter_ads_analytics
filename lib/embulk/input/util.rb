require "active_support/core_ext/time"

class Util
  class << self
    def filter_entities_by_time_string(data, start_date_string, end_date_string, timezone_string)
      return data unless timezone_string

      tz = ActiveSupport::TimeZone[timezone_string]

      if start_date_string
        start_date = tz.parse(start_date_string)
        data = data.select { |x| start_date <= Time.parse(x["created_at"]) }
      end

      if end_date_string
        end_date = tz.parse(end_date_string)
        data = data.select { |x| end_date >= Time.parse(x["created_at"]) }
      end

      data
    end
  end
end
