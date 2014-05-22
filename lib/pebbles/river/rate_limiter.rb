module Pebbles
  module River

    # Simple queueless token-bucket limiter.
    class RateLimiter

      def initialize(max_rate, window_seconds)
        @last_check = Time.now
        @max_rate = @allowance = max_rate
        @window_seconds = window_seconds
      end

      def increment
        now = Time.now
        time_passed = now - @last_check
        @last_check = now

        @allowance += time_passed * (@max_rate / @window_seconds)
        if @allowance > @max_rate
          @allowance = @max_rate
        end
        if @allowance < 1
          sleep((1 - @allowance) * (@window_seconds / @max_rate))
          @allowance = 0
        else
          @allowance -= 1
        end
      end

    end

  end
end