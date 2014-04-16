module Pebbles
  module River

    class River

      attr_reader :environment

      def initialize(options = {})
        options = {environment: options} if options.is_a?(String)  # Backwards compatibility

        @environment = (options[:environment] || ENV['RACK_ENV'] || 'development').dup.freeze
        @last_connect_attempt = nil
      end

      def connected?
        bunny.connected?
      end

      def connect
        unless connected?
          handle_connection_error do
            bunny.start
            bunny.qos
          end
        end
      end

      def disconnect
        if connected?
          begin
            bunny.stop
          rescue *CONNECTION_EXCEPTIONS
            # Ignore
          end
        end
      end

      def publish(options = {})
        connect
        handle_connection_error(SendFailure) do
          exchange.publish(options.to_json,
            persistent: options.fetch(:persistent, true),
            key: Routing.routing_key_for(options.slice(:event, :uid)))
        end
      end

      def queue(options = {})
        raise ArgumentError.new 'Queue must be named' unless options[:name]

        connect

        queue = bunny.queue(options[:name], QUEUE_OPTIONS.dup)
        Subscription.new(options).queries.each do |key|
          queue.bind(exchange.name, key: key)
        end
        queue
      end

      def exchange_name
        return @exchange_name ||= format_exchange_name
      end

      private

        def bunny
          @bunny ||= Bunny.new
        end

        def format_exchange_name
          name = 'pebblebed.river'
          name << ".#{environment}" if @environment != 'production'
          name
        end

        def exchange
          connect
          @exchange ||= bunny.exchange(exchange_name, EXCHANGE_OPTIONS.dup)
        end

        def handle_connection_error(exception_klass = ConnectFailure, &block)
          last_exception = nil
          Timeout.timeout(MAX_RETRY_TIMEOUT) do
            retry_until, retry_count = nil, 0
            begin
              yield
            rescue *CONNECTION_EXCEPTIONS => exception
              last_exception = exception
              retry_count += 1
              backoff(retry_count)
              retry
            end
          end
        rescue Timeout::Error => timeout
          last_exception ||= timeout
          raise exception_klass.new(last_exception.message, last_exception)
        end

        def backoff(iteration)
          sleep([(1.0 / 2.0 * (2.0 ** [30, iteration].min - 1.0)).ceil, MAX_BACKOFF_SECONDS].min)
        end

        MAX_RETRY_TIMEOUT = 10

        MAX_BACKOFF_SECONDS = MAX_RETRY_TIMEOUT

        QUEUE_OPTIONS = {durable: true}.freeze

        EXCHANGE_OPTIONS = {type: :topic, durable: :true}.freeze

        CONNECTION_EXCEPTIONS = [
          Bunny::ConnectionError,
          Bunny::ForcedChannelCloseError,
          Bunny::ForcedConnectionCloseError,
          Bunny::ServerDownError,
          Bunny::ProtocolError,
          # These should be caught by Bunny, but apparently aren't.
          Errno::ECONNRESET
        ].freeze

    end

  end
end
