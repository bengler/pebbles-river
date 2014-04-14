module Pebbles
  module River

    class SendFailure < StandardError

      attr_reader :connection_exception

      def initialize(message, connection_exception = nil)
        super(message)
        @connection_exception = connection_exception
      end

    end

    class River

      attr_reader :environment

      def initialize(options = {})
        options = {environment: options} if options.is_a?(String)  # Backwards compatibility

        @environment = (options[:environment] || ENV['RACK_ENV'] || 'development').dup.freeze
      end

      def connected?
        bunny.connected?
      end

      def connect
        unless connected?
          bunny.start
          bunny.qos
        end
      end

      def disconnect
        bunny.stop if connected?
      end

      def publish(options = {})
        connect
        handle_connection_error do
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

        def handle_connection_error(&block)
          retry_until = nil
          begin
            yield
          rescue *CONNECTION_EXCEPTIONS => exception
            retry_until ||= Time.now + 4
            if Time.now < retry_until
              sleep(0.5)
              retry
            else
              raise SendFailure.new(exception.message, exception)
            end
          end
        end

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
