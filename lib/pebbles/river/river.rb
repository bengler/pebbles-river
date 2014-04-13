module Pebbles
  module River

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
        exchange.publish(options.to_json,
          persistent: options.fetch(:persistent, true),
          key: Routing.routing_key_for(options.slice(:event, :uid)))
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

        QUEUE_OPTIONS = {durable: true}.freeze

        EXCHANGE_OPTIONS = {type: :topic, durable: :true}.freeze

    end

  end
end
