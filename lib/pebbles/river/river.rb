module Pebbles
  module River

    class River

      attr_reader :environment
      attr_reader :exchange
      attr_reader :session
      attr_reader :channel
      attr_reader :prefetch
      attr_reader :exchange_name

      def initialize(options = {})
        options = {environment: options} if options.is_a?(String)  # Backwards compatibility

        @environment = (options[:environment] || ENV['RACK_ENV'] || 'development').dup.freeze

        @exchange_name = 'pebblebed.river'
        @exchange_name << ".#{environment}" if @environment != 'production'

        @last_connect_attempt = nil

        @prefetch = options[:prefetch]
      end

      def connected?
        @session && @session.connected?
      end

      def connect
        unless @session and @channel and @exchange
          disconnect
          handle_session_error do
            @session = Bunny::Session.new(::Pebbles::River.rabbitmq_options)
            @session.start

            @channel = @session.create_channel
            @channel.prefetch(@prefetch) if @prefetch

            @exchange = @channel.exchange(@exchange_name, EXCHANGE_OPTIONS.dup)
          end
        end
      end

      def disconnect
        if @channel
          begin
            @channel.close
          rescue *CONNECTION_EXCEPTIONS
            # Ignore
          end
          @channel = nil
        end
        if @session
          begin
            @session.stop
          rescue *CONNECTION_EXCEPTIONS
            # Ignore
          end
          @session = nil
        end
        @exchange = nil
      end

      def publish(options = {})
        handle_session_error(SendFailure) do
          connect

          # Note: Using self.exchange so it can be stubbed in tests
          self.exchange.publish(options.to_json,
            persistent: options.fetch(:persistent, true),
            key: Routing.routing_key_for(options.slice(:event, :uid)))
        end
      end

      def queue(options = {})
        options.assert_valid_keys(:name, :ttl, :event, :path, :klass,
          :dead_letter_routing_key, :routing_key)

        raise ArgumentError.new 'Queue must be named' unless options[:name]

        queue_args = {}
        if (ttl = options[:ttl])
          queue_args['x-message-ttl'] = ttl
        end
        if (dead_letter_routing_key = options[:dead_letter_routing_key])
          queue_args['x-dead-letter-exchange'] = @exchange_name
          queue_args['x-dead-letter-routing-key'] = dead_letter_routing_key
        end
        queue_opts = {durable: true, arguments: queue_args}

        connect
        queue = @channel.queue(options[:name], queue_opts)
        if (routing_key = options[:routing_key])
          queue.bind(exchange.name, key: routing_key)
        end
        Routing.binding_routing_keys_for(options.slice(:event, :class, :path)).each do |key|
          queue.bind(exchange.name, key: key)
        end
        queue
      end

      private

        def handle_session_error(exception_klass = ConnectFailure, &block)
          last_exception = nil
          Timeout.timeout(MAX_RETRY_TIMEOUT) do
            retry_count = 0
            begin
              yield
            rescue *CONNECTION_EXCEPTIONS => exception
              disconnect
              last_exception = exception
              retry_count += 1
              backoff(retry_count)
              retry
            end
          end
        rescue Timeout::Error => timeout_exception
          # Timeouts can screw up the connection, so forcibly close it
          disconnect

          if last_exception
            raise exception_klass.new(last_exception.message, last_exception)
          else
            raise exception_klass.new("Timeout", timeout_exception)
          end
        end

        def backoff(iteration)
          sleep([(1.0 / 2.0 * (2.0 ** [30, iteration].min - 1.0)).ceil, MAX_BACKOFF_SECONDS].min)
        end

        MAX_RETRY_TIMEOUT = 30

        MAX_BACKOFF_SECONDS = 10

        EXCHANGE_OPTIONS = {type: :topic, durable: :true}.freeze

    end

  end
end
