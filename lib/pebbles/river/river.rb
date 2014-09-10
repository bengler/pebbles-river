module Pebbles
  module River

    class River

      attr_reader :environment
      attr_reader :exchange
      attr_reader :session
      attr_reader :channel

      def initialize(options = {})
        options = {environment: options} if options.is_a?(String)  # Backwards compatibility

        @environment = (options[:environment] || ENV['RACK_ENV'] || 'development').dup.freeze
        @last_connect_attempt = nil
      end

      def connected?
        @session && @session.connected?
      end

      def connect
        unless @session and @channel and @exchange
          disconnect
          handle_session_error do
            session = Bunny::Session.new(::Pebbles::River.rabbitmq_options)
            session.start

            channel = session.create_channel

            exchange = channel.exchange(exchange_name, EXCHANGE_OPTIONS.dup)

            @session, @channel, @exchange = session, channel, exchange
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
        raise ArgumentError.new 'Queue must be named' unless options[:name]

        queue_opts = {durable: true}
        if (ttl = options[:ttl])
          queue_opts[:arguments] = {'x-message-ttl' => ttl}
        end

        connect
        queue = @channel.queue(options[:name], queue_opts)
        Subscription.new(options).queries.each do |key|
          queue.bind(exchange.name, key: key)
        end
        queue
      end

      private

        def exchange_name
          return @exchange_name ||= format_exchange_name
        end

        def format_exchange_name
          name = 'pebblebed.river'
          name << ".#{environment}" if @environment != 'production'
          name
        end

        def handle_session_error(exception_klass = ConnectFailure, &block)
          last_exception = nil
          Timeout.timeout(MAX_RETRY_TIMEOUT) do
            retry_until, retry_count = nil, 0
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
        rescue Timeout::Error => timeout
          last_exception ||= timeout
          raise exception_klass.new(last_exception.message, last_exception)
        end

        def backoff(iteration)
          sleep([(1.0 / 2.0 * (2.0 ** [30, iteration].min - 1.0)).ceil, MAX_BACKOFF_SECONDS].min)
        end

        MAX_RETRY_TIMEOUT = 10

        MAX_BACKOFF_SECONDS = MAX_RETRY_TIMEOUT

        EXCHANGE_OPTIONS = {type: :topic, durable: :true}.freeze

        CONNECTION_EXCEPTIONS = [
          Bunny::Exception,
          # These should be caught by Bunny, but apparently aren't.
          Errno::ECONNRESET
        ].freeze

    end

  end
end
