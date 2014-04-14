module Pebbles
  module River

    # Implements a queue worker.
    class Worker

      class << self
        def run(handler, options = {})
          Worker.new(handler, options).run
        end
      end

      attr_reader :queue_options
      attr_reader :handler
      attr_reader :river

      # Initializes worker with a handler. Options:
      #
      # * `queue`: Same queue options as `River#queue`.
      # * `on_exception`: If provided, called when a message could not be handled
      #   due to an exception.
      # * `on_connection_error`: If provided, call on recovered connection errors.
      #   Uses `on_exception` if not implemented.
      # * `managed_acking`: If true, ack/nack handling is automatic; every message
      #   is automatically acked unless the handler returns false or the handler
      #   raises an exception, in which case it's nacked. If false, the handler
      #   must do the ack/nacking. Defaults to true.
      #
      # The handler must implement `call(payload, extra)`, where the payload is
      # the message payload, and the extra argument contains message metadata as
      # a hash. If the handler returns false, it is considered rejected, and will
      # be nacked. Otherwise, the message with be acked.
      #
      def initialize(handler, options = {})
        options.assert_valid_keys(
          :queue,
          :on_exception,
          :on_connection_error,
          :managed_acking)

        unless handler.respond_to?(:call)
          raise ArgumentError.new('Handler must implement #call protocool')
        end

        @queue_options = (options[:queue] || {}).freeze
        @managed_acking = !!options.fetch(:managed_acking, true)
        @on_exception = options[:on_exception] || ->(e) { }
        @on_connection_error = options[:on_connection_error] || @on_exception
        @handler = handler
        @river = River.new
        @next_event_time = Time.now
      end

      # Runs the handler once.
      def run_once
        with_exceptions do
          now = Time.now

          if @next_event_time > now
            sleep(@next_event_time - now)
            now = Time.now
          end

          if should_run?
            if process_next
              @next_event_time = now
            else
              if @handler.respond_to?(:on_idle)
                with_exceptions do
                  @handler.on_idle
                end
              end
              @next_event_time = now + 1
            end
          else
            @next_event_time = now + 5
          end
        end
        nil
      end

      # Runs the handler. This will process the queue indefinitely.
      def run
        @enabled = true
        while enabled? do
          run_once
        end
      end

      # Stops any concurrent run.
      def stop
        @enabled = false
      end

      # Are we enabled?
      def enabled?
        @enabled
      end

      private

        def should_run?
          if @handler.respond_to?(:should_run?)
            @handler.should_run?
          else
            true
          end
        end

        def queue
          @river.connect unless @river.connected?
          return @queue ||= @river.queue(@queue_options)
        end

        def process_next
          with_exceptions do
            with_connection_error_handling do
              queue.pop(auto_ack: false, ack: true) do |raw_message|
                if raw_message[:payload] != :queue_empty
                  process_message(raw_message)
                  return true
                else
                  return false
                end
              end
            end
          end
        end

        def process_message(raw_message)
          begin
            message = Message.new(raw_message, queue)
          rescue => exception
            ignore_exceptions do
              queue.nack(delivery_tag: message[:delivery_details][:delivery_tag])
            end
            raise exception
          else
            begin
              result = @handler.call(message)
            rescue *CONNECTION_EXCEPTIONS
              raise
            rescue => exception
              if @managed_acking
                ignore_exceptions do
                  message.nack
                end
              end
              raise exception
            else
              if result != false and @managed_acking
                message.ack
              end
            end
          end
        end

        def with_connection_error_handling(&block)
          yield
        rescue *CONNECTION_EXCEPTIONS => exception
          if @queue
            ignore_exceptions do
              @queue.close
            end
            @queue = nil
          end

          @river.disconnect

          ignore_exceptions do
            @on_connection_error.call(exception)
          end
        end

        def with_exceptions(&block)
          begin
            yield
          rescue => exception
            ignore_exceptions do
              @on_exception.call(exception)
            end
          end
        end

        def ignore_exceptions(&block)
          yield
        rescue
          # Ignore
        end

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
