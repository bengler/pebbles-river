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
      # * `on_exception`: If provided, called with `exception` as an argument
      #   when a message could not be handled due to an exception. (Connection
      #   errors are not reported, however.)
      # * `logger`: Optional logger. Defaults to stderr. Pass nil to disable.
      # * `managed_acking`: If true, ack/nack handling is automatic; every message
      #   is automatically acked unless the handler returns false or the handler
      #   raises an exception, in which case it's nacked. If false, the handler
      #   must do the ack/nacking. Defaults to true.
      # * `prefetch`: If specified, sets channel's prefetch count.
      #
      # The handler must implement `call(payload, extra)`, where the payload is
      # the message payload, and the extra argument contains message metadata as
      # a hash. If the handler returns false, it is considered rejected, and will
      # be nacked. Otherwise, the message with be acked.
      #
      def initialize(handler, options = {})
        options.assert_valid_keys(
          :queue,
          :logger,
          :on_exception,
          :managed_acking,
          :prefetch)

        unless handler.respond_to?(:call)
          raise ArgumentError.new('Handler must implement #call protocool')
        end

        @queue_options = (options[:queue] || {}).freeze
        if options[:managed_acking] != nil
          @managed_acking = !!options.fetch(:managed_acking, true)
        else
          @managed_acking = true
        end
        @dead_lettered = !!@queue_options[:dead_letter_routing_key]
        @on_exception = options[:on_exception] || ->(*args) { }
        @handler = handler
        @river = River.new(options.slice(:prefetch))
        @next_event_time = Time.now
        @rate_limiter = RateLimiter.new(1.0, 10)
        @logger = options.fetch(:logger, Logger.new($stderr))
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
            queue.pop(manual_ack: true) do |delivery_info, properties, content|
              if delivery_info
                process_message(delivery_info, properties, content)
                return true
              else
                return false
              end
            end
          end
        end

        def process_message(delivery_info, properties, content)
          begin
            message = Message.new(content, delivery_info, queue)
          rescue InvalidPayloadError => e
            if @logger
              @logger.error("Invalid payload, ignoring message: #{e}")
              reject(delivery_info, requeue: false)
            end
          rescue => e
            ignore_exceptions do
              reject(delivery_info)
            end
            raise e
          else
            begin
              result = @handler.call(message)
            rescue Bunny::Exception
              raise
            rescue => e
              if @managed_acking
                ignore_exceptions do
                  reject(delivery_info)
                end
              end
              raise e
            else
              if @managed_acking
                case result
                  when false
                    reject(delivery_info)
                  else
                    message.ack
                end
              end
            end
          end
        end

        def reject(delivery_info, requeue: nil)
          if requeue.nil?
            # Normally requeue, except if we are dead-lettering to another queue, where
            # requeue = false means to bounce it to DLX.
            requeue = !@dead_lettered
          end
          queue.channel.reject(delivery_info.delivery_tag.to_i, requeue)
        end

        def with_exceptions(&block)
          begin
            yield
          rescue Bunny::Exception
            raise
          rescue Timeout::Error
            if @logger
              @logger.error("Timeout polling for messages (ignoring)")
            end
          rescue => e
            if @logger
              @logger.error("Exception (#{e.class}) while handling message: #{e}")
            end
            @rate_limiter.increment
            ignore_exceptions do
              @on_exception.call(e)
            end
          end
        end

        def ignore_exceptions(&block)
          yield
        rescue => e
          if @logger
            @logger.warn("Ignoring exception (#{e.class}): #{e}")
          end
        end

    end

  end
end
