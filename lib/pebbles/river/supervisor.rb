module Pebbles
  module River

    class ConfigurationError < StandardError; end

    class Supervisor < Servolux::Server

      def initialize(name, options = {})
        super(name, {interval: 5}.merge(options.slice(:logger, :pid_file)))

        options.assert_valid_keys(:logger, :pid_file, :worker_count, :worker)

        @worker_count = options[:worker_count] || 1

        @queue_modules = []
      end

      def start_workers
        if @queue_modules.empty?
          raise ConfigurationError.new("No listeners configured")
        end

        @prefork = MultiPrefork.new(
          min_workers: @worker_count,
          modules: @queue_modules)
      end

      def add_listener(listener, queue_spec)
        worker = Pebbles::River::Worker.new(listener,
          queue: queue_spec,
          on_exception: ->(e) {
            if logger.respond_to?(:exception)
              logger.exception(e)
            else
              logger.error("Exception #{e.class}: #{e} #{e.backtrace.join("\n")}")
            end
          })

        process_name = "#{@name}: queue worker: #{queue_spec[:name]}"
        logger = @logger

        @queue_modules.push(-> {
          $0 = process_name
          trap('TERM') { worker.stop }
          worker.run
        })
      end

      # From Servolux::Server
      def before_starting
        $0 = "#{name}: master"

        logger.info "Starting workers"
        @prefork.start(1)
      end

      # From Servolux::Server
      def after_stopping
        shutdown_workers
      end

      # From Servolux::Server
      def usr2
        shutdown_workers
      end

      # From Servolux::Server
      def run
        @prefork.ensure_worker_pool_size
      rescue => e
        if logger.respond_to? :exception
          logger.exception(e)
        else
          logger.error(e.inspect)
          logger.error(e.backtrace.join("\n"))
        end
      end

      private

        def shutdown_workers
          logger.info "Shutting down all workers"
          @prefork.stop
          loop do
            break if @prefork.live_worker_count <= 0
            logger.info "Waiting for workers to quit"
            sleep 0.25
          end
        end

    end

  end
end
