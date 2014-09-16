module Pebbles
  module River

    class ConfigurationError < StandardError; end

    # A simple supervisor which runs workers in preforked pools of child processes.
    # If a worker dies, a new child process is created.
    class Supervisor < Servolux::Server

      def initialize(name, options = {})
        super(name, {interval: 5}.merge(options.slice(:logger, :pid_file)))

        options.assert_valid_keys(:logger, :pid_file, :worker_count, :worker)

        @worker_count = options[:worker_count] || 1

        @prefork_pools = []
        @worker_modules = []
      end

      def start_workers
        if @worker_modules.empty?
          raise ConfigurationError.new("No listeners configured")
        end

        @worker_modules.each do |min_worker_count, m|
          @prefork_pools.push(
            Servolux::Prefork.new(
              min_workers: min_worker_count,
              module: m))
        end
      end

      # Add a listener. The listener must support the `#call(message)` method.
      # The queue specification contains the parameters naming the queue and
      # so on; see `Pebbles::River::River#queue`. The worker options:
      #
      # * `managed_acking`: Passed along to `Pebbles::River::Worker.new`.
      # * `worker_count`: Number of parallel workers to run. Defaults to the
      #   global setting.
      #
      def add_listener(listener, queue_spec, worker_options = {})
        worker_options.assert_valid_keys(:managed_acking, :worker_count)

        worker = Pebbles::River::Worker.new(listener,
          queue: queue_spec,
          managed_acking: worker_options[:managed_acking],
          on_exception: ->(e) {
            if logger.respond_to?(:exception)
              logger.exception(e)
            else
              logger.error("Exception #{e.class}: #{e} #{e.backtrace.join("\n")}")
            end
          })

        process_name = "#{@name}: queue worker: #{queue_spec[:name]}"
        logger = @logger
        worker_count = worker_options[:worker_count] || @worker_count

        @worker_modules.push([worker_count, Module.new {
          define_method :execute do
            $0 = process_name
            trap('TERM') do
              logger.info "Worker received TERM"
              worker.stop
              exit(0)
            end
            worker.run
          end
        }])
      end

      # From Servolux::Server
      def before_starting
        $0 = "#{name}: master"

        logger.info "Starting workers"
        @prefork_pools.each do |prefork|
          prefork.start(1)
        end
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
        @prefork_pools.each do |prefork|
          prefork.ensure_worker_pool_size
        end
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
          @prefork_pools.each(&:stop)
          loop do
            break if @prefork_pools.all? { |prefork| prefork.live_worker_count <= 0 }
            logger.info "Waiting for workers to quit"
            sleep 0.25
          end
        end

    end

  end
end
