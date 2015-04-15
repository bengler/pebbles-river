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
        @recovering = true
      end

      def start_workers
        if @worker_modules.empty?
          raise ConfigurationError.new("No listeners configured")
        end

        @worker_modules.each do |name, min_worker_count, m|
          if min_worker_count > 0
            prefork = Servolux::Prefork.new(min_workers: min_worker_count, module: m)
            @prefork_pools.push([name, prefork])
          else
            logger.info "[#{name}] Workers disabled"
          end
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

        name = queue_spec[:name]

        process_name = "#{@name}: queue worker: #{name}"
        logger = @logger
        worker_count = worker_options[:worker_count] || @worker_count

        @worker_modules.push([name, worker_count, Module.new {
          define_method :execute do
            $0 = process_name
            trap('TERM') do
              logger.info "[#{name}] Worker received TERM, stopping"
              worker.stop
              exit(0)
            end
            worker.run
          end
        }])
      end

      # From Servolux::Server
      def before_starting
        $0 = "#{self.name}: master"

        logger.info "Starting workers"
        @prefork_pools.each do |name, prefork|
          logger.info "[#{name}] Starting workers"
          prefork.ensure_worker_pool_size
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
        ensure_workers
      rescue => e
        if logger.respond_to? :exception
          logger.exception(e)
        else
          logger.error(e.inspect)
          logger.error(e.backtrace.join("\n"))
        end
      end

      private

        def ensure_workers
          complete = true
          @prefork_pools.each do |name, prefork|
            if prefork.below_minimum_workers?
              complete = false
            else
              had_workers = true
            end

            prefork.prune_workers

            if had_workers and prefork.below_minimum_workers?
              logger.error "[#{name}] One or more worker died"
            end

            while prefork.below_minimum_workers? do
              @recovering = true
              logger.info "[#{name}] Too few workers (" \
                "#{prefork.live_worker_count} alive, #{prefork.dead_worker_count} dead), spawning another"
              prefork.add_workers(1)
            end
          end

          if @recovering and complete
            @recovering = false
            logger.info "All workers up"
          end
        end

        def shutdown_workers
          logger.info "Telling all workers to shut down"
          @prefork_pools.each do |name, prefox|
            prefox.stop
          end

          last_logged_time = Time.now
          loop do
            count = @prefork_pools.inject(0) { |sum, (name, prefork)| sum + prefork.live_worker_count }
            break if count == 0

            if Time.now - last_logged_time > 5
              logger.info "Still waiting for #{count} workers to quit..."
              last_logged_time = Time.now
            end

            sleep 0.25
          end
        end

    end

  end
end
