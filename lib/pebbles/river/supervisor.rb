module Pebbles
  module River

    class Supervisor < Servolux::Server

      def initialize(name, options = {})
        super(name, {interval: 5}.merge(options.slice(:logger, :pid_file)))

        options.assert_valid_keys(:logger, :pid_file, :worker_count, :worker)

        @worker_count = options[:worker_count] || 1
        @worker = options[:worker]

        worker = @worker
        @pool = Servolux::Prefork.new(min_workers: @worker_count) do
          LOGGER.info "spawn"
          begin
            $0 = "#{name}: worker"
            trap('TERM') { worker.stop }
            worker.run
          rescue Exception => e
            LOGGER.info e.to_s
          end
        end
      end

      def before_starting
        $0 = "#{name}: master"

        logger.info "Starting workers"
        @pool.start(1)
      end

      def after_stopping
        shutdown_workers
      end

      def usr2
        shutdown_workers
      end

      def run
        @pool.ensure_worker_pool_size
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
          @pool.stop
          loop do
            break if @pool.live_worker_count <= 0
            logger.info "Waiting for workers to quit"
            sleep 0.25
          end
        end

    end

  end
end
