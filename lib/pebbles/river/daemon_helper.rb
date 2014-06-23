require 'logger'
require 'mercenary'

module Pebbles
  module River

    # Simple helper class for easily writing daemons that run multiple queue
    # workers. Handles command line parsing and daemonization.
    #
    # Adapter should support:
    #
    # * `name`: Name of the process. Optional, defaults to `$0`.
    #
    # * `configure_start_command(command)`. Implement to add more options to
    #   the start command, eg. configuration options. Optional.
    #
    # * `on_start(options)`. Implement to inject code before the program
    #   starts. Options are the start options, a hash. Optional.
    #
    # * `configure_supervisor(supervisor)`. This must call `add_listener` on the
    #   supervisor to add workers. Required.
    #
    class DaemonHelper

      def initialize(adapter, options = {})
        @adapter = adapter

        @name = @adapter.name if @adapter.respond_to?(:name)
        @name ||= File.basename($0).gsub(/\.rb$/, '')

        @logger = options.fetch(:logger, Logger.new($stderr))
      end

      def run
        Mercenary.program(@name) do |p|
          p.syntax "#{@name} <subcommand> [OPTION ...]"
          p.command(:start) do |c|
            c.syntax 'start'
            c.description 'Starts daemon'
            c.option :daemonize, '-d', '--daemon', 'To daemonize; otherwise will run synchronously.'
            c.option :pidfile, '-p', '--pidfile PIDFILE', 'Path to pid file.'
            c.option :workers, Integer, '-w', '--workers N', 'Set number of workers per queue (defaults to 1).'
            c.action do |_, options|
              start(options)
            end
            if @adapter.respond_to?(:configure_start_command)
              @adapter.configure_start_command(c)
            end
          end
          p.command(:stop) do |c|
            c.syntax 'stop'
            c.description 'Stops daemon'
            c.option :pidfile, '-p', '--pidfile PIDFILE', 'Path to pid file.'
            c.action do |_, options|
              stop(options)
            end
          end
          p.command(:status) do |c|
            c.syntax 'status'
            c.description 'Prints daemon status'
            c.option :pidfile, '-p', '--pidfile PIDFILE', 'Path to pid file.'
            c.action do |_, options|
              status(options)
            end
          end
        end

        if ARGV.any?
          abort "Unknown command '#{ARGV.first}'."
        else
          abort "Run with -h for help."
        end
      end

      private

        def start(options)
          if @adapter.respond_to?(:on_start)
            @adapter.on_start(options)
          end

          daemon = new_daemon(options)
          if daemon.alive?
            abort "#{daemon.name} is already running."
          end
          if options[:daemonize]
            print "Starting #{daemon.name}"
            daemon.startup
            puts ", done."
          else
            daemon.server.startup
          end
          exit
        end

        def stop(options)
          daemon = get_daemon(options)
          unless daemon.alive?
            abort "#{daemon.name} is not running."
          end
          print "Stopping #{daemon.name}"
          daemon.shutdown
          puts ", done."
          exit
        end

        def status(options)
          daemon = get_daemon(options)
          if daemon.alive?
            puts "#{daemon.name} is running."
          else
            puts "#{daemon.name} is not running."
          end
          exit(daemon.alive? ? 0 : 1)
        end

        def get_daemon(options)
          unless options[:pidfile]
            abort "Specify pidfile with --pidfile."
          end

          Servolux::Daemon.new(
            name: @name,
            pid_file: options[:pidfile],
            logger: @logger,
            startup_command: '/bin/true',
            nochdir: true)
        end

        def new_daemon(options)
          unless options[:pidfile]
            abort "Specify pidfile with --pidfile."
          end

          supervisor = Pebbles::River::Supervisor.new(@name,
            pid_file: options[:pidfile],
            logger: @logger,
            worker_count: options[:workers])

          @adapter.configure_supervisor(supervisor)

          supervisor.start_workers

          Servolux::Daemon.new(
            server: supervisor,
            nochdir: true)
        end

    end

  end
end