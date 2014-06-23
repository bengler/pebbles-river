module Pebbles
  module River

    # Overrides preforker to instantiate a round-robin set of modules. The
    # desired worker count is multiplied by the number of modules; for example,
    # if specifying modules [A, B] and the worker count is 2, then 4 actual
    # processes will be forked.
    class MultiPrefork < ::Servolux::Prefork

      def initialize(options, &block)
        raise ArgumentError, "Block invocation not supported" if block
        raise ArgumentError, "Must pass :modules, not :module" if options[:module]

        # Like Prefork we support passing procs
        @modules = options[:modules].map { |m|
          if m.is_a?(Proc)
            Module.new { define_method :execute, &m }
          else
            m
          end
        }
        @index = 0

        options = {}.merge(options)
        options[:module] = @modules.first
        options.delete(:modules)
        if (min_workers = options[:min_workers])
          options[:min_workers] = min_workers * @modules.length
        end
        if (max_workers = options[:max_workers])
          options[:max_workers] = max_workers * @modules.length
        end

        super(options)
      end

      # This cheats by overriding `Prefork#add_workers` and replacing the
      # `@module instance` variable.
      def add_workers(number = 1)
        number.times do
          @module = @modules[@index]
          super(1)
          @index = (@index + 1) % @modules.length
        end
      end
    end

  end
end