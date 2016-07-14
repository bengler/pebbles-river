module Pebbles
  module River
    module Routing

      def self.routing_key_for(options)
        options.assert_valid_keys(:uid, :event)
        raise ArgumentError.new(':event is required') unless options[:event]
        raise ArgumentError.new(':uid is required') unless options[:uid]

        uid = Pebblebed::Uid.new(options[:uid])
        key = [options[:event], uid.klass, uid.path].compact
        key.join('._.')
      end

      def self.binding_routing_keys_for(options)
        options.assert_valid_keys(:path, :class, :event)
        keys = []
        if options[:event] or options[:path] or options[:class]
          element_to_routing_key_parts(options[:event]).each do |event|
            element_to_routing_key_parts(options[:class]).each do |klass|
              element_to_routing_key_parts(options[:path]).each do |pathspec|
                path_to_routing_key_parts(pathspec).each do |path|
                  keys << [event, klass, path].join('._.')
                end
              end
            end
          end
        end
        keys
      end

      private

        def self.path_to_routing_key_parts(s)
          required, optional = s.split('^').map { |p| p.split('.') }
          required = Array(required.join('.'))
          optional ||= []
          (0..optional.length).map {|i| required + optional[0,i]}.map {|p| p.join('.')}
        end

        def self.element_to_routing_key_parts(s)
          s ||= '#'
          if s.respond_to?(:to_a)
            s = s.join('|')
          end
          s.gsub('**', '#').split('|')
        end

    end
  end
end
