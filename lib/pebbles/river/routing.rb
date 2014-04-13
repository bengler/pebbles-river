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

    end
  end
end