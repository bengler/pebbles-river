require 'active_support/core_ext/hash/keys'
require 'active_support/core_ext/hash/slice'
require 'json'
require 'bunny'
require 'pebblebed/uid'
require 'servolux'

require_relative "river/version"
require_relative "river/errors"
require_relative "river/message"
require_relative "river/worker"
require_relative "river/subscription"
require_relative "river/supervisor"
require_relative "river/routing"
require_relative "river/river"
require_relative "river/compatibility"
require_relative "river/rate_limiter"
require_relative "river/daemon_helper"

module Pebbles::River

  CONNECTION_EXCEPTIONS = [
    Bunny::Exception,
    # These should be caught by Bunny, but apparently aren't.
    Errno::ECONNRESET
  ].freeze

  def self.rabbitmq_options
    @rabbitmq_options ||= {}.freeze
  end

  def self.rabbitmq_options=(options)
    @rabbitmq_options = options.freeze
  end

end
