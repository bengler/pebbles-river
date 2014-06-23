require 'simplecov'
require 'rspec'
require 'rspec/mocks'

SimpleCov.add_filter 'spec'
SimpleCov.add_filter 'config'
SimpleCov.start

require_relative '../lib/pebbles/river'

module SpecHelpers
  def create_exception(exception_class)
    # TODO: Using #allocate here because Bunny has a whole bunch
    #   of exceptions; the one comparison against ECONNRESET is sad
    #   and could be generalized
    if exception_class == Errno::ECONNRESET
      raise exception_class.new
    else
      raise exception_class.allocate
    end
  end
end

RSpec.configure do |config|
  config.mock_with :rspec
  config.include SpecHelpers
end

