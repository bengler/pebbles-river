require 'simplecov'
require 'rspec'
require 'rspec/mocks'

SimpleCov.add_filter 'spec'
SimpleCov.add_filter 'config'
SimpleCov.start

require_relative '../lib/pebbles/river'

RSpec.configure do |c|
  c.mock_with :rspec
end
