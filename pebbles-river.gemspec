# coding: utf-8

lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

require 'pebbles/river/version'

Gem::Specification.new do |spec|
  spec.name          = "pebbles-river"
  spec.version       = Pebbles::River::VERSION
  spec.authors       = ["Alexander Staubo", "Simen Svale Skogsrud"]
  spec.email         = ["alex@bengler.no"]
  spec.summary       =
  spec.description   = %q{Implements an event river mechanism for Pebblebed.}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_runtime_dependency 'pebblebed', '>= 0.1.3'
  spec.add_runtime_dependency 'bunny', '~> 0.8.0'
  spec.add_runtime_dependency 'activesupport', '>= 3.0'
  spec.add_runtime_dependency 'servolux', '~> 0.10'
  spec.add_runtime_dependency 'mercenary', '~> 0.3.3'

  spec.add_development_dependency 'rspec'
  spec.add_development_dependency "bundler", "~> 1.5"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "simplecov"
end
