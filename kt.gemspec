# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'kt/version'

Gem::Specification.new do |spec|
  spec.name          = "kt"
  spec.version       = KT::VERSION
  spec.authors       = ["Teodor Pripoae"]
  spec.email         = ["toni@kuende.com"]
  spec.description   = %q{Kyoto Tycoon client}
  spec.summary       = %q{Kyoto Tycoon client}
  spec.homepage      = "https://github.com/kuende/kt-ruby"
  spec.license       = "Apache 2.0"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = []
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_dependency "excon", "~> 0.51.0"
  spec.add_dependency "connection_pool", "~> 2.2.0"

  # testing
  spec.add_development_dependency "bundler", ">= 1.3"
  spec.add_development_dependency "rspec", "~> 3.4.0"
  spec.add_development_dependency "pry", "~> 0.10.4"
end
