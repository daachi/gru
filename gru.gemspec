# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'gru/version'

Gem::Specification.new do |spec|
  spec.name          = "gru"
  spec.version       = Gru::VERSION
  spec.authors       = ["Jeffrey Gillis"]
  spec.email         = ["jeffrey.gillis1@gmail.com"]
  spec.summary       = %q{An atomic worker/minion manager.}
  spec.description   = %q{This is a worker/minion manager using different atomic data stores.}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_dependency "redis", "> 0.0"
  spec.add_development_dependency "bundler", "~> 1.7"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "rspec", "~> 3.1.0"
  spec.add_development_dependency "pry", "> 0.0"
  spec.add_development_dependency "awesome_print", "> 0.0"
end
