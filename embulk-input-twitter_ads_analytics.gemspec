
Gem::Specification.new do |spec|
  spec.name          = "embulk-input-twitter_ads_analytics"
  spec.version       = "0.1.1"
  spec.authors       = ["naotaka nakane"]
  spec.summary       = "Twitter Ads Analytics input plugin for Embulk"
  spec.description   = "Loads records from Twitter Ads Analytics."
  spec.email         = ["n.nakane0219@gmail.com"]
  spec.licenses      = ["MIT"]
  # TODO set this: spec.homepage      = "https://github.com/n.nakane0219/embulk-input-twitter_ads_analytics"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r{^(test|spec)/})
  spec.require_paths = ["lib"]

  #spec.add_dependency 'YOUR_GEM_DEPENDENCY', ['~> YOUR_GEM_DEPENDENCY_VERSION']
  spec.add_dependency 'oauth', ['~> 0.5.4']
  spec.add_dependency 'activesupport', ['~> 5.2.3']

  # spec.add_development_dependency 'embulk', ['>= 0.9.17']
  spec.add_development_dependency 'bundler', ['>= 1.10.6']
  spec.add_development_dependency 'rake', ['>= 10.0']
end
