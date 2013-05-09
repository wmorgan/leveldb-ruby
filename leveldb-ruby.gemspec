# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name        = "leveldb-ruby"
  s.version     = "0.15.1"
  s.platform    = Gem::Platform::RUBY
  s.licenses    = ["MIT"]
  s.summary     = "a Ruby binding to LevelDB"
  s.email       = "wmorgan-leveldb-ruby-gemspec@masanjin.net"
  s.homepage    = "http://github.com/wmorgan/leveldb-ruby"
  s.description = "LevelDB-Ruby is a Ruby binding to LevelDB."
  s.authors     = ["William Morgan"]
  s.extensions  = %w(ext/leveldb/extconf.rb)

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- test/*`.split("\n")
  s.require_paths = ["lib", "ext"]
  s.extra_rdoc_files = %w(README ext/leveldb/leveldb.cc)
  s.rdoc_options  = %w(-c utf8 --main README --title LevelDB)

  s.add_development_dependency("rake")
  s.add_development_dependency("rdoc")
end

