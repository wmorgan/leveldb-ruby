require 'rubygems'
require 'rubygems/package_task'
require 'find'

spec = Gem::Specification.new do |s|
 s.name = "leveldb-ruby"
 s.version = "0.15"
 s.date = Time.now
 s.email = "wmorgan-leveldb-ruby-gemspec@masanjin.net"
 s.authors = ["William Morgan"]
 s.summary = "a Ruby binding to LevelDB"
 s.homepage = "http://github.com/wmorgan/leveldb-ruby"
 s.files = %w(README LICENSE ext/leveldb/extconf.rb ext/leveldb/platform.rb lib/leveldb.rb ext/leveldb/leveldb.cc leveldb/Makefile leveldb/build_detect_platform)
 Find.find("leveldb") { |x| s.files += [x] if x =~ /\.(cc|h|c)$/}
 s.extensions = %w(ext/leveldb/extconf.rb)
 s.executables = []
 s.extra_rdoc_files = %w(README ext/leveldb/leveldb.cc)
 s.rdoc_options = %w(-c utf8 --main README --title LevelDB)
 s.description = "LevelDB-Ruby is a Ruby binding to LevelDB."
end

task :rdoc do |t|
  sh "rm -rf doc; rdoc #{spec.rdoc_options.join(' ')} #{spec.extra_rdoc_files.join(' ')} lib/leveldb.rb"
end

Gem::PackageTask.new spec do
end

require 'rake/testtask'
Rake::TestTask.new(:test) do |test|
  test.libs << 'ext' << 'test'
  test.pattern = 'test/**/*_test.rb'
  test.verbose = true
end

task :build do |t|
  sh "cd ext/leveldb && ruby extconf.rb && make"
end

task :clean do |t|
  sh "cd leveldb     && make clean"
  sh "cd ext/leveldb && make clean"
end

# vim: syntax=ruby
