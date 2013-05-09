require 'rubygems'
require 'rubygems/package_task'
require 'rake/testtask'

desc "Build the docs"
task :rdoc do |t|
  sh "rm -rf doc; rdoc #{spec.rdoc_options.join(' ')} #{spec.extra_rdoc_files.join(' ')} lib/leveldb.rb"
end

spec = eval File.read(File.expand_path("../leveldb-ruby.gemspec", __FILE__))
Gem::PackageTask.new spec do
end

Rake::TestTask.new(:test => :build) do |test|
  test.libs << 'ext' << 'test'
  test.pattern = 'test/**/*_test.rb'
  test.verbose = true
end

desc "Build the extension"
task :build do |t|
  sh "cd ext/leveldb && ruby extconf.rb && make"
end

desc "Default task"
task :default => :test

# vim: syntax=ruby
