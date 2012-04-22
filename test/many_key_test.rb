#!/usr/bin/env ruby

require 'test/unit'
require File.expand_path("../../lib/leveldb", __FILE__)
require 'fileutils'

class ManyKeyTest < Test::Unit::TestCase
  DB_PATH = "/tmp/manykey.db"

  def initialize(*a)
    super
    FileUtils.rm_rf DB_PATH
    @db = LevelDB::DB.new DB_PATH
  end

  def test_get
    100_000.times { |x| @db.put x.to_s, "abcdefghijklmnopqrstuvwxyz" }
    assert_equal 100_000, @db.size
  end
end
