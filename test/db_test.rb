require 'test/unit'
require File.expand_path("../../lib/leveldb", __FILE__)
require 'fileutils'

class DBTest < Test::Unit::TestCase
  path = File.expand_path("../db_test.db", __FILE__)
  FileUtils.rm_rf path
  DB = LevelDB::DB.new(path)

  def test_put
    DB.put "test:async", "1"
    DB.put "test:sync", "1", :sync => true

    assert_equal "1", DB.get("test:async")
    assert_equal "1", DB.get("test:sync")
  end
end

