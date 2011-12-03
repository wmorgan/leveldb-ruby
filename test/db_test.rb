require 'test/unit'
require File.expand_path("../../lib/leveldb", __FILE__)
require 'fileutils'

class DBTest < Test::Unit::TestCase
  path = File.expand_path("../db_test.db", __FILE__)
  FileUtils.rm_rf path
  DB = LevelDB::DB.new(path)

  def test_get
    DB.put 'test:read', '1'

    assert_equal '1', DB.get('test:read')
    assert_equal '1', DB.get('test:read', :fill_cache => false)
    assert_equal '1', DB.get('test:read',
                             :fill_cache => false,
                             :verify_checksums => true)
  end

  def test_put
    DB.put "test:async", "1"
    DB.put "test:sync", "1", :sync => true

    assert_equal "1", DB.get("test:async")
    assert_equal "1", DB.get("test:sync")
  end

  def test_delete
    DB.put 'test:async', '1'
    DB.put 'test:sync', '1'

    assert DB.delete("test:async")
    assert DB.delete("test:sync", :sync => true)
  end
end

