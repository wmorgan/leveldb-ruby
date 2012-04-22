require 'test/unit'
require File.expand_path("../../lib/leveldb", __FILE__)
require 'fileutils'

class DBTest < Test::Unit::TestCase
  DB_PATH = "/tmp/iteration.db"

  def initialize(*a)
    super
    FileUtils.rm_rf DB_PATH
    @db = LevelDB::DB.new DB_PATH
  end

  def test_get
    @db.put 'test:read', '1'

    assert_equal '1', @db.get('test:read')
    assert_equal '1', @db.get('test:read', :fill_cache => false)
    assert_equal '1', @db.get('test:read',
                             :fill_cache => false,
                             :verify_checksums => true)
  end

  def test_put
    @db.put "test:async", "1"
    @db.put "test:sync", "1", :sync => true

    assert_equal "1", @db.get("test:async")
    assert_equal "1", @db.get("test:sync")
  end

  def test_delete
    @db.put 'test:async', '1'
    @db.put 'test:sync', '1'

    assert @db.delete("test:async")
    assert @db.delete("test:sync", :sync => true)
  end

  def test_batch
    @db.put 'a', '1'
    @db.put 'b', '1'

    @db.batch do |b|
      b.put 'a', 'batch'
      b.delete 'b'
    end

    assert_equal 'batch', @db.get('a')
    assert_nil @db.get('b')

    @db.batch :sync => true do |b|
      b.put 'b', 'batch'
      b.delete 'a'
    end

    assert_equal 'batch', @db.get('b')
    assert_nil @db.get('a')
  end
end

