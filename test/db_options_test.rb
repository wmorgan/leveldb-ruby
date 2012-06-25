require 'test/unit'
require File.expand_path("../../lib/leveldb", __FILE__)
require 'fileutils'

class DBOptionsTest < Test::Unit::TestCase
  def setup
    @path = File.expand_path(File.join('..', 'db_test.db'), __FILE__)
  end

  def teardown
    FileUtils.rm_rf @path
  end

  def assert_false x; assert !x end

  def test_create_if_missing_behavior
    assert_raises(LevelDB::Error) { LevelDB::DB.make(@path, {}) } # create if missing is false
    db = LevelDB::DB.make @path, :create_if_missing => true
    assert db.options.create_if_missing
    db.close!
    db2 = LevelDB::DB.make @path, {} # should work the second time
    assert_false db2.options.create_if_missing
    db2.close!

    FileUtils.rm_rf @path
    assert_nothing_raised { LevelDB::DB.new @path } # by default should set create_if_missing to true
  end

  def test_error_if_exists_behavior
    db = LevelDB::DB.make @path, :create_if_missing => true
    assert_false db.options.error_if_exists
    db.close!

    assert_raises(LevelDB::Error) { LevelDB::DB.make @path, :create_if_missing => true, :error_if_exists => true }
  end

  def test_paranoid_check_default
    db = LevelDB::DB.new @path
    assert_false db.options.paranoid_checks
  end

  def test_paranoid_check_on
    db = LevelDB::DB.new @path, :paranoid_checks => true
    assert db.options.paranoid_checks
  end

  def test_paranoid_check_off
    db = LevelDB::DB.new @path, :paranoid_checks => false
    assert_false db.options.paranoid_checks
  end

  def test_write_buffer_size_default
    db = LevelDB::DB.new @path
    assert_equal LevelDB::Options::DEFAULT_WRITE_BUFFER_SIZE, db.options.write_buffer_size
  end

  def test_write_buffer_size
    db = LevelDB::DB.new @path, :write_buffer_size => 10 * 1042
    assert_equal (10 * 1042), db.options.write_buffer_size
  end

  def test_write_buffer_size_invalid
    assert_raises(TypeError) { LevelDB::DB.new @path, :write_buffer_size => "1234" }
  end

  def test_max_open_files_default
    db = LevelDB::DB.new @path
    assert_equal LevelDB::Options::DEFAULT_MAX_OPEN_FILES, db.options.max_open_files
  end

  def test_max_open_files
    db = LevelDB::DB.new(@path, :max_open_files => 2000)
    assert_equal db.options.max_open_files, 2000
  end

  def test_max_open_files_invalid
    assert_raises(TypeError) { LevelDB::DB.new @path, :max_open_files => "2000" }
  end

  def test_cache_size_default
    db = LevelDB::DB.new @path
    assert_nil db.options.block_cache_size
  end

  def test_cache_size
    db = LevelDB::DB.new @path, :block_cache_size => 10 * 1024 * 1024
    assert_equal (10 * 1024 * 1024), db.options.block_cache_size
  end

  def test_cache_size_invalid
    assert_raises(TypeError) { LevelDB::DB.new @path, :block_cache_size => false }
  end

  def test_block_size_default
    db = LevelDB::DB.new @path
    assert_equal LevelDB::Options::DEFAULT_BLOCK_SIZE, db.options.block_size
  end

  def test_block_size
    db = LevelDB::DB.new @path, :block_size => (2 * 1024)
    assert_equal (2 * 1024), db.options.block_size
  end

  def test_block_size_invalid
    assert_raises(TypeError) { LevelDB::DB.new @path, :block_size => true }
  end

  def test_block_restart_interval_default
    db = LevelDB::DB.new @path
    assert_equal LevelDB::Options::DEFAULT_BLOCK_RESTART_INTERVAL, db.options.block_restart_interval
  end

  def test_block_restart_interval
    db = LevelDB::DB.new @path, :block_restart_interval => 32
    assert_equal db.options.block_restart_interval, 32
  end

  def test_block_restart_interval_invalid
    assert_raises(TypeError) { LevelDB::DB.new @path, :block_restart_interval => "abc" }
  end

  def test_compression_default
    db = LevelDB::DB.new @path
    assert_equal db.options.compression, LevelDB::CompressionType::SnappyCompression
  end

  def test_compression
    db = LevelDB::DB.new @path, :compression => LevelDB::CompressionType::NoCompression
    assert_equal db.options.compression, LevelDB::CompressionType::NoCompression
  end

  def test_compression_invalid_type
    assert_raises(TypeError) { LevelDB::DB.new @path, :compression => "1234" }
    assert_raises(TypeError) { LevelDB::DB.new @path, :compression => 999 }
  end
end
