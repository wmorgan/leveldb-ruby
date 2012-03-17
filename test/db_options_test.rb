require 'test/unit'
require File.expand_path("../../lib/leveldb", __FILE__)
require 'fileutils'

class DBOptionsTest < Test::Unit::TestCase
  def setup
    @path = File.expand_path(File.join('..', 'db_test.db'), __FILE__)
  end

  def test_create_if_missing_default
    db = LevelDB::DB.make(@path, {})
    assert_equal db.options.create_if_missing, false
  end

  def test_create_if_missing
    db = LevelDB::DB.make(@path, :create_if_missing => true)
    assert_equal db.options.create_if_missing, true
  end

  def test_error_if_exists_default
    db = LevelDB::DB.make(@path, {})
    assert_equal db.options.error_if_exists, false
  end

  def test_error_if_exists
    FileUtils.rm_rf @path
    db = LevelDB::DB.make(@path, :error_if_exists => true, :create_if_missing => true)
    assert_equal db.options.error_if_exists, true
  end

  def test_paranoid_check_default
    db = LevelDB::DB.new(@path)
    assert_equal db.options.paranoid_checks, false
  end

  def test_paranoid_check_on
    db = LevelDB::DB.new(@path, :paranoid_checks => true)
    assert_equal db.options.paranoid_checks, true
  end

  def test_paranoid_check_off
    db = LevelDB::DB.new(@path, :paranoid_checks => false)
    assert_equal db.options.paranoid_checks, false
  end

  def test_write_buffer_size_default
    db = LevelDB::DB.new(@path)
    assert_equal db.options.write_buffer_size, (4 * 1024 * 1024)
  end

  def test_write_buffer_size
    db = LevelDB::DB.new(@path, :write_buffer_size => 10 * 1042)
    assert_equal db.options.write_buffer_size, (10 * 1042)
  end

  def test_max_open_files_default
    db = LevelDB::DB.new(@path)
    assert_equal db.options.max_open_files, 1000
  end

  def test_max_open_files
    db = LevelDB::DB.new(@path, :max_open_files => 2000)
    assert_equal db.options.max_open_files, 2000
  end

  def test_cache_size_default
    db = LevelDB::DB.new(@path)
    assert_equal db.options.block_cache_size, nil
  end

  def test_cache_size
    db = LevelDB::DB.new(@path, :block_cache_size => 10 * 1024 * 1024)
    assert_equal db.options.block_cache_size, (10 * 1024 * 1024)
  end

  def test_block_size_default
    db = LevelDB::DB.new(@path)
    assert_equal db.options.block_size, (4 * 1024)
  end

  def test_block_size
    db = LevelDB::DB.new(@path, :block_size => (2 * 1024))
    assert_equal db.options.block_size, (2 * 1024)
  end

  def test_block_restart_interval_default
    db = LevelDB::DB.new(@path)
    assert_equal db.options.block_restart_interval, 16
  end

  def test_block_restart_interval
    db = LevelDB::DB.new(@path, {:block_restart_interval => 32})
    assert_equal db.options.block_restart_interval, 32
  end

  def test_compression_default
    db = LevelDB::DB.new(@path)
    assert_equal db.options.compression, LevelDB::CompressionType::SnappyCompression
  end

  def test_compression
    db = LevelDB::DB.new(@path, :compression => LevelDB::CompressionType::NoCompression)
    assert_equal db.options.compression, LevelDB::CompressionType::NoCompression
  end
end
