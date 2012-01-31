require File.expand_path('test_helper', File.dirname(__FILE__))

class DBOptionsTest < Test::Unit::TestCase
  def setup
    @path = File.expand_path(File.join('..', 'db_test.db'), __FILE__)
  end

  def test_paranoid_check_default
    db = LevelDB::DB.new(@path)
    assert_equal db.options.paranoid_checks, false
  end

  def test_paranoid_check_on
    db = LevelDB::DB.new(:path => @path, :paranoid_checks => true)
    assert_equal db.options.paranoid_checks, true
  end

  def test_paranoid_check_off
    db = LevelDB::DB.new(:path => @path, :paranoid_checks => false)
    assert_equal db.options.paranoid_checks, false
  end

  def test_write_buffer_size_default
    db = LevelDB::DB.new(:path => @path)
    assert_equal db.options.write_buffer_size, (4 * 1024 * 1024)
  end

  def test_write_buffer_size
    db = LevelDB::DB.new(:path => @path, :write_buffer_size => 10 * 1042)
    assert_equal db.options.write_buffer_size, (10 * 1042)
  end

  def test_max_open_files_default
    db = LevelDB::DB.new(:path => @path, :write_buffer_size => 10 * 1042)
    assert_equal db.options.max_open_files, 1000
  end
end
