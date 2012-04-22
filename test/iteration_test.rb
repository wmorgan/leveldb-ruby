require 'test/unit'
require File.expand_path("../../lib/leveldb", __FILE__)
require 'fileutils'

class IterationTest < Test::Unit::TestCase
  DB_PATH = "/tmp/iteration.db"

  def initialize(*a)
    super
    FileUtils.rm_rf DB_PATH
    @db = LevelDB::DB.new DB_PATH
    @db.put "a/1", "1"
    @db.put "b/1", "2"
    @db.put "b/2", "3"
    @db.put "b/3", "4"
    @db.put "c/1", "5"
  end

  def test_each
    expected = %w(a/1 b/1 b/2 b/3 c/1)
    keys = []
    @db.each do |key, value|
      keys << key
    end

    assert_equal expected, keys
  end

  def test_each_with_key_from
    expected = %w(b/1 b/2 b/3 c/1)
    keys = []
    @db.each(:from => 'b') do |key, value|
      keys << key
    end

    assert_equal expected, keys
  end

  def test_each_with_key_from_to
    expected = %w(b/1 b/2 b/3)
    keys = []
    @db.each(:from => 'b', :to => 'b/4') do |key, value|
      keys << key
    end

    assert_equal expected, keys
  end

  def test_reverse_each
    expected = %w(c/1 b/3 b/2 b/1 a/1)
    keys = []
    @db.each(:reversed => true) do |key, value|
      keys << key
    end

    assert_equal expected, keys
  end

  def test_reverse_each_with_key_from
    expected = %w(b/1 a/1)
    keys = []
    @db.each(:from => 'b', :reversed => true) do |key, value|
      keys << key
    end

    assert_equal expected, keys
  end

  def test_reverse_each_with_key_from_to
    expected = %w(c/1 b/3 b/2 b/1)
    keys = []
    @db.each(:from => 'c', :to => 'b', :reversed => true) do |key, value|
      keys << key
    end

    assert_equal expected, keys
  end

  def test_iterator_reverse_each_with_key_from_to
    expected_keys = %w(c/1 b/3 b/2 b/1)
    expected_values = %w(5 4 3 2)
    keys = []
    values = []

    iter = LevelDB::Iterator.new @db, :from => 'c', :to => 'b', :reversed => true
    iter.each do |key, value|
      keys << key
      values << value
    end

    assert_equal expected_keys, keys
    assert_equal expected_values, values
  end

  def test_iterator_each
    expected_keys = %w(a/1 b/1 b/2 b/3 c/1)
    expected_values = %w(1 2 3 4 5)
    keys = []
    values = []
    iter = LevelDB::Iterator.new @db
    iter.each do |key, value|
      keys << key
      values << value
    end

    assert_equal expected_keys, keys
    assert_equal expected_values, values
  end

  def test_iterator_peek
    iter = LevelDB::Iterator.new @db
    assert_equal %w(a/1 1), iter.peek, iter.invalid_reason
    assert_equal %w(a/1 1), iter.peek, iter.invalid_reason
    assert_nil iter.scan
    assert_equal %w(b/1 2), iter.peek, iter.invalid_reason
  end

  def test_iterator_init_with_default_options
    iter = LevelDB::Iterator.new @db
    assert_equal @db, iter.db
    assert_nil iter.from
    assert_nil iter.to
    assert !iter.reversed?
  end

  def test_iterator_init_with_options
    iter = LevelDB::Iterator.new @db, :from => 'abc', :to => 'def', :reversed => true
    assert_equal @db,iter.db
    assert_equal 'abc', iter.from
    assert_equal 'def', iter.to
    assert iter.reversed?
  end

  def test_iterator_init_with_no_args
    assert_raise ArgumentError do
      LevelDB::Iterator.new
    end
  end

  def test_iterator_requires_db
    assert_raise ArgumentError do
      LevelDB::Iterator.new 'db'
    end
  end
end

