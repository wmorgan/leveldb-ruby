require 'test/unit'
require File.expand_path("../../lib/leveldb", __FILE__)

class IterationTest < Test::Unit::TestCase
  DB = LevelDB::DB.new(File.expand_path("../iteration.db", __FILE__))
  DB.put "a/1", "1"
  DB.put "b/1", "1"
  DB.put "b/2", "1"
  DB.put "b/3", "1"
  DB.put "c/1", "1"

  def test_each
    expected = %w(a/1 b/1 b/2 b/3 c/1)
    keys = []
    DB.each do |key, value|
      keys << key
    end

    assert_equal expected, keys
  end

  def test_each_with_key_from
    expected = %w(b/1 b/2 b/3 c/1)
    keys = []
    DB.each('b') do |key, value|
      keys << key
    end

    assert_equal expected, keys
  end

  def test_each_with_key_from_to
    expected = %w(b/1 b/2 b/3)
    keys = []
    DB.each('b', 'b/4') do |key, value|
      keys << key
    end

    assert_equal expected, keys
  end

  def test_reverse_each
    expected = %w(c/1 b/3 b/2 b/1 a/1)
    keys = []
    DB.reverse_each do |key, value|
      keys << key
    end

    assert_equal expected, keys
  end

  def test_reverse_each_with_key_from
    expected = %w(b/1 a/1)
    keys = []
    DB.reverse_each('b') do |key, value|
      keys << key
    end

    assert_equal expected, keys
  end

  def test_reverse_each_with_key_from_to
    expected = %w(c/1 b/3 b/2 b/1)
    keys = []
    DB.reverse_each('c', 'b') do |key, value|
      keys << key
    end

    assert_equal expected, keys
  end
end

