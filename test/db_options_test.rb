require 'test/unit'
require File.expand_path(File.join('..', '..', 'lib', 'leveldb'), __FILE__)
require 'fileutils'

class DBOptionsDefaultTest < Test::Unit::TestCase
  def setup
    path = File.expand_path(File.join('..', 'db_test.db'), __FILE__)
    @db = LevelDB::DB.new(path)
  end

  def teardown
  end

  def test_paranoid_check
    assert_equal @db.options.paranoid_checks, true
  end
end
