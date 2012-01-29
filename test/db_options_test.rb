require File.expand_path('test_helper', File.dirname(__FILE__))

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
