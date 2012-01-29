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
end
