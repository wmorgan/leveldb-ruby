require 'leveldb/leveldb' # the c extension

module LevelDB
class DB
  include Enumerable
  class << self

    ## Loads or creates a LevelDB database as necessary, stored on disk at
    ## +pathname+.
    def new pathname
      make pathname.to_str, true, false
    end

    ## Creates a new LevelDB database stored on disk at +pathname+. Throws an
    ## exception if the database already exists.
    def create pathname
      make pathname.to_str, true, true
    end

    ## Loads a LevelDB database stored on disk at +pathname+. Throws an
    ## exception unless the database already exists.
    def load pathname
      make pathname.to_str, false, false
    end
  end

  alias :includes? :exists?
  alias :contains? :exists?
  alias :member? :exists?
  alias :[] :get
  alias :[]= :put

  def keys; map { |k, v| k } end
  def values; map { |k, v| v } end
end
end
