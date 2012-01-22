require 'leveldb/leveldb' # the c extension

module LevelDB
class DB
  include Enumerable
  class << self

    ## Loads or creates a LevelDB database as necessary, stored on disk at
    ## +pathname+.
    def new pathname
      make({ :path => path_string(pathname) ,
             :create_if_missing => true,
             :error_if_exists => false})
    end

    ## Creates a new LevelDB database stored on disk at +pathname+. Throws an
    ## exception if the database already exists.
    def create pathname
      make({
             :path => path_string(pathname),
             :create_if_missing => true,
             :error_if_exists => true})
    end

    ## Loads a LevelDB database stored on disk at +pathname+. Throws an
    ## exception unless the database already exists.
    def load pathname
      make({
             :path => path_string(pathname),
             :create_if_missing => false,
             :error_if_exists => false})
    end

    private

    ## Coerces the argument into a String for use as a filename/-path
    def path_string pathname
      File.respond_to?(:path) ? File.path(pathname) : pathname.to_str
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

class WriteBatch
  class << self
    private :new
  end
end
end
