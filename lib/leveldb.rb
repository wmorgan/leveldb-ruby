require 'leveldb/leveldb' # the c extension

module LevelDB
class DB
  class << self

    ## Loads or creates a LevelDB database as necessary, stored on disk at
    ## +pathname+.
    def new pathname
      make pathname, true, false
    end

    ## Creates a new LevelDB database stored on disk at +pathname+. Throws an
    ## exception if the database already exists.
    def create pathname
      make pathname, true, true
    end

    ## Loads a LevelDB database stored on disk at +pathname+. Throws an
    ## exception unless the database already exists.
    def load pathname
      make pathname, false, false
    end
  end
end
end
