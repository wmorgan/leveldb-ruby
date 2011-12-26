require 'leveldb/leveldb' # the c extension

module LevelDB
  class DB
    include Enumerable
    class << self

      ## Loads or creates a LevelDB database as necessary, stored on disk at
      ## +pathname+.
      def new pathname
        make path_string(pathname), true, false
      end

      ## Creates a new LevelDB database stored on disk at +pathname+. Throws an
      ## exception if the database already exists.
      def create pathname
        make path_string(pathname), true, true
      end

      ## Loads a LevelDB database stored on disk at +pathname+. Throws an
      ## exception unless the database already exists.
      def load pathname
        make path_string(pathname), false, false
      end

      private

      ## Coerces the argument into a String for use as a filename/-path
      def path_string pathname
        File.respond_to?(:path) ? File.path(pathname) : pathname.to_str
      end
    end

    attr_reader :pathname

    alias :includes? :exists?
    alias :contains? :exists?
    alias :member? :exists?
    alias :[] :get
    alias :[]= :put

    def keys; map { |k, v| k } end
    def values; map { |k, v| v } end

    def inspect
      %(<#{self.class} #{@pathname.inspect}>)
    end
  end

  class Iterator
    attr_reader :db, :from, :to

    def self.new(db, options = nil)
      make(db, options || {})
    end

    def reversed?
      !!@reversed
    end

    def inspect
      %(<#{self.class} #{@db.inspect} @from=#{@from.inspect} @to=#{@to.inspect}#{' (reversed)' if @reversed}>)
    end
  end

  class WriteBatch
    class << self
      private :new
    end
  end
end
