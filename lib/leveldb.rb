require 'leveldb/leveldb' # the c extension

module LevelDB
  class DB
    include Enumerable
    class << self

      ## Loads or creates a LevelDB database as necessary, stored on disk at
      ## +pathname+.
      # see more detail open methods.
      def new opt
        options = {}
        if opt.is_a?(String)
          options[:path] = opt
        else
          options.merge!(opt)
        end
        options[:create_if_missing] ||= true
        options[:error_if_exists] ||= false
        open(options)
      end

      ## Creates a new LevelDB database stored on disk at +pathname+. Throws an
      ## exception if the database already exists.
      # see more detail open methods.
      def create pathname
        options = {}
        if opt.is_a?(String)
          options[:path] = opt
        else
          options.merge!(opt)
        end
        options[:create_if_missing] ||= true
        options[:error_if_exists] ||= true
        open(options)
      end

      ## Loads a LevelDB database stored on disk at +pathname+. Throws an
      ## exception unless the database already exists.
      # see more detail open methods.
      def load pathname
        options = {}
        if opt.is_a?(String)
          options[:path] = opt
        else
          options.merge!(opt)
        end
        options[:create_if_missing] ||= false
        options[:error_if_exists] ||= false
        open(options)
      end

      # open level-db database
      # options[:path]:: path for level-db data
      # options[:paranoid_checks]:: true/false. If this value is true, db use paranoid_checks
      # return:: LevelDB::DB instance
      def open(options)
        make(options)
      end

      private

      ## Coerces the argument into a String for use as a filename/-path
      def path_string pathname
        File.respond_to?(:path) ? File.path(pathname) : pathname.to_str
      end
    end

    attr_reader :options

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

  class Options
  end
end
