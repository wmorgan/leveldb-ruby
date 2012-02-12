require 'leveldb/leveldb' # the c extension

module LevelDB
  class DB
    include Enumerable
    class << self

      ## Loads or creates a LevelDB database as necessary, stored on disk at
      ## +pathname+.
      # see more detail make methods.
      def new opt
        options = {}
        if opt.is_a?(String)
          options[:path] = opt
        else
          options.merge!(opt)
        end
        options[:create_if_missing] ||= true
        options[:error_if_exists] ||= false
        make(options)
      end

      ## Creates a new LevelDB database stored on disk at +pathname+. Throws an
      ## exception if the database already exists.
      # see more detail make methods.
      def create pathname
        options = {}
        if opt.is_a?(String)
          options[:path] = opt
        else
          options.merge!(opt)
        end
        options[:create_if_missing] ||= true
        options[:error_if_exists] ||= true
        make(options)
      end

      ## Loads a LevelDB database stored on disk at +pathname+. Throws an
      ## exception unless the database already exists.
      # see more detail make methods.
      def load pathname
        options = {}
        if opt.is_a?(String)
          options[:path] = opt
        else
          options.merge!(opt)
        end
        options[:create_if_missing] ||= false
        options[:error_if_exists] ||= false
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
    attr_reader :block_cache_size
  end

  module CompressionType
    NoCompression     = 0x0
    SnappyCompression = 0x1
  end
end
