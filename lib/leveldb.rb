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
        make(options)
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
        make(options)
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
        make(options)
      end

      private

      ## Coerces the argument into a String for use as a filename/-path
      def path_string pathname
        File.respond_to?(:path) ? File.path(pathname) : pathname.to_str
      end
    end

    attr_reader :options

    # get data from db
    # key:: key you want to get
    # options[:fill_cache] Should the data read for this iteration be cached in memory?
    #                      Callers may wish to set this field to false for bulk scans.
    #                      true or false
    #                      Default: true
    # options[:verify_checksums] If true, all data read from underlying storage will be
    #                            verified against corresponding checksums.
    #                            Default: false
    # return:: value of stored db

    # put
    # options[:sync]:: If true, the write will be flushed from the operating system
    #                  buffer cache (by calling WritableFile::Sync()) before the write
    #                  is considered complete.  If this flag is true, writes will be
    #                  slower.
    #
    #                  If this flag is false, and the machine crashes, some recent
    #                  writes may be lost.  Note that if it is just the process that
    #                  crashes (i.e., the machine does not reboot), no writes will be
    #                  lost even if sync==false.
    #
    #                  In other words, a DB write with sync==false has similar
    #                  crash semantics as the "write()" system call.  A DB write
    #                  with sync==true has similar crash semantics to a "write()"
    #                  system call followed by "fsync()".
    #
    #                  Default: false
    # returns:: stored value

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
