require 'leveldb/leveldb' # the c extension

module LevelDB
class DB
  include Enumerable
  class << self
    ## Loads or creates a LevelDB database as necessary, stored on disk at
    ## +pathname+.
    ##
    ## See #make for possible options.
    def new pathname, options={}
      make path_string(pathname),
           options.merge(:create_if_missing => true,
                         :error_if_exists => false)
    end

    ## Creates a new LevelDB database stored on disk at +pathname+. Throws an
    ## exception if the database already exists.
    ##
    ## See #make for possible options.
    def create pathname, options={}
      make path_string(pathname),
           options.merge(:create_if_missing => true,
                         :error_if_exists => true)
    end

    ## Loads a LevelDB database stored on disk at +pathname+. Throws an
    ## exception unless the database already exists.
    def load pathname
      make path_string(pathname),
           { :create_if_missing => false, :error_if_exists => false }
    end

    private

    ## Coerces the argument into a String for use as a filename/-path
    def path_string pathname
      File.respond_to?(:path) ? File.path(pathname) : pathname.to_str
    end
  end

  attr_reader :pathname
  attr_reader :options

  alias :includes? :exists?
  alias :contains? :exists?
  alias :member? :exists?
  alias :[] :get
  alias :[]= :put
  alias :close! :close

  def each(*args, &block)
    i = iterator(*args)
    i.each(&block) if block
    i
  end

  def iterator(*args); Iterator.new self, *args end
  def keys; map { |k, v| k } end
  def values; map { |k, v| v } end

  def inspect
    %(<#{self.class} #{@pathname.inspect}>)
  end
end

class Iterator
  include Enumerable

  attr_reader :db, :from, :to

  def self.new(db, opts={})
    make db, opts
  end

  def reversed?; @reversed end
  def inspect
    %(<#{self.class} #{@db.inspect} @from=#{@from.inspect} @to=#{@to.inspect}#{' (reversed)' if @reversed}>)
  end
end

class WriteBatch
  class << self
    private :new
  end
end

class Options
  attr_reader :create_if_missing, :error_if_exists,
              :block_cache_size, :paranoid_checks,
              :write_buffer_size, :max_open_files,
              :block_size, :block_restart_interval,
              :compression
end

module CompressionType
  NoCompression     = 0x0
  SnappyCompression = 0x1
end
end # module LevelDB
