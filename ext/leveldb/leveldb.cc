#include <ruby.h>
#include <memory>

#include "leveldb/db.h"
#include "leveldb/slice.h"
#include "leveldb/cache.h"
#include "leveldb/write_batch.h"

using namespace std;

// support 1.9 and 1.8
#ifndef RSTRING_PTR
#define RSTRING_PTR(v) RSTRING(v)->ptr
#endif

// convert status errors into exceptions
#define RAISE_ON_ERROR(status) do { \
  if(!status.ok()) { \
    VALUE exc = rb_exc_new2(c_error, status.ToString().c_str()); \
    rb_exc_raise(exc); \
  }  \
} while(0)

#define RUBY_STRING_TO_SLICE(x) leveldb::Slice(RSTRING_PTR(x), RSTRING_LEN(x))
#define SLICE_TO_RUBY_STRING(x) rb_str_new(x.data(), x.size())
#define STRING_TO_RUBY_STRING(x) rb_str_new(x.data(), x.size())

namespace {
  VALUE c_batch;
  VALUE c_error;
  VALUE c_db_options;
  VALUE k_fill;
  VALUE k_verify;
  VALUE k_sync;
  VALUE k_path;
  ID to_s;
  leveldb::ReadOptions uncached_read_options;

  bool hash_val_test(VALUE h, VALUE key) {
    VALUE v = rb_hash_aref(h, key);
    return RTEST(v);
  }

  VALUE str2sym(const char* char_p) {
    return ID2SYM(rb_intern(char_p));
  }

  struct bound_db {
    leveldb::DB* db;

    bound_db()
      : db(0)
    {
    }

    ~bound_db() {
      clear_data();
    }

    void clear_data() {
      if(db) {
        delete db;
        db = 0;
      }
    }
  };

  void db_free(bound_db* db) {
    delete db;
  }

  struct bound_db_options {
    leveldb::Options* options;

    bound_db_options()
      : options(0)
    {
    }

    ~bound_db_options() {
      if(options) {
        if(options->block_cache) {
          delete options->block_cache;
          options->block_cache = 0;
        }

        delete options;
        options = 0;
      }
    }
  };

  void db_options_free(bound_db_options* options) {
    delete options;
  }

  void set_db_option(VALUE o_options, VALUE opts) {
    Check_Type(opts, T_HASH);

    bound_db_options* db_options;
    Data_Get_Struct(o_options, bound_db_options, db_options);
    leveldb::Options* options = db_options->options;

    if(hash_val_test(opts, str2sym("create_if_missing"))) {
      options->create_if_missing = true;
    }

    if(hash_val_test(opts, str2sym("error_if_exists"))) {
      options->error_if_exists = true;
    }

    VALUE v;

    v = rb_hash_aref(opts, str2sym("paranoid_checks"));
    if(!NIL_P(v)) {
      if(Qtrue == v) {
        options->paranoid_checks = true;
      } else {
        options->paranoid_checks = false;
      }
    }

    v = rb_hash_aref(opts, str2sym("write_buffer_size"));
    if(FIXNUM_P(v)) {
      options->write_buffer_size = NUM2UINT(v);
    }

    v = rb_hash_aref(opts, str2sym("max_open_files"));
    if(FIXNUM_P(v)) {
      options->max_open_files = NUM2INT(v);
    }

    v = rb_hash_aref(opts, str2sym("block_cache_size"));
    if(FIXNUM_P(v)) {
      options->block_cache = leveldb::NewLRUCache(NUM2INT(v));
      rb_iv_set(o_options, "@block_cache_size", v);
    }

    v = rb_hash_aref(opts, str2sym("block_size"));
    if(FIXNUM_P(v)) {
      options->block_size = NUM2UINT(v);
    }

    v = rb_hash_aref(opts, str2sym("block_restart_interval"));
    if(FIXNUM_P(v)) {
      options->block_restart_interval = NUM2INT(v);
    }

    v = rb_hash_aref(opts, str2sym("compression"));
    if(FIXNUM_P(v)) {
      switch(NUM2INT(v)) {
      case 0x0:
        options->compression = leveldb::kNoCompression;
        break;

      case 0x1:
        options->compression = leveldb::kSnappyCompression;
        break;
      }
    }
  }

  /*
   * call-seq:
   *   make(options)
   *
   * open level-db database
   * [options[ :path ]] path for level-db data
   *                     
   *                    This parameter is required.
   * [options[ :paranoid_checks ]] If true, the implementation will do aggressive checking of the
   *                               data it is processing and will stop early if it detects any
   *                               errors.  This may have unforeseen ramifications: for example, a
   *                               corruption of one DB entry may cause a large number of entries to
   *                               become unreadable or for the entire DB to become unopenable.
   *                               
   *                               Default: false
   * [options[ :write_buffer_size ]] Amount of data to build up in memory (backed by an unsorted log
   *                                 on disk) before converting to a sorted on-disk file.
   *                                 
   *                                 Larger values increase performance, especially during bulk
   *                                 loads.
   *                                 Up to two write buffers may be held in memory at the same time,
   *                                 so you may wish to adjust this parameter to control memory
   *                                 usage.
   *                                 Also, a larger write buffer will result in a longer recovery
   *                                 time the next time the database is opened.
   *                                 
   *                                 Default: 4MB
   * [options[ :max_open_files ]] Number of open files that can be used by the DB.  You may need to
   *                              increase this if your database has a large working set (budget
   *                              one open file per 2MB of working set).
   *                              
   *                              Default: 1000
   * [options[ :block_cache_size ]] Control over blocks (user data is stored in a set of blocks,
   *                                and a block is the unit of reading from disk).
   *                                
   *                                If non nil, use the specified cache size.
   *                                If nil, leveldb will automatically create and use an 8MB
   *                                internal cache.
   *                                
   *                                Default: nil
   * [options[ :block_size ]] Approximate size of user data packed per block.  Note that the
   *                          block size specified here corresponds to uncompressed data.  The
   *                          actual size of the unit read from disk may be smaller if
   *                          compression is enabled.  This parameter can be changed dynamically.
   *                          
   *                          Default: 4K
   * [options[ :block_restart_interval ]] Number of keys between restart points for delta
   *                                      encoding of keys.
   *                                      This parameter can be changed dynamically.
   *                                      Most clients should leave this parameter alone.
   *                                      
   *                                      Default: 16
   * [options[ :compression ]] LevelDB::CompressionType::SnappyCompression or
   *                           LevelDB::CompressionType::NoCompression.
   *                           
   *                           Compress blocks using the specified compression algorithm.
   *                           This parameter can be changed dynamically.
   *
   *                           Default: LevelDB::CompressionType::SnappyCompression,
   *                           which gives lightweight but fast compression.
   *                           
   *                           Typical speeds of SnappyCompression on an Intel(R) Core(TM)2 2.4GHz:
   *                               ~200-500MB/s compression
   *                               ~400-800MB/s decompression
   *                           Note that these speeds are significantly faster than most
   *                           persistent storage speeds, and therefore it is typically never
   *                           worth switching to NoCompression.  Even if the input data is
   *                           incompressible, the SnappyCompression implementation will
   *                           efficiently detect that and will switch to uncompressed mode.
   * [return] LevelDB::DB instance
   */
  VALUE db_make(VALUE klass, VALUE params) {
    Check_Type(params, T_HASH);
    VALUE path = rb_hash_aref(params, k_path);
    Check_Type(path, T_STRING);

    auto_ptr<bound_db> db(new bound_db);
    std::string pathname = std::string((char*)RSTRING_PTR(path));

    auto_ptr<bound_db_options> db_options(new bound_db_options);
    db_options->options = new leveldb::Options;
    leveldb::Options* options = db_options->options;
    VALUE o_options = Data_Wrap_Struct(c_db_options, NULL, db_options_free, db_options.release());
    set_db_option(o_options, params);

    leveldb::Status status = leveldb::DB::Open(*(options), pathname, &db->db);
    RAISE_ON_ERROR(status);

    VALUE o_db = Data_Wrap_Struct(klass, NULL, db_free, db.release());
    rb_iv_set(o_db, "@options", o_options);
    VALUE argv[1] = { path };
    rb_obj_call_init(o_db, 1, argv);

    return o_db;
  }

  VALUE db_close(VALUE self) {
    bound_db* db;
    Data_Get_Struct(self, bound_db, db);
    db->clear_data();
    return Qtrue;
  }

  leveldb::ReadOptions parse_read_options(VALUE options) {
    leveldb::ReadOptions readOptions;

    if(!NIL_P(options)) {
      Check_Type(options, T_HASH);
      if(rb_hash_aref(options, k_fill) == Qfalse)
        readOptions.fill_cache = false;
      if(rb_hash_aref(options, k_verify) == Qtrue)
        readOptions.verify_checksums = true;
    }

    return readOptions;
  }

  leveldb::WriteOptions parse_write_options(VALUE options) {
    leveldb::WriteOptions writeOptions;

    if(!NIL_P(options)) {
      Check_Type(options, T_HASH);
      if(rb_hash_aref(options, k_sync) == Qtrue)
        writeOptions.sync = true;
    }

    return writeOptions;
  }

  /*
   * call-seq:
   *   get(key, options = nil)
   *
   * get data from db
   *
   * [key]  key you want to get
   * [options[ :fill_cache ]] Should the data read for this iteration be cached in memory?
   *                          Callers may wish to set this field to false for bulk scans.
   *                          
   *                          true or false
   *                          
   *                          Default: true
   * [options[ :verify_checksums ]] If true, all data read from underlying storage will be
   *                                verified against corresponding checksums.
   *                                
   *                                Default: false
   * [return] value of stored db
   */
  VALUE db_get(int argc, VALUE* argv, VALUE self) {
    VALUE v_key, v_options;
    rb_scan_args(argc, argv, "11", &v_key, &v_options);
    Check_Type(v_key, T_STRING);
    leveldb::ReadOptions readOptions = parse_read_options(v_options);

    bound_db* db;
    Data_Get_Struct(self, bound_db, db);

    leveldb::Slice key = RUBY_STRING_TO_SLICE(v_key);
    std::string value;
    leveldb::Status status = db->db->Get(readOptions, key, &value);
    if(status.IsNotFound()) return Qnil;

    RAISE_ON_ERROR(status);
    return STRING_TO_RUBY_STRING(value);
  }

  VALUE db_delete(int argc, VALUE* argv, VALUE self) {
    VALUE v_key, v_options;
    rb_scan_args(argc, argv, "11", &v_key, &v_options);
    Check_Type(v_key, T_STRING);
    leveldb::WriteOptions writeOptions = parse_write_options(v_options);

    bound_db* db;
    Data_Get_Struct(self, bound_db, db);

    leveldb::Slice key = RUBY_STRING_TO_SLICE(v_key);
    std::string value;
    leveldb::Status status = db->db->Get(uncached_read_options, key, &value);

    if(status.IsNotFound()) return Qnil;

    status = db->db->Delete(writeOptions, key);
    RAISE_ON_ERROR(status);

    return STRING_TO_RUBY_STRING(value);
  }

  VALUE db_exists(VALUE self, VALUE v_key) {
    Check_Type(v_key, T_STRING);

    bound_db* db;
    Data_Get_Struct(self, bound_db, db);

    leveldb::Slice key = RUBY_STRING_TO_SLICE(v_key);
    std::string value;
    leveldb::Status status = db->db->Get(leveldb::ReadOptions(), key, &value);

    if(status.IsNotFound()) return Qfalse;
    return Qtrue;
  }

  /*
   * call-seq:
   *   put(key, value, options = nil)
   *
   * store data into DB
   *
   * [key] key you want to store
   * [value] data you want to store
   * [options[ :sync ]] If true, the write will be flushed from the operating system
   *                    buffer cache (by calling WritableFile::Sync()) before the write
   *                    is considered complete.  If this flag is true, writes will be
   *                    slower.
   *                    
   *                    If this flag is false, and the machine crashes, some recent
   *                    writes may be lost.  Note that if it is just the process that
   *                    crashes (i.e., the machine does not reboot), no writes will be
   *                    lost even if sync==false.
   *                    
   *                    In other words, a DB write with sync==false has similar
   *                    crash semantics as the "write()" system call.  A DB write
   *                    with sync==true has similar crash semantics to a "write()"
   *                    system call followed by "fsync()".
   *                    
   *                    Default: false
   * [return] stored value
   */
  VALUE db_put(int argc, VALUE* argv, VALUE self) {
    VALUE v_key, v_value, v_options;

    rb_scan_args(argc, argv, "21", &v_key, &v_value, &v_options);
    Check_Type(v_key, T_STRING);
    Check_Type(v_value, T_STRING);
    leveldb::WriteOptions writeOptions = parse_write_options(v_options);

    bound_db* db;
    Data_Get_Struct(self, bound_db, db);

    leveldb::Slice key = RUBY_STRING_TO_SLICE(v_key);
    leveldb::Slice value = RUBY_STRING_TO_SLICE(v_value);
    leveldb::Status status = db->db->Put(writeOptions, key, value);

    RAISE_ON_ERROR(status);

    return v_value;
  }

  /*
   * get db item count.
   *
   * [return] db item count
   */
  VALUE db_size(VALUE self) {
    long count = 0;

    bound_db* db;
    Data_Get_Struct(self, bound_db, db);
    leveldb::Iterator* it = db->db->NewIterator(uncached_read_options);

    // apparently this is how we have to do it. slow and painful!
    for (it->SeekToFirst(); it->Valid(); it->Next()) count++;
    RAISE_ON_ERROR(it->status());
    delete it;
    return INT2NUM(count);
  }

  VALUE db_iterate(VALUE self, VALUE key_from, VALUE key_to, bool reversed) {
    bound_db* db;
    Data_Get_Struct(self, bound_db, db);
    leveldb::Iterator* it = db->db->NewIterator(uncached_read_options);

    if(RTEST(key_from)) {
      it->Seek(RUBY_STRING_TO_SLICE(rb_funcall(key_from, to_s, 0)));
    } else {
      if(reversed) {
        it->SeekToLast();
      } else {
        it->SeekToFirst();
      }
    }

    bool passed_limit = false;
    bool check_limit = RTEST(key_to);
    std::string key_to_str;

    if(check_limit)
      key_to_str = RUBY_STRING_TO_SLICE(rb_funcall(key_to, to_s, 0)).ToString();

    while(!passed_limit && it->Valid()) {
      leveldb::Slice key_sl = it->key();

      if(check_limit &&
         (reversed ?
          (key_sl.ToString() < key_to_str) :
          (key_sl.ToString() > key_to_str))) {
        passed_limit = true;
      } else {
        VALUE key = SLICE_TO_RUBY_STRING(key_sl);
        VALUE value = SLICE_TO_RUBY_STRING(it->value());
        VALUE ary = rb_ary_new2(2);
        rb_ary_push(ary, key);
        rb_ary_push(ary, value);
        rb_yield(ary);
        reversed ? it->Prev() : it->Next();
      }
    }

    RAISE_ON_ERROR(it->status());
    delete it;

    return self;
  }

  VALUE db_each(int argc, VALUE* argv, VALUE self) {
    VALUE key_from, key_to;
    rb_scan_args(argc, argv, "02", &key_from, &key_to);

    return db_iterate(self, key_from, key_to, false);
  }

  VALUE db_reverse_each(int argc, VALUE* argv, VALUE self) {
    VALUE key_from, key_to;
    rb_scan_args(argc, argv, "02", &key_from, &key_to);

    return db_iterate(self, key_from, key_to, true);
  }

  VALUE db_init(VALUE self, VALUE v_pathname) {
    rb_iv_set(self, "@pathname", v_pathname);
    return self;
  }

  typedef struct bound_batch {
    leveldb::WriteBatch batch;
  } bound_batch;

  void batch_free(bound_batch* batch) {
    delete batch;
  }

  VALUE batch_make(VALUE klass) {
    bound_batch* batch = new bound_batch;
    batch->batch = leveldb::WriteBatch();

    VALUE o_batch = Data_Wrap_Struct(klass, NULL, batch_free, batch);
    VALUE argv[0];
    rb_obj_call_init(o_batch, 0, argv);

    return o_batch;
  }

  VALUE batch_put(VALUE self, VALUE v_key, VALUE v_value) {
    Check_Type(v_key, T_STRING);
    Check_Type(v_value, T_STRING);

    bound_batch* batch;
    Data_Get_Struct(self, bound_batch, batch);
    batch->batch.Put(RUBY_STRING_TO_SLICE(v_key), RUBY_STRING_TO_SLICE(v_value));

    return v_value;
  }

  VALUE batch_delete(VALUE self, VALUE v_key) {
    Check_Type(v_key, T_STRING);
    bound_batch* batch;
    Data_Get_Struct(self, bound_batch, batch);
    batch->batch.Delete(RUBY_STRING_TO_SLICE(v_key));
    return Qtrue;
  }

  VALUE db_batch(int argc, VALUE* argv, VALUE self) {
    VALUE o_batch = batch_make(c_batch);

    rb_yield(o_batch);

    bound_batch* batch;
    bound_db* db;
    Data_Get_Struct(o_batch, bound_batch, batch);
    Data_Get_Struct(self, bound_db, db);

    VALUE v_options;
    rb_scan_args(argc, argv, "01", &v_options);
    leveldb::WriteOptions writeOptions = parse_write_options(v_options);

    leveldb::Status status = db->db->Write(writeOptions, &batch->batch);
    RAISE_ON_ERROR(status);
    return Qtrue;
  }

  // -------------------------------------------------------
  // db_options methods
  VALUE db_options_paranoid_checks(VALUE self) {
    bound_db_options* db_options;
    Data_Get_Struct(self, bound_db_options, db_options);
    if(db_options->options->paranoid_checks) {
      return Qtrue;
    } else {
      return Qfalse;
    }
  }

  VALUE db_options_write_buffer_size(VALUE self) {
    bound_db_options* db_options;
    Data_Get_Struct(self, bound_db_options, db_options);
    return UINT2NUM(db_options->options->write_buffer_size);
  }

  VALUE db_options_max_open_files(VALUE self) {
    bound_db_options* db_options;
    Data_Get_Struct(self, bound_db_options, db_options);
    return INT2NUM(db_options->options->max_open_files);
  }

  VALUE db_options_block_size(VALUE self) {
    bound_db_options* db_options;
    Data_Get_Struct(self, bound_db_options, db_options);
    return UINT2NUM(db_options->options->block_size);
  }

  VALUE db_options_block_restart_interval(VALUE self) {
    bound_db_options* db_options;
    Data_Get_Struct(self, bound_db_options, db_options);
    return INT2NUM(db_options->options->block_restart_interval);
  }

  VALUE db_options_compression(VALUE self) {
    bound_db_options* db_options;
    Data_Get_Struct(self, bound_db_options, db_options);
    return INT2NUM(db_options->options->compression);
  }
}

extern "C" {
  void Init_leveldb() {
    k_fill = ID2SYM(rb_intern("fill_cache"));
    k_verify = ID2SYM(rb_intern("verify_checksums"));
    k_sync = ID2SYM(rb_intern("sync"));
    k_path = ID2SYM(rb_intern("path"));
    to_s = rb_intern("to_s");
    uncached_read_options = leveldb::ReadOptions();
    uncached_read_options.fill_cache = false;

    VALUE m_leveldb = rb_define_module("LevelDB");

    VALUE c_db = rb_define_class_under(m_leveldb, "DB", rb_cObject);
    rb_define_singleton_method(c_db, "make", RUBY_METHOD_FUNC(db_make), 1);
    rb_define_method(c_db, "initialize", RUBY_METHOD_FUNC(db_init), 1);
    rb_define_method(c_db, "get", RUBY_METHOD_FUNC(db_get), -1);
    rb_define_method(c_db, "delete", RUBY_METHOD_FUNC(db_delete), -1);
    rb_define_method(c_db, "put", RUBY_METHOD_FUNC(db_put), -1);
    rb_define_method(c_db, "exists?", RUBY_METHOD_FUNC(db_exists), 1);
    rb_define_method(c_db, "close", RUBY_METHOD_FUNC(db_close), 0);
    rb_define_method(c_db, "size", RUBY_METHOD_FUNC(db_size), 0);
    rb_define_method(c_db, "each", RUBY_METHOD_FUNC(db_each), -1);
    rb_define_method(c_db, "reverse_each", RUBY_METHOD_FUNC(db_reverse_each), -1);
    rb_define_method(c_db, "batch", RUBY_METHOD_FUNC(db_batch), -1);

    c_batch = rb_define_class_under(m_leveldb, "WriteBatch", rb_cObject);
    rb_define_singleton_method(c_batch, "make", RUBY_METHOD_FUNC(batch_make), 0);
    rb_define_method(c_batch, "put", RUBY_METHOD_FUNC(batch_put), 2);
    rb_define_method(c_batch, "delete", RUBY_METHOD_FUNC(batch_delete), 1);

    c_error = rb_define_class_under(m_leveldb, "Error", rb_eStandardError);
    c_db_options = rb_define_class_under(m_leveldb, "Options", rb_cObject);
    rb_define_method(c_db_options, "paranoid_checks",
                     RUBY_METHOD_FUNC(db_options_paranoid_checks), 0);
    rb_define_method(c_db_options, "write_buffer_size",
                     RUBY_METHOD_FUNC(db_options_write_buffer_size), 0);
    rb_define_method(c_db_options, "max_open_files",
                     RUBY_METHOD_FUNC(db_options_max_open_files), 0);
    rb_define_method(c_db_options, "block_size",
                     RUBY_METHOD_FUNC(db_options_block_size), 0);
    rb_define_method(c_db_options, "block_restart_interval",
                     RUBY_METHOD_FUNC(db_options_block_restart_interval), 0);
    rb_define_method(c_db_options, "compression",
                     RUBY_METHOD_FUNC(db_options_compression), 0);
  }
}
