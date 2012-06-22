#include <ruby.h>
#include <memory>

#include "leveldb/db.h"
#include "leveldb/cache.h"
#include "leveldb/slice.h"
#include "leveldb/write_batch.h"

using namespace std;

static VALUE m_leveldb;
static VALUE c_db;
static VALUE c_iter;
static VALUE c_batch;
static VALUE c_error;
static VALUE k_fill;
static VALUE k_verify;
static VALUE k_sync;
static VALUE k_from;
static VALUE k_to;
static VALUE k_reversed;
static VALUE k_class;
static VALUE k_name;
static ID k_to_s;
static leveldb::ReadOptions uncached_read_options;

static VALUE c_db_options;
static VALUE k_create_if_missing;
static VALUE k_error_if_exists;
static VALUE k_paranoid_checks;
static VALUE k_write_buffer_size;
static VALUE k_block_cache_size;
static VALUE k_block_size;
static VALUE k_block_restart_interval;
static VALUE k_compression;
static VALUE k_max_open_files;

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

typedef struct bound_db {
  leveldb::DB* db;
} bound_db;

static void db_free(bound_db* db) {
  if(db->db != NULL) {
    delete db->db;
    db->db = NULL;
  }
  delete db;
}

static void set_val(VALUE opts, VALUE key, VALUE db_options, bool* pOptionVal) {
  VALUE v = rb_hash_aref(opts, key);
  VALUE set_v;
  if(NIL_P(v) || v == Qfalse) {
    *pOptionVal = false;
    set_v = Qfalse;
  } else if(v == Qtrue){
    *pOptionVal = true;
    set_v = Qtrue;
  } else {
    rb_raise(rb_eTypeError, "invalid type for %s", rb_id2name(SYM2ID(key)));
  }

  string param("@");
  param += rb_id2name(SYM2ID(key));
  rb_iv_set(db_options, param.c_str(), set_v);
}

static void set_val(VALUE opts, VALUE key, VALUE db_options, size_t* pOptionVal) {
  VALUE v = rb_hash_aref(opts, key);
  VALUE set_v;
  if(NIL_P(v)) {
    set_v = UINT2NUM(*pOptionVal);
  } else if(FIXNUM_P(v)) {
    *pOptionVal = NUM2UINT(v);
    set_v = v;
  } else {
    rb_raise(rb_eTypeError, "invalid type for %s", rb_id2name(SYM2ID(key)));
  }

  string param("@");
  param += rb_id2name(SYM2ID(key));
  rb_iv_set(db_options, param.c_str(), set_v);
}

static void set_val(VALUE opts, VALUE key, VALUE db_options, int* pOptionVal) {
  VALUE v = rb_hash_aref(opts, key);
  VALUE set_v;
  if(NIL_P(v)) {
    set_v = INT2NUM(*pOptionVal);
  } else if(FIXNUM_P(v)) {
    *pOptionVal = NUM2INT(v);
    set_v = v;
  } else {
    rb_raise(rb_eTypeError, "invalid type for %s", rb_id2name(SYM2ID(key)));
  }

  string param("@");
  param += rb_id2name(SYM2ID(key));
  rb_iv_set(db_options, param.c_str(), set_v);
}

static void set_db_option(VALUE o_options, VALUE opts, leveldb::Options* options) {
  if(!NIL_P(o_options)) {
    Check_Type(opts, T_HASH);

    set_val(opts, k_create_if_missing, o_options, &(options->create_if_missing));
    set_val(opts, k_error_if_exists, o_options, &(options->error_if_exists));
    set_val(opts, k_paranoid_checks, o_options, &(options->paranoid_checks));
    set_val(opts, k_write_buffer_size, o_options, &(options->write_buffer_size));
    set_val(opts, k_max_open_files, o_options, &(options->max_open_files));
    set_val(opts, k_block_size, o_options, &(options->block_size));
    set_val(opts, k_block_restart_interval, o_options, &(options->block_restart_interval));

    VALUE v;
    v = rb_hash_aref(opts, k_block_cache_size);
    if(!NIL_P(v)) {
      if(FIXNUM_P(v)) {
        options->block_cache = leveldb::NewLRUCache(NUM2INT(v));
        rb_iv_set(o_options, "@block_cache_size", v);
      } else {
        rb_raise(rb_eTypeError, "invalid type for %s", rb_id2name(SYM2ID(k_block_cache_size)));
      }
    }

    v = rb_hash_aref(opts, k_compression);
    rb_iv_set(o_options, "@compression", UINT2NUM(options->compression));
    if(!NIL_P(v)) {
      if(FIXNUM_P(v)) {
        switch(NUM2INT(v)) {
        case leveldb::kNoCompression:
          options->compression = leveldb::kNoCompression;
          rb_iv_set(o_options, "@compression", v);
          break;

        case leveldb::kSnappyCompression:
          options->compression = leveldb::kSnappyCompression;
          rb_iv_set(o_options, "@compression", v);
          break;

        default:
          rb_raise(rb_eTypeError, "invalid type for %s", rb_id2name(SYM2ID(k_compression)));
          break;
        }
      } else {
        rb_raise(rb_eTypeError, "invalid type for %s", rb_id2name(SYM2ID(k_compression)));
      }
    }
  }
}

/*
 * call-seq:
 *   make(pathname, options)
 *
 * open level-db database
 *
 * pathname path for database
 *
 * [options[ :create_if_missing ]] create if database doesn't exit
 *
 * [options[ :error_if_exists ]] raise error if database exists
 *
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
static VALUE db_make(int argc, VALUE* argv, VALUE self) {
  VALUE v_pathname, v_options;
  rb_scan_args(argc, argv, "11", &v_pathname, &v_options);
  Check_Type(v_pathname, T_STRING);

  auto_ptr<bound_db> db(new bound_db);
  std::string pathname = std::string((char*)RSTRING_PTR(v_pathname));

  leveldb::Options options;
  VALUE o_options = rb_class_new_instance(0, NULL, c_db_options);
  set_db_option(o_options, v_options, &options);

  leveldb::Status status = leveldb::DB::Open(options, pathname, &db->db);
  VALUE o_db = Data_Wrap_Struct(self, NULL, db_free, db.release());
  RAISE_ON_ERROR(status);

  rb_iv_set(o_db, "@options", o_options);
  VALUE init_argv[1] = { v_pathname };
  rb_obj_call_init(o_db, 1, init_argv);

  return o_db;
}

static VALUE db_close(VALUE self) {
  bound_db* db;
  Data_Get_Struct(self, bound_db, db);

  if(db->db != NULL) {
    delete db->db;
    db->db = NULL;
  }
  return Qtrue;
}

static leveldb::ReadOptions parse_read_options(VALUE options) {
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

static leveldb::WriteOptions parse_write_options(VALUE options) {
  leveldb::WriteOptions writeOptions;

  if(!NIL_P(options)) {
    Check_Type(options, T_HASH);
    if(rb_hash_aref(options, k_sync) == Qtrue)
      writeOptions.sync = true;
  }

  return writeOptions;
}

#define RUBY_STRING_TO_SLICE(x) leveldb::Slice(RSTRING_PTR(x), RSTRING_LEN(x))
#define SLICE_TO_RUBY_STRING(x) rb_str_new(x.data(), x.size())
#define STRING_TO_RUBY_STRING(x) rb_str_new(x.data(), x.size())

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
static VALUE db_get(int argc, VALUE* argv, VALUE self) {
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

static VALUE db_delete(int argc, VALUE* argv, VALUE self) {
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

static VALUE db_exists(VALUE self, VALUE v_key) {
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
static VALUE db_put(int argc, VALUE* argv, VALUE self) {
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

static VALUE db_size(VALUE self) {
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

static VALUE db_init(VALUE self, VALUE v_pathname) {
  rb_iv_set(self, "@pathname", v_pathname);
  return self;
}

typedef struct current_iteration {
  leveldb::Iterator* iterator;
  bool passed_limit;
  bool check_limit;
  bool reversed;
  int checked_valid; // 0 = unchecked, 1 = valid, -1 = invalid
  std::string key_to_str;
  leveldb::Slice current_key;
} current_iteration;

static void current_iteration_free(current_iteration* iter) {
  delete iter;
}

static VALUE iter_make(VALUE klass, VALUE db, VALUE options) {
  if(c_db != rb_funcall(db, k_class, 0)) {
    rb_raise(rb_eArgError, "db must be a LevelDB::DB");
  }

  bound_db* b_db;
  Data_Get_Struct(db, bound_db, b_db);

  current_iteration* iter = new current_iteration;
  iter->passed_limit = false;
  iter->check_limit = false;
  iter->checked_valid = 0;
  iter->iterator = b_db->db->NewIterator(uncached_read_options);

  VALUE o_iter = Data_Wrap_Struct(klass, NULL, current_iteration_free, iter);

  VALUE argv[2];
  argv[0] = db;
  argv[1] = options;
  rb_obj_call_init(o_iter, 2, argv);

  return o_iter;
}

static VALUE iter_init(VALUE self, VALUE db, VALUE options) {
  if(c_db != rb_funcall(db, k_class, 0)) {
    rb_raise(rb_eArgError, "db must be a LevelDB::DB");
  }

  rb_iv_set(self, "@db", db);
  current_iteration* iter;
  Data_Get_Struct(self, current_iteration, iter);

  VALUE key_from = Qnil;
  VALUE key_to = Qnil;

  if(!NIL_P(options)) {
    Check_Type(options, T_HASH);
    key_from = rb_hash_aref(options, k_from);
    key_to = rb_hash_aref(options, k_to);

    if(RTEST(key_to)) {
      iter->check_limit = true;
      iter->key_to_str = RUBY_STRING_TO_SLICE(rb_funcall(key_to, k_to_s, 0)).ToString();
    }

    rb_iv_set(self, "@from", key_from);
    rb_iv_set(self, "@to", key_to);
    if(NIL_P(rb_hash_aref(options, k_reversed))) {
      iter->reversed = false;
      rb_iv_set(self, "@reversed", false);
    } else {
      iter->reversed = true;
      rb_iv_set(self, "@reversed", true);
    }
  }

  if(RTEST(key_from)) {
    iter->iterator->Seek(RUBY_STRING_TO_SLICE(rb_funcall(key_from, k_to_s, 0)));
  } else {
    if(iter->reversed) {
      iter->iterator->SeekToLast();
    } else {
      iter->iterator->SeekToFirst();
    }
  }

  return self;
}

static bool iter_valid(current_iteration* iter) {
  if(iter->checked_valid == 0) {
    if(iter->passed_limit) {
      iter->checked_valid = -2;
    } else {
      if(iter->iterator->Valid()) {
        iter->current_key = iter->iterator->key();

        if(iter->check_limit &&
            (iter->reversed ?
              (iter->current_key.ToString() < iter->key_to_str) :
              (iter->current_key.ToString() > iter->key_to_str))) {
          iter->passed_limit = true;
          iter->checked_valid = -2;
        } else {
          iter->checked_valid = 1;
        }

      } else {
        iter->checked_valid = -1;
      }
    }
  }

  if(iter->checked_valid == 1)
    return true;
  else
    return false;
}

static VALUE iter_invalid_reason(VALUE self) {
  current_iteration* iter;
  Data_Get_Struct(self, current_iteration, iter);
  if(iter_valid(iter)) {
    return Qnil;
  } else {
    return INT2FIX(iter->checked_valid);
  }
}

static VALUE iter_next_value(current_iteration* iter) {
  VALUE arr = rb_ary_new2(2);
  rb_ary_push(arr, SLICE_TO_RUBY_STRING(iter->current_key));
  rb_ary_push(arr, SLICE_TO_RUBY_STRING(iter->iterator->value()));
  return arr;
}

static void iter_scan_iterator(current_iteration* iter) {
  if(iter->reversed)
    iter->iterator->Prev();
  else
    iter->iterator->Next();
  iter->checked_valid = 0;
}

static VALUE iter_peek(VALUE self) {
  current_iteration* iter;
  Data_Get_Struct(self, current_iteration, iter);
  if(iter_valid(iter)) {
    return iter_next_value(iter);
  } else {
    return Qnil;
  }
}

static VALUE iter_scan(VALUE self) {
  current_iteration* iter;
  Data_Get_Struct(self, current_iteration, iter);
  if(iter_valid(iter))
    iter_scan_iterator(iter);
  return Qnil;
}

static VALUE iter_next(VALUE self) {
  current_iteration* iter;
  Data_Get_Struct(self, current_iteration, iter);

  VALUE arr = Qnil;

  if(iter_valid(iter)) {
    arr = iter_next_value(iter);
    iter_scan_iterator(iter);
  }

  return arr;
}

static VALUE iter_each(VALUE self) {
  current_iteration* iter;
  Data_Get_Struct(self, current_iteration, iter);

  while(iter_valid(iter)) {
    rb_yield(iter_next_value(iter));
    iter_scan_iterator(iter);
  }

  RAISE_ON_ERROR(iter->iterator->status());
  delete iter->iterator;
  return self;
}

typedef struct bound_batch {
  leveldb::WriteBatch batch;
} bound_batch;

static void batch_free(bound_batch* batch) {
  delete batch;
}

static VALUE batch_make(VALUE klass) {
  bound_batch* batch = new bound_batch;
  batch->batch = leveldb::WriteBatch();

  VALUE o_batch = Data_Wrap_Struct(klass, NULL, batch_free, batch);
  VALUE argv[0];
  rb_obj_call_init(o_batch, 0, argv);

  return o_batch;
}

static VALUE batch_put(VALUE self, VALUE v_key, VALUE v_value) {
  Check_Type(v_key, T_STRING);
  Check_Type(v_value, T_STRING);

  bound_batch* batch;
  Data_Get_Struct(self, bound_batch, batch);
  batch->batch.Put(RUBY_STRING_TO_SLICE(v_key), RUBY_STRING_TO_SLICE(v_value));

  return v_value;
}

static VALUE batch_delete(VALUE self, VALUE v_key) {
  Check_Type(v_key, T_STRING);
  bound_batch* batch;
  Data_Get_Struct(self, bound_batch, batch);
  batch->batch.Delete(RUBY_STRING_TO_SLICE(v_key));
  return Qtrue;
}

static VALUE db_batch(int argc, VALUE* argv, VALUE self) {
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

extern "C" {
void Init_leveldb() {
  k_fill = ID2SYM(rb_intern("fill_cache"));
  k_verify = ID2SYM(rb_intern("verify_checksums"));
  k_sync = ID2SYM(rb_intern("sync"));
  k_from = ID2SYM(rb_intern("from"));
  k_to = ID2SYM(rb_intern("to"));
  k_reversed = ID2SYM(rb_intern("reversed"));
  k_class = rb_intern("class");
  k_name = rb_intern("name");
  k_create_if_missing = ID2SYM(rb_intern("create_if_missing"));
  k_error_if_exists = ID2SYM(rb_intern("error_if_exists"));
  k_paranoid_checks = ID2SYM(rb_intern("paranoid_checks"));
  k_write_buffer_size = ID2SYM(rb_intern("write_buffer_size"));
  k_block_cache_size = ID2SYM(rb_intern("block_cache_size"));
  k_block_size = ID2SYM(rb_intern("block_size"));
  k_block_restart_interval = ID2SYM(rb_intern("block_restart_interval"));
  k_compression = ID2SYM(rb_intern("compression"));
  k_max_open_files = ID2SYM(rb_intern("max_open_files"));
  k_to_s = rb_intern("to_s");

  uncached_read_options = leveldb::ReadOptions();
  uncached_read_options.fill_cache = false;

  m_leveldb = rb_define_module("LevelDB");

  c_db = rb_define_class_under(m_leveldb, "DB", rb_cObject);
  rb_define_singleton_method(c_db, "make", RUBY_METHOD_FUNC(db_make), -1);
  rb_define_method(c_db, "initialize", RUBY_METHOD_FUNC(db_init), 1);
  rb_define_method(c_db, "get", RUBY_METHOD_FUNC(db_get), -1);
  rb_define_method(c_db, "delete", RUBY_METHOD_FUNC(db_delete), -1);
  rb_define_method(c_db, "put", RUBY_METHOD_FUNC(db_put), -1);
  rb_define_method(c_db, "exists?", RUBY_METHOD_FUNC(db_exists), 1);
  rb_define_method(c_db, "close", RUBY_METHOD_FUNC(db_close), 0);
  rb_define_method(c_db, "size", RUBY_METHOD_FUNC(db_size), 0);
  rb_define_method(c_db, "batch", RUBY_METHOD_FUNC(db_batch), -1);

  c_iter = rb_define_class_under(m_leveldb, "Iterator", rb_cObject);
  rb_define_singleton_method(c_iter, "make", RUBY_METHOD_FUNC(iter_make), 2);
  rb_define_method(c_iter, "initialize", RUBY_METHOD_FUNC(iter_init), 2);
  rb_define_method(c_iter, "each", RUBY_METHOD_FUNC(iter_each), 0);
  rb_define_method(c_iter, "next", RUBY_METHOD_FUNC(iter_next), 0);
  rb_define_method(c_iter, "scan", RUBY_METHOD_FUNC(iter_scan), 0);
  rb_define_method(c_iter, "peek", RUBY_METHOD_FUNC(iter_peek), 0);
  rb_define_method(c_iter, "invalid_reason", RUBY_METHOD_FUNC(iter_invalid_reason), 0);

  c_batch = rb_define_class_under(m_leveldb, "WriteBatch", rb_cObject);
  rb_define_singleton_method(c_batch, "make", RUBY_METHOD_FUNC(batch_make), 0);
  rb_define_method(c_batch, "put", RUBY_METHOD_FUNC(batch_put), 2);
  rb_define_method(c_batch, "delete", RUBY_METHOD_FUNC(batch_delete), 1);

  c_db_options = rb_define_class_under(m_leveldb, "Options", rb_cObject);

  c_error = rb_define_class_under(m_leveldb, "Error", rb_eStandardError);
}
}
