#include <ruby.h>

#include "leveldb/db.h"
#include "leveldb/slice.h"
#include "leveldb/cache.h"
#include "leveldb/write_batch.h"

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

  VALUE db_make(VALUE klass, VALUE params) {
    Check_Type(params, T_HASH);
    VALUE path = rb_hash_aref(params, k_path);
    Check_Type(path, T_STRING);

    bound_db* db = new bound_db;
    std::string pathname = std::string((char*)RSTRING_PTR(path));

    bound_db_options* options = new bound_db_options;
    options->options = new leveldb::Options;
    if(hash_val_test(params, ID2SYM(rb_intern("create_if_missing")))) {
      options->options->create_if_missing = true;
    }

    if(hash_val_test(params, ID2SYM(rb_intern("error_if_exists")))) {
      options->options->error_if_exists = true;
    }
    leveldb::Status status = leveldb::DB::Open(*(options->options), pathname, &db->db);
    RAISE_ON_ERROR(status);

    VALUE o_db = Data_Wrap_Struct(klass, NULL, db_free, db);
    VALUE o_options = Data_Wrap_Struct(c_db_options, NULL, db_options_free, options);
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
    rb_define_singleton_method(c_db, "make", (VALUE (*)(...))db_make, 1);
    rb_define_method(c_db, "initialize", (VALUE (*)(...))db_init, 1);
    rb_define_method(c_db, "get", (VALUE (*)(...))db_get, -1);
    rb_define_method(c_db, "delete", (VALUE (*)(...))db_delete, -1);
    rb_define_method(c_db, "put", (VALUE (*)(...))db_put, -1);
    rb_define_method(c_db, "exists?", (VALUE (*)(...))db_exists, 1);
    rb_define_method(c_db, "close", (VALUE (*)(...))db_close, 0);
    rb_define_method(c_db, "size", (VALUE (*)(...))db_size, 0);
    rb_define_method(c_db, "each", (VALUE (*)(...))db_each, -1);
    rb_define_method(c_db, "reverse_each", (VALUE (*)(...))db_reverse_each, -1);
    rb_define_method(c_db, "batch", (VALUE (*)(...))db_batch, -1);

    c_batch = rb_define_class_under(m_leveldb, "WriteBatch", rb_cObject);
    rb_define_singleton_method(c_batch, "make", (VALUE (*)(...))batch_make, 0);
    rb_define_method(c_batch, "put", (VALUE (*)(...))batch_put, 2);
    rb_define_method(c_batch, "delete", (VALUE (*)(...))batch_delete, 1);

    c_error = rb_define_class_under(m_leveldb, "Error", rb_eStandardError);
    c_db_options = rb_define_class_under(m_leveldb, "Options", rb_cObject);
    rb_define_method(c_db_options, "paranoid_checks", (VALUE (*)(...))db_options_paranoid_checks, 0);
  }
}
