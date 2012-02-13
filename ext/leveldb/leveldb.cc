#include <ruby.h>

#include "leveldb/db.h"
#include "leveldb/slice.h"
#include "leveldb/write_batch.h"

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
static ID to_s;
static leveldb::ReadOptions uncached_read_options;

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

static VALUE db_make(VALUE klass, VALUE v_pathname, VALUE v_create_if_necessary, VALUE v_break_if_exists) {
  Check_Type(v_pathname, T_STRING);

  bound_db* db = new bound_db;
  std::string pathname = std::string((char*)RSTRING_PTR(v_pathname));

  leveldb::Options options;
  if(RTEST(v_create_if_necessary)) options.create_if_missing = true;
  if(RTEST(v_break_if_exists)) options.error_if_exists = true;
  leveldb::Status status = leveldb::DB::Open(options, pathname, &db->db);
  RAISE_ON_ERROR(status);

  VALUE o_db = Data_Wrap_Struct(klass, NULL, db_free, db);
  VALUE argv[1] = { v_pathname };
  rb_obj_call_init(o_db, 1, argv);

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
    rb_raise(rb_eArgError, "db Must be a LevelDB::DB");
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
    rb_raise(rb_eArgError, "db Must be a LevelDB::DB");
  }

  rb_iv_set(self, "@db", db);
  current_iteration* iter;
  Data_Get_Struct(self, current_iteration, iter);

  VALUE key_from;
  VALUE key_to;

  if(!NIL_P(options)) {
    Check_Type(options, T_HASH);
    key_from = rb_hash_aref(options, k_from);
    key_to = rb_hash_aref(options, k_to);

    if(RTEST(key_to)) {
      iter->check_limit = true;
      iter->key_to_str = RUBY_STRING_TO_SLICE(rb_funcall(key_to, to_s, 0)).ToString();
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
    iter->iterator->Seek(RUBY_STRING_TO_SLICE(rb_funcall(key_from, to_s, 0)));
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
  to_s = rb_intern("to_s");
  uncached_read_options = leveldb::ReadOptions();
  uncached_read_options.fill_cache = false;

  m_leveldb = rb_define_module("LevelDB");

  c_db = rb_define_class_under(m_leveldb, "DB", rb_cObject);
  rb_define_singleton_method(c_db, "make", (VALUE (*)(...))db_make, 3);
  rb_define_method(c_db, "initialize", (VALUE (*)(...))db_init, 1);
  rb_define_method(c_db, "get", (VALUE (*)(...))db_get, -1);
  rb_define_method(c_db, "delete", (VALUE (*)(...))db_delete, -1);
  rb_define_method(c_db, "put", (VALUE (*)(...))db_put, -1);
  rb_define_method(c_db, "exists?", (VALUE (*)(...))db_exists, 1);
  rb_define_method(c_db, "close", (VALUE (*)(...))db_close, 0);
  rb_define_method(c_db, "size", (VALUE (*)(...))db_size, 0);
  rb_define_method(c_db, "batch", (VALUE (*)(...))db_batch, -1);

  c_iter = rb_define_class_under(m_leveldb, "Iterator", rb_cObject);
  rb_define_singleton_method(c_iter, "make", (VALUE (*)(...))iter_make, 2);
  rb_define_method(c_iter, "initialize", (VALUE (*)(...))iter_init, 2);
  rb_define_method(c_iter, "each", (VALUE (*)(...))iter_each, 0);
  rb_define_method(c_iter, "next", (VALUE (*)(...))iter_next, 0);
  rb_define_method(c_iter, "scan", (VALUE (*)(...))iter_scan, 0);
  rb_define_method(c_iter, "peek", (VALUE (*)(...))iter_peek, 0);
  rb_define_method(c_iter, "invalid_reason", (VALUE (*)(...))iter_invalid_reason, 0);

  c_batch = rb_define_class_under(m_leveldb, "WriteBatch", rb_cObject);
  rb_define_singleton_method(c_batch, "make", (VALUE (*)(...))batch_make, 0);
  rb_define_method(c_batch, "put", (VALUE (*)(...))batch_put, 2);
  rb_define_method(c_batch, "delete", (VALUE (*)(...))batch_delete, 1);

  c_error = rb_define_class_under(m_leveldb, "Error", rb_eStandardError);
}
}
