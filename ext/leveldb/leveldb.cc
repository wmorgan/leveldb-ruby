#include <ruby.h>

#include "leveldb/db.h"
#include "leveldb/slice.h"

static VALUE m_leveldb;
static VALUE c_db;
static VALUE c_error;

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

#define RUBY_STRING_TO_SLICE(x) leveldb::Slice(RSTRING_PTR(x), RSTRING_LEN(x))
#define SLICE_TO_RUBY_STRING(x) rb_str_new(x.data(), x.size())
#define STRING_TO_RUBY_STRING(x) rb_str_new(x.data(), x.size())
static VALUE db_get(VALUE self, VALUE v_key) {
  Check_Type(v_key, T_STRING);

  bound_db* db;
  Data_Get_Struct(self, bound_db, db);

  leveldb::Slice key = RUBY_STRING_TO_SLICE(v_key);
  std::string value;
  leveldb::Status status = db->db->Get(leveldb::ReadOptions(), key, &value);
  if(status.IsNotFound()) return Qnil;

  RAISE_ON_ERROR(status);
  return STRING_TO_RUBY_STRING(value);
}

static VALUE db_delete(VALUE self, VALUE v_key) {
  Check_Type(v_key, T_STRING);

  bound_db* db;
  Data_Get_Struct(self, bound_db, db);

  leveldb::Slice key = RUBY_STRING_TO_SLICE(v_key);
  std::string value;
  leveldb::Status status = db->db->Get(leveldb::ReadOptions(), key, &value);

  if(status.IsNotFound()) return Qnil;

  status = db->db->Delete(leveldb::WriteOptions(), key);
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

static VALUE db_put(VALUE self, VALUE v_key, VALUE v_value) {
  Check_Type(v_key, T_STRING);
  Check_Type(v_value, T_STRING);

  bound_db* db;
  Data_Get_Struct(self, bound_db, db);

  leveldb::Slice key = RUBY_STRING_TO_SLICE(v_key);
  leveldb::Slice value = RUBY_STRING_TO_SLICE(v_value);
  leveldb::Status status = db->db->Put(leveldb::WriteOptions(), key, value);

  RAISE_ON_ERROR(status);

  return v_value;
}

static VALUE db_size(VALUE self) {
  long count = 0;

  bound_db* db;
  Data_Get_Struct(self, bound_db, db);
  leveldb::Iterator* it = db->db->NewIterator(leveldb::ReadOptions());

  // apparently this is how we have to do it. slow and painful!
  for (it->SeekToFirst(); it->Valid(); it->Next()) count++;
  RAISE_ON_ERROR(it->status());
  delete it;
  return INT2NUM(count);
}

static VALUE db_iterate(VALUE self, VALUE key_from, VALUE key_to, bool reversed) {
  bound_db* db;
  Data_Get_Struct(self, bound_db, db);
  leveldb::Iterator* it = db->db->NewIterator(leveldb::ReadOptions());
  ID to_s = rb_intern("to_s");

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

static VALUE db_each(int argc, VALUE* argv, VALUE self) {
  VALUE key_from, key_to;
  rb_scan_args(argc, argv, "02", &key_from, &key_to);

  return db_iterate(self, key_from, key_to, false);
}

static VALUE db_reverse_each(int argc, VALUE* argv, VALUE self) {
  VALUE key_from, key_to;
  rb_scan_args(argc, argv, "02", &key_from, &key_to);

  return db_iterate(self, key_from, key_to, true);
}

static VALUE db_init(VALUE self, VALUE v_pathname) {
  rb_iv_set(self, "@pathname", v_pathname);
  return self;
}

extern "C" {
void Init_leveldb() {
  m_leveldb = rb_define_module("LevelDB");

  c_db = rb_define_class_under(m_leveldb, "DB", rb_cObject);
  rb_define_singleton_method(c_db, "make", (VALUE (*)(...))db_make, 3);
  rb_define_method(c_db, "initialize", (VALUE (*)(...))db_init, 1);
  rb_define_method(c_db, "get", (VALUE (*)(...))db_get, 1);
  rb_define_method(c_db, "delete", (VALUE (*)(...))db_delete, 1);
  rb_define_method(c_db, "put", (VALUE (*)(...))db_put, 2);
  rb_define_method(c_db, "exists?", (VALUE (*)(...))db_exists, 1);
  rb_define_method(c_db, "close", (VALUE (*)(...))db_close, 0);
  rb_define_method(c_db, "size", (VALUE (*)(...))db_size, 0);
  rb_define_method(c_db, "each", (VALUE (*)(...))db_each, -1);
  rb_define_method(c_db, "reverse_each", (VALUE (*)(...))db_reverse_each, -1);

  c_error = rb_define_class_under(m_leveldb, "Error", rb_eStandardError);
}
}
