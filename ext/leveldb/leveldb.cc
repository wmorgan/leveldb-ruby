#include <ruby.h>

#include "leveldb/db.h"
#include "leveldb/write_batch.h"

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

typedef struct bound_batch {
  bound_db* db;
  leveldb::WriteBatch* batch;
} bound_batch;

static void db_free(bound_db* db) {
  delete db->db;
}

static VALUE db_make(VALUE klass, VALUE v_pathname, VALUE v_create_if_necessary, VALUE v_break_if_exists) {
  Check_Type(v_pathname, T_STRING);

  bound_db* db = new bound_db;
  char* pathname_c = RSTRING_PTR(v_pathname);
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

static VALUE db_get(VALUE self, VALUE v_key) {
  Check_Type(v_key, T_STRING);

  bound_db* db;
  Data_Get_Struct(self, bound_db, db);

  std::string key = std::string((char*)RSTRING_PTR(v_key));
  std::string value;
  leveldb::Status status = db->db->Get(leveldb::ReadOptions(), key, &value);
  if(status.IsNotFound()) return Qnil;

  RAISE_ON_ERROR(status);
  return rb_str_new2(value.c_str());
}

static VALUE db_put(VALUE self, VALUE v_key, VALUE v_value) {
  Check_Type(v_key, T_STRING);
  Check_Type(v_value, T_STRING);

  bound_db* db;
  Data_Get_Struct(self, bound_db, db);

  std::string key = std::string((char*)RSTRING_PTR(v_key));
  std::string value = std::string((char*)RSTRING_PTR(v_value));

  leveldb::Status status = db->db->Put(leveldb::WriteOptions(), key, value);
  RAISE_ON_ERROR(status);

  return v_value;
}

static VALUE db_init(VALUE self, VALUE v_pathname) {
  rb_iv_set(self, "@pathname", v_pathname);
  return self;
}

extern "C" {
void Init_leveldb() {
  VALUE m_leveldb;

  m_leveldb = rb_define_module("LevelDB");

  c_db = rb_define_class_under(m_leveldb, "DB", rb_cObject);
  rb_define_singleton_method(c_db, "make", (VALUE (*)(...))db_make, 3);
  rb_define_method(c_db, "initialize", (VALUE (*)(...))db_init, 1);
  rb_define_method(c_db, "get", (VALUE (*)(...))db_get, 1);
  rb_define_method(c_db, "put", (VALUE (*)(...))db_put, 2);
  //rb_define_singleton_method(c_index, "create", db_create, 1);
  //rb_define_singleton_method(c_index, "load", db_load, 1);

  c_error = rb_define_class_under(m_leveldb, "Error", rb_eStandardError);
}
}
