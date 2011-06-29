require 'mkmf'
require 'fileutils'

Dir.chdir "../../leveldb"
system "make libleveldb.a" or abort
Dir.chdir "../ext/leveldb"

CONFIG['LDSHARED'] = "$(CXX) -shared"

$CFLAGS << " -I../../leveldb/include"
$LIBS << " -L../../leveldb -lleveldb"
create_makefile "leveldb/leveldb"
