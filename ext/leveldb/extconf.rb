require 'mkmf'
require 'fileutils'

Dir.chdir "../../leveldb"
system "make libleveldb.a" or abort
Dir.chdir "../ext/leveldb"

CONFIG['LDSHARED'] = CONFIG['LDSHARED'].sub('$(CC)', '$(CXX)').sub('gcc','g++')

$CFLAGS << " -I../../leveldb/include"
$LIBS << " -L../../leveldb -lleveldb"

create_makefile "leveldb/leveldb"
