require 'mkmf'
require 'fileutils'

Dir.chdir "../../leveldb"
system "make libleveldb.a" or abort
Dir.chdir "../ext/leveldb"

$CFLAGS << " -I../../leveldb/include"
$LIBS << " -L../../leveldb -Wl,--whole-archive -lleveldb -Wl,--no-whole-archive"
create_makefile "leveldb/leveldb"
