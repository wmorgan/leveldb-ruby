require 'mkmf'
require 'fileutils'
require "./platform.rb"

Dir.chdir "../../leveldb"
system "make libleveldb.a" or abort
Dir.chdir "../ext/leveldb"

set_platform_specific_variables!

$CFLAGS << " -I../../leveldb/include"
$LIBS << " -L../../leveldb -lleveldb"

create_makefile "leveldb/leveldb"
