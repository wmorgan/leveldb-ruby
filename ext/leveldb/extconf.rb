require 'mkmf'
require 'fileutils'
require "./platform.rb"

Dir.chdir "../../leveldb"
system "OPT=\"-O2 -DNDEBUG -fPIC\" make libleveldb.a" or abort
Dir.chdir "../ext/leveldb"

set_platform_specific_variables!

$CFLAGS << " -I../../leveldb/include"
$CFLAGS.sub! "-arch i386 ", ""
$LDFLAGS.sub! "-arch i386 ", ""
$LIBS << " -L../../leveldb -lleveldb"

create_makefile "leveldb/leveldb"
