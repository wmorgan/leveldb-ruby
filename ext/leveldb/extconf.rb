require 'mkmf'
require 'fileutils'

Dir.chdir "../../leveldb"
system "make libleveldb.a" or abort
Dir.chdir "../ext/leveldb"

CONFIG['LDSHARED'] = "$(CXX) -shared"

$CFLAGS << " -I../../leveldb/include"
$LIBS << " -L../../leveldb -lleveldb"
if RUBY_PLATFORM =~ /darwin/
  $CFLAGS.sub! "-arch i386", ""
  $LDFLAGS.sub! "-arch i386", ""
  $LIBS << " -lruby" # whyyyyy
end

create_makefile "leveldb/leveldb"
