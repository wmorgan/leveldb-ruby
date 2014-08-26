require 'mkmf'
require 'fileutils'
require "./platform.rb"

set_platform_specific_variables!

$INCFLAGS << ' ' << `pkg-config --cflags-only-I leveldb`.strip << ' '
$LIBS << ' ' << `pkg-config --libs leveldb`.strip << ' '

create_makefile "leveldb/leveldb"
