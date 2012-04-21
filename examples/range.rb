
$LOAD_PATH.unshift << File.expand_path('../../lib', __FILE__)

require 'leveldb'

db = LevelDB::DB.new "/tmp/asdf"



db['0'] = "3"
db['2'] = "30"
db['1'] = "6"



db.each(:from => 0, :to => 2) do |k, v|
  puts "#{k} => #{v}"
end

