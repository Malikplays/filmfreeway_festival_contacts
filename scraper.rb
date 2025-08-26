require 'sqlite3'

db = SQLite3::Database.new('data.sqlite')
db.execute <<-SQL
  CREATE TABLE IF NOT EXISTS data (
    name TEXT PRIMARY KEY,
    t INTEGER
  );
SQL
db.execute("INSERT OR REPLACE INTO data (name, t) VALUES (?, ?)", ['ping', Time.now.to_i])
puts "ok: wrote row to data.sqlite"
