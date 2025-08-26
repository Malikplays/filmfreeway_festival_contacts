# --- stdlib / gems ---
require 'net/http'
require 'uri'
require 'zlib'
require 'stringio'
require 'json'
require 'nokogiri'
require 'sqlite3'

# ====== Config ======
DB_FILE      = 'data.sqlite'
UA           = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
OPEN_TIMEOUT = 15
READ_TIMEOUT = 30
PER_REQ_CAP  = (ENV['SCRAPER_TIMEOUT_SECS'] || "60").to_i

# morph.io Secret (Settings → Secrets)
MORPH_SCRAPERAPI = ENV['MORPH_SCRAPERAPI'] or abort("Set MORPH_SCRAPERAPI in morph.io Secrets")

# ====== DB ======
DESIRED_COLS = %w[source_url name website email director location phone]

def ensure_table!
  db = SQLite3::Database.new(DB_FILE)
  db.execute <<-SQL
    CREATE TABLE IF NOT EXISTS festivals (
      source_url TEXT PRIMARY KEY,
      name       TEXT,
      website    TEXT,
      email      TEXT,
      director   TEXT,
      location   TEXT,
      phone      TEXT
    );
  SQL

  # Drop any unexpected columns (e.g., old scraped_at) by recreating the table
  cols  = db.execute("PRAGMA table_info(festivals)").map { |r| r[1] }
  extra = cols - DESIRED_COLS
  if extra.any?
    db.transaction do
      db.execute <<-SQL
        CREATE TABLE IF NOT EXISTS festivals_new (
          source_url TEXT PRIMARY KEY,
          name       TEXT,
          website    TEXT,
          email      TEXT,
          director   TEXT,
          location   TEXT,
          phone      TEXT
        );
      SQL
      common = (DESIRED_COLS & cols)
      if common.any?
        db.execute("INSERT OR REPLACE INTO festivals_new (#{common.join(',')}) SELECT #{common.join(',')} FROM festivals")
      end
      db.execute("DROP TABLE festivals")
      db.execute("ALTER TABLE festivals_new RENAME TO festivals")
    end
  end
  db
end

def db
  @db ||= ensure_table!
end

def upsert_row(row_hash)
  cols_existing = db.execute("PRAGMA table_info(festivals)").map { |r| r[1] }
