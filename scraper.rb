# --- stdlib / gems ---
require 'net/http'
require 'uri'
require 'zlib'
require 'stringio'
require 'json'
require 'nokogiri'
require 'sqlite3'

# ====== Config ======
DB_FILE = 'data.sqlite'
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
OPEN_TIMEOUT = 15
READ_TIMEOUT = 30
PER_REQ_CAP  = (ENV['SCRAPER_TIMEOUT_SECS'] || "60").to_i

# morph.io Secret
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

  cols = db.execute("PRAGMA table_info(festivals)").map { |r| r[1] }
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
  cols = DESIRED_COLS & cols_existing
  vals = cols.map { |c| row_hash[c.to_sym] }
  placeholders = (['?'] * cols.length).join(',')
  sql = "INSERT OR REPLACE INTO festivals (#{cols.join(',')}) VALUES (#{placeholders})"
  db.execute(sql, vals)
end

# ====== HTTP via ScraperAPI ======
def scraperapi_url(target_url, mode: :no_render, country: 'us', session: 1001)
  base = "https://api.scraperapi.com/"
  params = {
    "api_key"        => MORPH_SCRAPERAPI,
    "url"            => target_url,
    "country_code"   => country,
    "session_number" => session
  }
  params["render"] = "true" if mode == :render
  uri = URI(base)
  uri.query = URI.encode_www_form(params)
  uri.to_s
end

def mask_key(u) u.to_s.sub(/api_key=[^&]+/, 'api_key=***') end

def decompress(body, encoding)
  case (encoding || "").downcase
  when 'gzip'   then Zlib::GzipReader.new(StringIO.new(body)).read
  when 'deflate' then Zlib::Inflate.inflate(body)
  else body end
end

def timed_request(target, referer:)
  uri = URI(target)
  req = Net::HTTP::Get.new(uri)
  req['User-Agent']      = UA
  req['Accept']          = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8'
  req['Accept-Language'] = 'en-US,en;q=0.9'
  req['Accept-Encoding'] = 'gzip, deflate'
  req['Upgrade-Insecure-Requests'] = '1'
  req['Referer']         = referer
  req['Connection']      = 'keep-alive'

  start = Time.now
  res = nil
  STDERR.puts "GET via ScraperAPI: #{mask_key(uri)}"
  Net::HTTP.start(uri.host, uri.port, use_ssl: uri.scheme == 'https') do |h|
    h.open_timeout = OPEN_TIMEOUT
    h.read_timeout = READ_TIMEOUT
    res = h.request(req)
    raise "Timeout after #{PER_REQ_CAP}s for #{uri}" if (Time.now - start) > PER_REQ_CAP
  end
  code = res.code.to_i
  raise "HTTP #{code}" if code >= 400
  body = decompress(res.body, res['content-encoding'])
  STDERR.puts "✓ #{mask_key(uri)} (#{(Time.now - start).round(1)}s)"
  body
end

def http_get(url, referer: url)
  [:no_render, :render].each do |mode|
    begin
      return timed_request(scraperapi_url(url, mode: mode), referer: referer)
    rescue => e
      STDERR.puts "… #{mode} failed: #{e}"
      if e.to_s =~ /HTTP (429|5\d\d)|Timeout/
        sleep 1.0
        STDERR.puts "Retrying #{mode} once…"
        begin
          return timed_request(scraperapi_url(url, mode: mode), referer: referer)
        rescue => e2
          STDERR.puts "… retry failed: #{e2}"
        end
      end
    end
  end
  raise "failed to fetch #{url}"
end

def http_get_rendered(url) timed_request(scraperapi_url(url, mode: :render), referer: url) end

# ====== DOM helpers ======
SOCIAL = %w[facebook.com twitter.com instagram.com youtube.com tiktok.com linkedin.com linktr.ee]
MAP_HOSTS = %w[google.com maps.google.com goo.gl bing.com mapbox.com openstreetmap.org]
BLOCKY_CLASSES = /Modal|modal|Overlay|Dialog|Signup|Login|Str_
