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

# Require key from morph.io Secrets
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

  # migrate away any extra columns (e.g., scraped_at)
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

# Upsert using only existing columns
def upsert_row(row_hash)
  cols_existing = db.execute("PRAGMA table_info(festivals)").map { |r| r[1] }
  cols = DESIRED_COLS & cols_existing
  vals = cols.map { |c| row_hash[c.to_sym] }
  placeholders = (['?'] * cols.length).join(',')
  sql = "INSERT OR REPLACE INTO festivals (#{cols.join(',')}) VALUES (#{placeholders})"
  db.execute(sql, vals)
end

# ====== ScraperAPI helpers ======
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

def mask_key(u)
  u.to_s.sub(/api_key=[^&]+/, 'api_key=***')
end

def decompress(body, encoding)
  case (encoding || "").downcase
  when 'gzip'
    Zlib::GzipReader.new(StringIO.new(body)).read
  when 'deflate'
    Zlib::Inflate.inflate(body)
  else
    body
  end
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

# Try no-render first (faster), then render (handles JS)
def http_get(url, referer: "https://filmfreeway.com/")
  [:no_render, :render].each do |mode|
    target = scraperapi_url(url, mode: mode)
    begin
      return timed_request(target, referer: "https://filmfreeway.com/")
    rescue => e
      STDERR.puts "… #{mode} failed: #{e}"
      if e.to_s =~ /HTTP (429|5\d\d)|Timeout/
        sleep 1.0
        STDERR.puts "Retrying #{mode} once…"
        begin
          return timed_request(target, referer: "https://filmfreeway.com/")
        rescue => e2
          STDERR.puts "… retry failed: #{e2}"
        end
      end
    end
  end
  raise "failed to fetch #{url}"
end

def http_get_rendered(url)
  timed_request(scraperapi_url(url, mode: :render), referer: "https://filmfreeway.com/")
end

# ====== Parsing helpers (label-precise) ======
SOCIAL = %w[facebook.com twitter.com instagram.com youtube.com tiktok.com linkedin.com linktr.ee]

def absolute(base, href)
  return nil if href.nil? || href.empty?
  URI.join(base, href).to_s
rescue
  href
end

# Find the smallest useful container that holds a label and its value/controls
def container_for_label(doc, label)
  down = "translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz')"

  # <dt>Label</dt> <dd>...</dd>
  dt = doc.at_xpath("//dt[#{down}='#{label.downcase}']")
  return dt.xpath("following-sibling::*[1]").first if dt

  # <strong>/<b>Label</strong> VALUE
  strong = doc.at_xpath("//*[self::strong or self::b][#{down}='#{label.downcase}']")
  return (strong.ancestors('li,div,section,p,dd').first || strong.parent) if strong

  # generic element with exact label text → return nearest container
  lbl = doc.at_xpath("//*[self::div or self::span or self::p or self::li][#{down}='#{label.downcase}']")
  return (lbl.ancestors('li,div,section,p,dd').first || lbl.parent) if lbl

  # final: any node that contains the word as a separate token
  contains = doc.at_xpath("//*[contains(#{down},'#{label.downcase}')]")
  if contains
    return contains.ancestors('li,div,section,p,dd').first || contains.parent
  end

  nil
end

# Visible text in the container, minus the label word itself
def value_text_from_container(container, label)
  return nil unless container
  txt = container.text.strip
  txt = txt.gsub(/#{Regexp.escape(label)}\s*[:\-–—]?\s*/i, '')
  txt = txt.gsub(/\s+/, ' ').strip
  txt.empty? ? nil : txt
end

# Find <a> by visible text (case-insensitive). Exact match then contains.
def at_link_by_text(doc, label)
  down = "translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz')"
  doc.at_xpath("//a[@href and #{down}='#{label.downcase}']") ||
    doc.at_xpath("//a[@href and contains(#{down},'#{label.downcase}')]")
end

# Extract URL/email from JS-y attributes when href is a placeholder
def extract_url_from_attrs(node)
  if node['onclick']
    if node['onclick'] =~ /mailto:([^\s'")<>]+)/i
      return "mailto:#{$1}"
    end
    if node['onclick'] =~ /https?:\/\/[^\s'"]+/i
      return node['onclick'][/https?:\/\/[^\s'"]+/i]
    end
  end
  %w[
    data-clipboard-text data-email data-address data-mail data-mailto data-text data-value
    data-copy data-copy-text data-contact data-user data-domain data-href data-url
  ].each do |attr|
    v = node[attr]
    next unless
