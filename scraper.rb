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
MORPH_SCRAPERAPI = ENV['MORPH_SCRAPERAPI'] || abort("Set MORPH_SCRAPERAPI in morph.io Secrets")

# ====== DB ======
DESIRED_COLS = %w[source_url name website location phone]

def ensure_table!
  db = SQLite3::Database.new(DB_FILE)
  db.execute <<-SQL
    CREATE TABLE IF NOT EXISTS festivals (
      source_url TEXT PRIMARY KEY,
      name       TEXT,
      website    TEXT,
      location   TEXT,
      phone      TEXT
    );
  SQL

  # Remove any legacy columns
  cols_now   = db.execute("PRAGMA table_info(festivals)").map { |r| r[1] }
  extra_cols = cols_now - DESIRED_COLS
  if extra_cols.any?
    db.transaction do
      db.execute <<-SQL
        CREATE TABLE IF NOT EXISTS festivals_new (
          source_url TEXT PRIMARY KEY,
          name       TEXT,
          website    TEXT,
          location   TEXT,
          phone      TEXT
        );
      SQL
      common = (DESIRED_COLS & cols_now)
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

def mask_key(u)
  u.to_s.sub(/api_key=[^&]+/, 'api_key=***')
end

def decompress(body, encoding)
  case (encoding || "").downcase
  when 'gzip'    then Zlib::GzipReader.new(StringIO.new(body)).read
  when 'deflate' then Zlib::Inflate.inflate(body)
  else body
  end
rescue
  body
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

def http_get(url, referer: nil)
  referer ||= url
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

def http_get_rendered(url)
  timed_request(scraperapi_url(url, mode: :render), referer: url)
end

# ====== DOM helpers ======
BLOCKY_CLASSES  = /(Modal|modal|Overlay|Dialog|Signup|Login|StrongPassword|cookie|vex_)/i

def absolute(base, href)
  return nil if href.nil? || href.empty?
  URI.join(base, href).to_s
rescue
  href
end

def small_container(node)
  return nil unless node
  node.ancestors('dd').first ||
    node.ancestors('li').first ||
    node.ancestors('div').find { |d| (d['class'].to_s !~ BLOCKY_CLASSES) && d.inner_html.size < 4000 } ||
    node.ancestors('section').find { |s| s.inner_html.size < 4000 } ||
    node
end

def find_label_node(doc, *labels)
  labels.each do |label|
    down = "translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz')"
    n = doc.at_xpath("//dt[#{down}='#{label.downcase}']"); return n if n
    n = doc.at_xpath("//*[self::strong or self::b][#{down}='#{label.downcase}']"); return n if n
    n = doc.at_xpath("//*[self::div or self::span or self::p or self::li][#{down}='#{label.downcase}']"); return n if n
    n = doc.at_xpath("//*[contains(#{down},'#{label.downcase}')]"); return n if n
  end
  nil
end

def looks_like_noise?(s)
  return true if s.nil? || s.empty?
  return true if s.size > 500
  bad = ['{"user_signed_in"', 'fonts_to_prefetch', 'ModalLogin', 'StrongPassword', 'setup.require(']
  return true if bad.any? { |b| s.include?(b) }
  false
end

# ====== WEBSITE (exact href from title="Visit website") ======
def website_from_page(doc, page_url)
  # Priority 1: exact title="Visit website"
  a = doc.at_css("a[title='Visit website'][href]")
  # Priority 1b: case-insensitive contains on @title / @aria-label
  a ||= doc.at_xpath("//a[@href and contains(translate(@title,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'visit website')]")
  a ||= doc.at_xpath("//a[@href and contains(translate(@aria-label,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'visit website')]")
  # Priority 2: visible text equals "Website"
  a ||= doc.at_xpath("//a[@href and normalize-space(translate(string(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'))='website']")

  return nil unless a
  absolute(page_url, a['href'].to_s) # return the exact href (fbclid/utm preserved)
end

# ====== LOCATION (visible address text only) ======
LABEL_WORDS    = ['Website','Facebook','Instagram','Twitter','X','Email','View Map','Contact']
LABEL_WORDS_RE = Regexp.union(LABEL_WORDS.map { |w| /#{Regexp.escape(w)}/i })

def text_only_from(node)
  return nil unless node
  frag = Nokogiri::HTML.fragment(node.to_html)
  frag.css('script,style,noscript').remove
  frag.css('a').each { |a| a.replace(a.text) } # keep visible text, drop hrefs
  txt = frag.xpath(".//text()[normalize-space(.)!='']").map(&:text).join(" ")
  txt = txt.gsub(LABEL_WORDS_RE, ' ')
  txt = txt.gsub(/\s+/, ' ').strip
  txt.empty? ? nil : txt
end

def location_from_page(doc)
  node = find_label_node(doc, 'Location')
  cont = small_container(node)

  candidates = []
  if cont
    candidates << text_only_from(cont)
  end

  # Fallback: sometimes address is under Contact
  node2 = find_label_node(doc, 'Contact', 'Contact Email')
  cont2 = small_container(node2)
  candidates << text_only_from(cont2) if cont2

  candidates.compact!
  # remove phone-like strings
  candidates.map! { |t| t.gsub(/\+?\d[\d\-\s().]{6,}\d/, '').gsub(/\s+/, ' ').strip }
  pick = candidates.find { |t| t =~ /\d/ && t =~ /(st|street|ave|avenue|rd|road|blvd|square|plaza|drive|dr|suite|#|zip|postal|m[0-9][a-z]\s?[0-9][a-z][0-9]|canada|usa|uk|city|toronto)\b/i }
  pick ||= candidates.find { |t| t.length.between?(10, 200) }
  return nil if pick && pick.length > 220
  pick
end

# ====== PHONE ======
def phone_from_page(doc)
  node = find_label_node(doc, 'Phone', 'Contact', 'Contact Email')
  cont = small_container(node)
  return nil unless cont
  if (t = cont.at_css("a[href^='tel:']"))
    num = t.text.strip
    return num unless looks_like_noise?(num)
  end
  if (m = cont.text.match(/(\+?\d[\d\-\s().]{6,}\d)/))
    num = m[1].strip
    return num unless looks_like_noise?(num)
  end
  nil
end

# ====== Scrape one festival page ======
def scrape_festival(url)
  html = http_get(url, referer: "https://filmfreeway.com/festivals")
  doc  = Nokogiri::HTML(html)

  name = doc.at('h1')&.text&.strip
  name ||= doc.at('title')&.text&.strip

  website  = website_from_page(doc, url)   # exact href from "Visit website"
  location = location_from_page(doc)       # address text only
  phone    = phone_from_page(doc)

  website  = nil if looks_like_noise?(website)
  location = nil if looks_like_noise?(location)
  phone    = nil if looks_like_noise?(phone)

  upsert_row({
    source_url: url,
    name: name,
    website: website,
    location: location,
    phone: phone
  })
end

# ====== Run ======
SEEDS = [
  # Example: Commffest
  "https://filmfreeway.com/CommffestGlobalCommunityFilmFestival",
  # Example you mentioned with title="Visit website"
  "https://filmfreeway.com/InternationalMediaFestivalOfWales"
]

SEEDS.each do |u|
  begin
    scrape_festival(u)
    puts "OK #{u}"
    sleep 1
  rescue => e
    warn "FAIL #{u} -> #{e}"
  end
end

puts "done"
