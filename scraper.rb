# --- stdlib / gems ---
require 'net/http'
require 'uri'
require 'zlib'
require 'stringio'
require 'nokogiri'
require 'sqlite3'

DB_FILE = 'data.sqlite'

# ========= DB helpers =========
def db
  @db ||= begin
    d = SQLite3::Database.new(DB_FILE)
    d.execute <<-SQL
      CREATE TABLE IF NOT EXISTS festivals (
        source_url TEXT PRIMARY KEY,
        name       TEXT,
        website    TEXT,
        email      TEXT,
        director   TEXT,
        location   TEXT,
        scraped_at INTEGER
      );
    SQL
    d
  end
end

def save_row(row)
  db.execute <<-SQL, row.values_at(:source_url,:name,:website,:email,:director,:location,:scraped_at)
    INSERT OR REPLACE INTO festivals
    (source_url,name,website,email,director,location,scraped_at)
    VALUES (?,?,?,?,?,?,?)
  SQL
end

# ========= Proxy (ScraperAPI) =========
# Prefer reading the key from morph.io Secrets (recommended):
#   Key: SCRAPERAPI_KEY   Value: 6285820b983cbb8559ec8cb5513492ee
# If the env var is missing, we fall back to the hardcoded key you provided.
SCRAPERAPI_KEY = (ENV['SCRAPERAPI_KEY'] && !ENV['SCRAPERAPI_KEY'].empty?) ? ENV['SCRAPERAPI_KEY'] : '6285820b983cbb8559ec8cb5513492ee'

def render_wrap(url)
  return nil if SCRAPERAPI_KEY.to_s.empty?
  # ScraperAPI with JS rendering enabled
  "https://api.scraperapi.com/?api_key=#{SCRAPERAPI_KEY}&render=true&url=#{URI.encode_www_form_component(url)}"
end

# ========= HTTP helpers (PREFERS PROXY) =========

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
OPEN_TIMEOUT = 8
READ_TIMEOUT = 15
PER_REQ_CAP  = 25  # hard cap per request (seconds)

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

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# http_get prefers the proxy: first lines choose the proxy URL if configured
def http_get(url, referer: "https://filmfreeway.com/")
  target = render_wrap(url) || url       # <-- prefer proxy if available
  referer = "https://filmfreeway.com/"   # neutral referer when using proxy
  uri   = URI(target)
  start = Time.now

  req = Net::HTTP::Get.new(uri)
  req['User-Agent']        = UA
  req['Accept']            = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8'
  req['Accept-Language']   = 'en-US,en;q=0.9'
  req['Accept-Encoding']   = 'gzip, deflate'
  req['Upgrade-Insecure-Requests'] = '1'
  req['Referer']           = referer
  req['Connection']        = 'keep-alive'

  res = nil
  Net::HTTP.start(uri.host, uri.port, use_ssl: uri.scheme == 'https') do |h|
    h.open_timeout = OPEN_TIMEOUT
    h.read_timeout = READ_TIMEOUT
    # hard-cap the whole request
    deadline = Time.now + PER_REQ_CAP
    begin
      res = h.request(req)
    rescue Net::ReadTimeout, Net::OpenTimeout
      raise "Timeout after #{PER_REQ_CAP}s for #{url}"
    end
  end

  code = res.code.to_i
  raise "HTTP #{code}" if code >= 400

  body = decompress(res.body, res['content-encoding'])
  body
end
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

# ========= Parsing helpers =========
def absolute(base, href)
  return nil if href.nil? || href.empty?
  URI.join(base, href).to_s rescue href
end

def external_site_from(doc, base_url)
  doc.css('a[href]').each do |a|
    text = a.text.to_s.downcase
    href = a['href'].to_s
    next if href.empty?
    full = absolute(base_url, href)
    next unless full&.start_with?('http')
    next if full.include?('filmfreeway.com')
    next if %w[facebook.com twitter.com instagram.com youtube.com tiktok.com linkedin.com].any? { |s| full.include?(s) }
    return full if text.include?('website') || text.include?('official')
  end
  nil
end

def find_email_in(text)
  text[/[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}/i]
end

def first_email_on(url)
  return nil unless url
  html = http_get(url, referer: "https://filmfreeway.com/")
  doc  = Nokogiri::HTML(html)

  # direct mailto on homepage?
  mail = doc.at_css("a[href^='mailto:']")
  return mail['href'].sub('mailto:','') if mail

  # try a couple of likely contact/about pages
  links = doc.css('a[href]').map { |a| absolute(url, a['href'].to_s) }.compact.uniq
  contactish = links.select { |u| u && u.downcase.match(/contact|about|team|imprint|kontakt/) }
  contactish.first(2).each do |cl|
    begin
      em = find_email_in(http_get(cl, referer: url))
      return em if em
    rescue
      next
    end
  end

  # last chance: regex on homepage
  find_email_in(html)
end

# ========= Festival scraping =========
def scrape_festival(url)
  html = http_get(url, referer: "https://filmfreeway.com/festivals")
  doc  = Nokogiri::HTML(html)

  name = doc.at('h1')&.text&.strip || doc.at('title')&.text&.strip
  website = external_site_from(doc, url)

  director = doc.xpath("//*[contains(translate(.,'DIRECTOR','director'),'director')]")
                .map { _1.text.strip }.find { |t| t =~ /director/i }
  location = doc.xpath("//*[contains(translate(.,'LOCATION','location'),'location')]")
                .map { _1.text.strip }.find { |t| t =~ /location/i }

  email = doc.at_css("a[href^='mailto:']")&.[]('href')&.sub('mailto:','')
  email ||= first_email_on(website) if website

  save_row({
    source_url: url,
    name: name,
    website: website,
    email: email,
    director: director,
    location: location,
    scraped_at: Time.now.to_i
  })
end

# ========= Run (seed one page; replace with your list later) =========
SEEDS = [
  "https://filmfreeway.com/CommffestGlobalCommunityFilmFestival"
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
