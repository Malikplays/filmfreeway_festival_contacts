# --- stdlib / gems ---
require 'net/http'
require 'uri'
require 'zlib'
require 'stringio'
require 'json'
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
        phone      TEXT,
        scraped_at INTEGER
      );
    SQL
    # migrate if an older run lacked 'phone'
    cols = d.execute("PRAGMA table_info(festivals)").map { |r| r[1] }
    d.execute("ALTER TABLE festivals ADD COLUMN phone TEXT") unless cols.include?('phone')
    d
  end
end

def save_row(row)
  db.execute <<-SQL, row.values_at(:source_url,:name,:website,:email,:director,:location,:phone,:scraped_at)
    INSERT OR REPLACE INTO festivals
    (source_url,name,website,email,director,location,phone,scraped_at)
    VALUES (?,?,?,?,?,?,?,?)
  SQL
end

# ========= Proxy (ScraperAPI) =========
SCRAPERAPI_KEY = (ENV['SCRAPERAPI_KEY'] && !ENV['SCRAPERAPI_KEY'].empty?) ? ENV['SCRAPERAPI_KEY'] : '6285820b983cbb8559ec8cb5513492ee'

def scraperapi_url(target_url, mode: :no_render, country: 'us', session: 1001)
  base = "https://api.scraperapi.com/"
  params = {
    "api_key"        => SCRAPERAPI_KEY,
    "url"            => target_url,
    "country_code"   => country,
    "session_number" => session
  }
  params["render"] = "true" if mode == :render
  uri = URI(base)
  uri.query = URI.encode_www_form(params)
  uri.to_s
end

# ========= HTTP helpers =========
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
OPEN_TIMEOUT = 15
READ_TIMEOUT = 30
PER_REQ_CAP  = (ENV['SCRAPER_TIMEOUT_SECS'] || "60").to_i

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

# Try ScraperAPI without render first (fast), then with render (robust).
def http_get(url, referer: "https://filmfreeway.com/")
  [:no_render, :render].each do |mode|
    target = scraperapi_url(url, mode: mode)
    STDERR.puts "GET #{url} via ScraperAPI (#{mode})"
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

  Net::HTTP.start(uri.host, uri.port, use_ssl: uri.scheme == 'https') do |h|
    h.open_timeout = OPEN_TIMEOUT
    h.read_timeout = READ_TIMEOUT
    res = h.request(req)
    raise "Timeout after #{PER_REQ_CAP}s for #{uri}" if (Time.now - start) > PER_REQ_CAP
  end

  code = res.code.to_i
  raise "HTTP #{code}" if code >= 400

  body = decompress(res.body, res['content-encoding'])
  elapsed = (Time.now - start).round(1)
  STDERR.puts "✓ #{uri} (#{elapsed}s)"
  body
end

# ========= Parsing helpers (label-precise) =========
def absolute(base, href)
  return nil if href.nil? || href.empty?
  URI.join(base, href).to_s rescue href
end

def at_link_by_text(doc, label)
  # exact text match first, then contains, both case-insensitive
  down = "translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz')"
  a = doc.at_xpath("//a[@href and #{down}='#{label.downcase}']")
  a ||= doc.at_xpath("//a[@href and contains(#{down},'#{label.downcase}')]")
  a
end

def value_next_to_label(doc, label)
  # 1) dt -> dd
  down = "translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz')"
  node = doc.at_xpath("//dt[#{down}='#{label.downcase}']/following-sibling::*[1]")
  return node.text.strip if node

  # 2) <strong>/<b>Label</strong> VALUE
  node = doc.at_xpath("//*[self::strong or self::b][#{down}='#{label.downcase}']/following-sibling::text()[1]")
  return node.text.strip if node && node.text

  # 3) generic: element whose text is exactly the label, take next sibling element
  label_node = doc.at_xpath("//*[self::div or self::span or self::p or self::li][#{down}='#{label.downcase}']")
  if label_node
    sib = label_node.xpath("following-sibling::*[1]").first
    return sib.text.strip if sib
  end

  nil
end

def website_from_label(doc, base_url)
  if (a = at_link_by_text(doc, 'website'))
    href = a['href'].to_s
    full = absolute(base_url, href)
    # only keep true external websites; do not keep socials/filmfreeway
    return full if full&.start_with?('http') &&
                   !full.include?('filmfreeway.com') &&
                   %w[facebook.com twitter.com instagram.com youtube.com tiktok.com linkedin.com linktr.ee].none? { |s| full.include?(s) }
  end
  nil
end

def email_from_label(doc)
  if (a = at_link_by_text(doc, 'email'))
    href = a['href'].to_s
    return href.sub(/^mailto:/i,'').strip unless href.empty?
  end
  # light fallback: any mailto on page
  mail = doc.at_css("a[href^='mailto:']")
  return mail['href'].sub(/^mailto:/i,'').strip if mail
  nil
end

def location_from_label(doc)
  # Must return the visible text, not any URL
  val = value_next_to_label(doc, 'location')
  return val unless val.nil? || val.empty?
  # fallback: JSON-LD location text if present
  jl = doc.css('script[type="application/ld+json"]').map { |s| JSON.parse(s.text) rescue nil }.compact
  jl = jl.is_a?(Array) ? jl.flatten : [jl]
  jl.each do |j|
    loc = j['location'] || j['address']
    if loc.is_a?(String)
      return loc.strip
    elsif loc.is_a?(Hash)
      parts = [loc['name'], loc['streetAddress'], loc['addressLocality'], loc['addressRegion'], loc['postalCode'], loc['addressCountry']].compact
      return parts.join(', ') unless parts.empty?
    end
  end
  nil
end

def phone_from_label(doc)
  # Must return the visible text (e.g., from a tel: link's text)
  val = value_next_to_label(doc, 'phone')
  return val unless val.nil? || val.empty?
  # fallback: visible text inside a tel link (still text, not href)
  tel = doc.at_css("a[href^='tel:']")
  return tel.text.strip if tel
  # fallback: regex on visible text
  m = doc.text.match(/(\+?\d[\d\-\s().]{6,}\d)/)
  m && m[1]
end

def director_guess(doc)
  doc.xpath("//*[contains(translate(.,'DIRECTOR','director'),'director')]")
     .map { _1.text.strip }.find { |t| t =~ /director/i }
end

# ========= Festival scraping =========
def scrape_festival(url)
  html = http_get(url, referer: "https://filmfreeway.com/festivals")
  doc  = Nokogiri::HTML(html)

  name     = doc.at('h1')&.text&.strip || doc.at('title')&.text&.strip
  website  = website_from_label(doc, url)         # URL of the "Website" link
  email    = email_from_label(doc)                # email from the "Email" link's href
  location = location_from_label(doc)             # visible text only
  phone    = phone_from_label(doc)                # visible text only
  director = director_guess(doc)                  # best-effort

  save_row({
    source_url: url,
    name: name,
    website: website,
    email: email,
    director: director,
    location: location,
    phone: phone,
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
