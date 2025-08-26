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
    # migrate if old table lacks 'phone'
    cols = d.execute("PRAGMA table_info(festivals)").map { |r| r[1] }
    unless cols.include?('phone')
      d.execute("ALTER TABLE festivals ADD COLUMN phone TEXT")
    end
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
PER_REQ_CAP  = (ENV['SCRAPER_TIMEOUT_SECS'] || "60").to_i  # hard cap per request

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
      # quick retry on throttling or timeout
      if e.to_s =~ /HTTP (429|5\d\d)/ || e.to_s =~ /Timeout/
        sleep 1.0
        STDERR.puts "Retrying #{mode} once…"
        begin
          return timed_request(target, referer: "https://filmfreeway.com/")
        rescue => e2
          STDERR.puts "… retry failed: #{e2}"
        end
      end
      # then fall through to next mode
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
    # Hard cap after the request returns (ScraperAPI does the heavy lifting)
    raise "Timeout after #{PER_REQ_CAP}s for #{uri}" if (Time.now - start) > PER_REQ_CAP
  end

  code = res.code.to_i
  raise "HTTP #{code}" if code >= 400

  body = decompress(res.body, res['content-encoding'])
  elapsed = (Time.now - start).round(1)
  STDERR.puts "✓ #{uri} (#{elapsed}s)"
  body
end

# ========= Parsing helpers =========
SOCIAL = %w[facebook.com twitter.com instagram.com youtube.com tiktok.com linkedin.com linktr.ee]

def absolute(base, href)
  return nil if href.nil? || href.empty?
  URI.join(base, href).to_s rescue href
end

# Prefer explicit "Website" / "Official" anchors, else first non-social external link on the page.
def external_site_from(doc, base_url)
  # Text-labeled website buttons/links
  a = doc.at_xpath("//a[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'website') or contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'official')][@href]")
  if a
    full = absolute(base_url, a['href'].to_s)
    return full if full&.start_with?('http') && !full.include?('filmfreeway.com') && SOCIAL.none? { |s| full.include?(s) }
  end
  # First sensible external link
  doc.css('a[href]').each do |link|
    href = link['href'].to_s
    next if href.empty?
    full = absolute(base_url, href)
    next unless full&.start_with?('http')
    next if full.include?('filmfreeway.com') || SOCIAL.any? { |s| full.include?(s) }
    return full
  end
  nil
end

def find_email_in(text)
  text[/[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}/i]
end

def find_phone_in(text)
  # Accepts +country, (), spaces, dashes; requires at least 7 digits total
  m = text.match(/(\+?\d[\d\-\s().]{6,}\d)/)
  m && m[1]
end

# JSON-LD (schema.org) often includes address/phone/url
def extract_jsonld(doc)
  blocks = []
  doc.css('script[type="application/ld+json"]').each do |s|
    begin
      j = JSON.parse(s.text)
      blocks.concat(Array(j))
    rescue
      next
    end
  end
  blocks
end

def location_from_jsonld(doc)
  extract_jsonld(doc).each do |j|
    # Look for Event/Organization with place/address
    loc = j['location'] || j['address']
    next unless loc
    # Various shapes: string, object, or nested
    if loc.is_a?(String)
      return loc.strip
    elsif loc.is_a?(Hash)
      parts = [loc['name'], loc['streetAddress'], loc['addressLocality'], loc['addressRegion'], loc['postalCode'], loc['addressCountry']].compact
      return parts.join(', ') unless parts.empty?
    elsif loc.is_a?(Array)
      loc.each do |o|
        next unless o.is_a?(Hash)
        parts = [o['name'], o['streetAddress'], o['addressLocality'], o['addressRegion'], o['postalCode'], o['addressCountry']].compact
        return parts.join(', ') unless parts.empty?
      end
    end
  end
  nil
end

def phone_from_jsonld(doc)
  extract_jsonld(doc).each do |j|
    tel = j['telephone'] || (j['contactPoint'] && j['contactPoint']['telephone'])
    return tel if tel && !tel.to_s.empty?
  end
  nil
end

def location_from_labels(doc)
  # Try label/value patterns like "Location", "Country", "City"
  candidates = doc.xpath("//*[self::li or self::div or self::span][contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'location')]")
                  .map { |n| n.text.strip }.uniq
  # Pick the shortest reasonably informative line
  candidates.sort_by!(&:length)
  candidates.find { |t| t.split.size <= 12 && t.downcase.include?('location') } ||
    candidates.first
end

# Fetch email/phone from external website (homepage or up to 2 contact-like pages)
MAX_CONTACT_PAGES = 2
def discover_contact_on_site(url)
  return [nil, nil] unless url
  html = http_get(url, referer: "https://filmfreeway.com/")
  doc  = Nokogiri::HTML(html)

  # direct mailto/phone on homepage
  mail = doc.at_css("a[href^='mailto:']")&.[]('href')&.sub('mailto:','')
  phone = find_phone_in(doc.text)

  return [mail, phone] if mail || phone

  links = doc.css('a[href]').map { |a| absolute(url, a['href'].to_s) }.compact.uniq
  contactish = links.select { |u| u && u.downcase.match(/contact|about|team|imprint|kontakt|legal|impressum/) }
  contactish.first(MAX_CONTACT_PAGES).each do |cl|
    begin
      txt = http_get(cl, referer: url)
      mail2 = find_email_in(txt)
      phone2 = find_phone_in(txt)
      return [mail2, phone2] if mail2 || phone2
    rescue
      next
    end
  end

  [find_email_in(html), find_phone_in(html)]
end

# ========= Festival scraping =========
def scrape_festival(url)
  html = http_get(url, referer: "https://filmfreeway.com/festivals")
  doc  = Nokogiri::HTML(html)

  # Name
  name = doc.at('h1')&.text&.strip || doc.at('title')&.text&.strip

  # External website
  website = external_site_from(doc, url)

  # Director (best-effort)
  director = doc.xpath("//*[contains(translate(.,'DIRECTOR','director'),'director')]")
                .map { _1.text.strip }.find { |t| t =~ /director/i }

  # Location & Phone: JSON-LD first, then visible labels
  location = location_from_jsonld(doc) || location_from_labels(doc)
  phone    = phone_from_jsonld(doc) || find_phone_in(doc.text)

  # Email: mailto on page, else try external site
  email = doc.at_css("a[href^='mailto:']")&.[]('href')&.sub('mailto:','')
  if (!email || email.empty?) || (!phone || phone.empty?)
    e2, p2 = discover_contact_on_site(website)
    email ||= e2
    phone ||= p2
  end

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
