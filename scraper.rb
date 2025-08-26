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
def ensure_table!
  d = SQLite3::Database.new(DB_FILE)
  d.execute <<-SQL
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
  d
end

def db
  @db ||= ensure_table!
end

# Upsert using only existing columns
def upsert_row(row_hash)
  cols_existing = db.execute("PRAGMA table_info(festivals)").map { |r| r[1] }
  desired = %w[source_url name website email director location phone]
  cols = desired & cols_existing
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

def mask_key(u); u.to_s.sub(/api_key=[^&]+/, 'api_key=***'); end

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

# Try no-render first (faster), then render (JS)
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
  URI.join(base, href).to_s rescue href
end

# <a> by visible text (case-insensitive). Exact match first; then contains.
def at_link_by_text(doc, label)
  down = "translate(normalize-space(string(.)),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz')"
  doc.at_xpath("//a[@href and #{down}='#{label.downcase}']") ||
    doc.at_xpath("//a[@href and contains(#{down},'#{label.downcase}')]")
end

# Value next to/under a label (dt->dd, <b>Label</b> Value, or next sibling)
def value_next_to_label(doc, label)
  down = "translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz')"
  n = doc.at_xpath("//dt[#{down}='#{label.downcase}']/following-sibling::*[1]")
  return n.text.strip if n

  n = doc.at_xpath("//*[self::strong or self::b][#{down}='#{label.downcase}']/following-sibling::text()[1]")
  return n.text.strip if n && n.text

  lbl = doc.at_xpath("//*[self::div or self::span or self::p or self::li][#{down}='#{label.downcase}']")
  if lbl
    sib = lbl.xpath("following-sibling::*[1]").first
    return sib.text.strip if sib
  end
  nil
end

# Extract URL/email from JS-y attributes when href is a placeholder
def extract_url_from_attrs(node)
  if node['onclick']
    return "mailto:#{$1}" if node['onclick'] =~ /mailto:([^\s'")<>]+)/i
    return $& if node['onclick'] =~ /https?:\/\/[^\s'"]+/i
  end
  %w[
    data-clipboard-text data-email data-address data-mail data-mailto data-text data-value
    data-copy data-copy-text data-contact data-user data-domain data-href data-url
  ].each do |attr|
    v = node[attr]
    next unless v && !v.empty?
    return "mailto:#{v}" if v =~ /\A[^@\s]+@[^@\s]+\.[^@\s]+\z/
    return v if v =~ /\Ahttps?:\/\//i
  end
  nil
end

def website_from_label(doc, base_url)
  a = at_link_by_text(doc, 'website')
  return nil unless a
  href = extract_url_from_attrs(a) || a['href'].to_s
  full = absolute(base_url, href)
  return nil unless full && full.start_with?('http')
  return nil if full.include?('filmfreeway.com') || SOCIAL.any? { |s| full.include?(s) }
  full
end

def email_from_label(doc, page_url)
  a = at_link_by_text(doc, 'email')
  return nil unless a

  collect_candidates = lambda do |node|
    vals = []
    vals << node['href'] if node['href']
    vals << node['onclick'] if node['onclick']
    %w[
      data-clipboard-text data-email data-address data-mail data-mailto data-text data-value
      data-copy data-copy-text data-contact data-user data-domain data-href data-url
    ].each { |k| v = node[k]; vals << v if v && !v.empty? }
    node.xpath(".//*[@*]").each { |n| n.attribute_nodes.each { |att| vals << att.value if att.value && !att.value.empty? } }
    node.xpath(".. | ../..").each { |n| n.attribute_nodes.each { |att| vals << att.value if att.value && !att.value.empty? } }
    vals.compact.uniq
  end

  parse_email = lambda do |strings|
    strings.each do |s|
      return $1.strip if s =~ /mailto:([^\s'")<>]+)/i
      if (m = s[/[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}/i])
        return m.strip
      end
    end
    nil
  end

  # 1) as-is
  email = parse_email.call(collect_candidates.call(a))

  # 2) if it's the JS decoy (#ff_javascript) or still empty, force-render and retry
  if (!email || email.empty?) && a['href'].to_s.include?('#ff_javascript')
    html2 = http_get_rendered(page_url)
    doc2  = Nokogiri::HTML(html2)
    a2    = at_link_by_text(doc2, 'email')
    email = parse_email.call(collect_candidates.call(a2)) if a2
    if (!email || email.empty?) && a2
      block_html = (a2.ancestors("section,div,li,dl").first || doc2).to_html
      email = block_html[/mailto:([^\s'"<>]+)/i, 1] ||
              block_html[/[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}/i]
    end
  end

  email
end

def location_from_label(doc)
  val = value_next_to_label(doc, 'location')
  return val unless val.nil? || val.empty?

  # tiny fallback: JSON-LD address text (never a URL)
  doc.css('script[type="application/ld+json"]').each do |s|
    begin
      j = JSON.parse(s.text)
      arr = j.is_a?(Array) ? j : [j]
      arr.each do |obj|
        loc = obj['location'] || obj['address']
        if loc.is_a?(String)
          return loc.strip
        elsif loc.is_a?(Hash)
          parts = [loc['name'], loc['streetAddress'], loc['addressLocality'], loc['addressRegion'], loc['postalCode'], loc['addressCountry']].compact
          return parts.join(', ') unless parts.empty?
        end
      end
    rescue
      next
    end
  end
  nil
end

def phone_from_label(doc)
  val = value_next_to_label(doc, 'phone')
  return val unless val.nil? || val.empty?
  tel = doc.at_css("a[href^='tel:']")
  return tel.text.strip if tel
  if (m = doc.text.match(/(\+?\d[\d\-\s().]{6,}\d)/))
    return m[1]
  end
  nil
end

def director_guess(doc)
  doc.xpath("//*[contains(translate(.,'DIRECTOR','director'),'director')]")
     .map { _1.text.strip }.find { |t| t =~ /director/i }
end

# ====== Scrape one festival page ======
def scrape_festival(url)
  html = http_get(url, referer: "https://filmfreeway.com/festivals")
  doc  = Nokogiri::HTML(html)

  name = doc.at('h1')&.text&.strip
  name ||= doc.at('title')&.text&.strip

  website  = website_from_label(doc, url)   # href of "Website"
  email    = email_from_label(doc, url)     # email from "Email" (handles JS)
  location = location_from_label(doc)       # visible text only
  phone    = phone_from_label(doc)          # visible text only
  director = director_guess(doc)            # best-effort text

  upsert_row({
    source_url: url,
    name: name,
    website: website,
    email: email,
    director: director,
    location: location,
    phone: phone
  })
end

# ====== Run (replace SEEDS with your list) ======
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
