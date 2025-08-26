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

  # Drop any extra columns (e.g., scraped_at)
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

# ====== ScraperAPI HTTP ======
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

def http_get(url, referer: "https://filmfreeway.com/")
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
  timed_request(scraperapi_url(url, mode: :render), referer: "https://filmfreeway.com/")
end

# ====== DOM helpers ======
SOCIAL = %w[facebook.com twitter.com instagram.com youtube.com tiktok.com linkedin.com linktr.ee]
BLOCKY_CLASSES = /Modal|modal|Overlay|Dialog|Signup|Login|StrongPassword|cookie/i

def absolute(base, href)
  return nil if href.nil? || href.empty?
  URI.join(base, href).to_s
rescue
  href
end

def small_container(node)
  return nil unless node
  # prefer dd/li/div/section nearby instead of large ancestors
  node.ancestors('dd').first ||
    node.ancestors('li').first ||
    node.ancestors('div').find { |d| (d['class'].to_s !~ BLOCKY_CLASSES) && d.inner_html.size < 5000 } ||
    node.ancestors('section').find { |s| s.inner_html.size < 5000 } ||
    node
end

def find_label_node(doc, *labels)
  labels.each do |label|
    down = "translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz')"
    # dt text
    n = doc.at_xpath("//dt[#{down}='#{label.downcase}']")
    return n if n
    # strong/b label
    n = doc.at_xpath("//*[self::strong or self::b][#{down}='#{label.downcase}']")
    return n if n
    # exact text in span/div/li/p
    n = doc.at_xpath("//*[self::div or self::span or self::p or self::li][#{down}='#{label.downcase}']")
    return n if n
    # contains
    n = doc.at_xpath("//*[contains(#{down},'#{label.downcase}')]")
    return n if n
  end
  nil
end

def looks_like_noise?(s)
  return true if s.nil? || s.empty?
  return true if s.size > 1000
  return true if s.include?('{"user_signed_in"') || s.include?('fonts_to_prefetch') || s.include?('ModalLogin') || s.include?('StrongPassword')
  false
end

def sanitize_website(u)
  return nil unless u && u.start_with?('http')
  host = URI(u).host rescue nil
  return nil unless host
  bad = (%w[filmfreeway.com static-assets.filmfreeway.com fonts.gstatic.com] + SOCIAL)
  return nil if bad.any? { |b| host.include?(b) }
  u
end

# ====== Field extractors ======
def website_from_page(doc, page_url)
  node = find_label_node(doc, 'Website')
  cont = small_container(node)
  return nil unless cont

  # First, any anchor *in the same container* that is not social/filmfreeway
  cont.css('a[href]').each do |a|
    next if a.text.strip =~ /\A(Facebook|Instagram|Twitter|X|YouTube|LinkedIn)\z/i
    href = a['href'].to_s
    href = absolute(page_url, href)
    href = sanitize_website(href)
    return href if href
  end

  # Fallback: any <a> whose visible text is exactly "Website"
  a = doc.at_xpath("//a[normalize-space(text())='Website' or contains(normalize-space(.),'Website')][@href]")
  if a
    href = absolute(page_url, a['href'].to_s)
    href = sanitize_website(href)
    return href if href
  end

  nil
end

def collect_attr_candidates(node)
  vals = []
  node.traverse do |n|
    n.attribute_nodes.each { |att| vals << att.value if att.value && !att.value.empty? }
  end
  node.xpath(".. | ../..").each do |n|
    n.attribute_nodes.each { |att| vals << att.value if att.value && !att.value.empty? }
  end
  vals.compact.uniq
end

def parse_email_from(strings)
  strings.each do |s|
    if s =~ /mailto:([^\s'")<>]+)/i
      return $1.strip
    end
    m = s[/[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}/i]
    return m.strip if m
  end
  nil
end

def email_from_page(doc, page_url)
  node = find_label_node(doc, 'Email', 'Contact Email', 'Contact')
  cont = small_container(node)
  return nil unless cont

  # 1) direct mailto in the block
  if (m = cont.at_css("a[href^='mailto:']"))
    return m['href'].sub(/^mailto:/i,'').strip
  end

  # 2) data-* / onclick hints in the block
  cand = parse_email_from(collect_attr_candidates(cont))
  return cand if cand

  # 3) JS decoy link like #ff_javascript → render and re-scan same block
  if (a = cont.at_css('a[href]')) && a['href'].to_s.include?('#ff_javascript')
    html2 = http_get_rendered(page_url)
    doc2  = Nokogiri::HTML(html2)
    node2 = find_label_node(doc2, 'Email', 'Contact Email', 'Contact')
    cont2 = small_container(node2)
    if cont2
      if (m2 = cont2.at_css("a[href^='mailto:']"))
        return m2['href'].sub(/^mailto:/i,'').strip
      end
      cand2 = parse_email_from(collect_attr_candidates(cont2))
      return cand2 if cand2
      if (m3 = cont2.text.match(/[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}/i))
        return m3[0]
      end
    end
  end

  nil
end

def location_from_page(doc)
  # Prefer an explicit “Location” label
  node = find_label_node(doc, 'Location')
  cont = small_container(node)
  if cont
    txt = cont.text.strip.gsub(/Location\s*[:\-–—]?\s*/i, '').gsub(/\s+/, ' ')
    return txt unless looks_like_noise?(txt)
  end

  # Fallback: some pages put address under “Contact” block
  node2 = find_label_node(doc, 'Contact', 'Contact Email')
  cont2 = small_container(node2)
  if cont2
    txt = cont2.text.strip
    # Heuristics: keep the segment that looks like an address (city, region, postal, country)
    # Canadian postal code pattern
    if (m = txt.match(/([A-Za-z .'\-]+,\s*[A-Za-z .'\-]+(?:\s+[A-Z]{2,3})?)\s+(?:[A-Z]\d[A-Z]\s?\d[A-Z]\d)\s*(Canada|United States|USA|United Kingdom|UK)?/))
      loc = [m[1], m[2]].compact.join(' ')
      loc = loc.gsub(/\s+/, ' ').strip
      return loc unless looks_like_noise?(loc)
    end
    # Simple "City, Region Country" fallback
    if (m2 = txt.match(/([A-Za-z .'\-]+,\s*[A-Za-z .'\-]+(?:,\s*[A-Za-z .'\-]+)?)/))
      loc2 = m2[1].gsub(/\s+/, ' ').strip
      return loc2 unless looks_like_noise?(loc2)
    end
  end

  nil
end

def phone_from_page(doc)
  node = find_label_node(doc, 'Phone', 'Contact', 'Contact Email')
  cont = small_container(node)
  return nil unless cont
  # visible text (not href)
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

def director_guess(doc)
  doc.xpath("//*[contains(translate(.,'DIRECTOR','director'),'director')]")
     .map { |n| n.text.strip }
     .find { |t| t =~ /director/i }
end

# ====== Scrape one festival page ======
def scrape_festival(url)
  html = http_get(url, referer: "https://filmfreeway.com/festivals")
  doc  = Nokogiri::HTML(html)

  name = doc.at('h1')&.text&.strip
  name ||= doc.at('title')&.text&.strip

  website  = website_from_page(doc, url)
  email    = email_from_page(doc, url)
  location = location_from_page(doc)
  phone    = phone_from_page(doc)
  director = director_guess(doc)

  # Final sanity
  website  = nil if looks_like_noise?(website) || !website.to_s.start_with?('http')
  email    = nil if looks_like_noise?(email)   || !(email.to_s.include?('@'))
  location = nil if looks_like_noise?(location)
  phone    = nil if looks_like_noise?(phone)

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
