require 'net/http'
require 'uri'
require 'nokogiri'
require 'sqlite3'

DB_FILE = 'data.sqlite'

# ---------------- DB helpers ----------------
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

# ---------------- HTTP with headers/cookies ----------------
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
$cookie = nil

def http_get(url, referer: "https://filmfreeway.com/")
  uri = URI(url)
  tries = 0
  loop do
    req = Net::HTTP::Get.new(uri)
    req['User-Agent']      = UA
    req['Accept']          = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    req['Accept-Language'] = 'en-US,en;q=0.9'
    req['Referer']         = referer
    req['Connection']      = 'keep-alive'
    req['Cookie']          = $cookie if $cookie

    res = Net::HTTP.start(uri.host, uri.port, use_ssl: uri.scheme == 'https') { |h| h.request(req) }

    # collect cookies
    if res.get_fields('set-cookie')
      fresh = res.get_fields('set-cookie').map { |c| c.split(';').first }.join('; ')
      $cookie = [$cookie, fresh].compact.join('; ')
    end

    case res
    when Net::HTTPRedirection
      uri = URI.join(uri, res['location'])
      referer = url
      next
    else
      code = res.code.to_i
      if code == 403 && tries == 0
        # warm up a session by visiting listings, then retry once
        http_get('https://filmfreeway.com/festivals', referer: 'https://filmfreeway.com/')
        tries += 1
        sleep 1
        next
      end
      raise "HTTP #{code}" if code >= 400
      return res.body
    end
  rescue => _
    tries += 1
    raise if tries > 3
    sleep(1 * tries)
  end
end

# ---------------- Parsing helpers ----------------
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
  mail = doc.at_css("a[href^='mailto:']")
  return mail['href'].sub('mailto:','') if mail

  links = doc.css('a[href]').map { |a| absolute(url, a['href'].to_s) }.compact.uniq
  contactish = links.select { |u| u && u.downcase.match(/contact|about|team|imprint|kontakt/) }
  contactish.first(5).each do |cl|
    begin
      em = find_email_in(http_get(cl, referer: url))
      return em if em
    rescue
      next
    end
  end
  find_email_in(html)
end

# ---------------- Festival scraping ----------------
def scrape_festival(url)
  # FilmFreeway page: use FilmFreeway referer
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

# ---- Try one page (swap in your list later) ----
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
