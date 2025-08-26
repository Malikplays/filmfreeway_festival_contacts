require 'open-uri'
require 'nokogiri'
require 'sqlite3'
require 'uri'

DB_FILE = 'data.sqlite'

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

def fetch(url)
  # simple retries
  3.times do |i|
    begin
      return URI.open(url, "User-Agent" => "FylmTV-Festival-Scraper").read
    rescue => e
      sleep(1 + i)
      raise e if i == 2
    end
  end
end

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
    next unless full.start_with?('http')
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
  html = fetch(url)
  doc  = Nokogiri::HTML(html)
  mail = doc.at_css("a[href^='mailto:']")
  return mail['href'].sub('mailto:','') if mail

  # try common contact/about pages
  links = doc.css('a[href]').map { |a| absolute(url, a['href'].to_s) }.compact.uniq
  contactish = links.select { |u| u && u.downcase.match(/contact|about|team|imprint|kontakt/) }
  (contactish.first(5)).each do |cl|
    begin
      em = find_email_in(fetch(cl))
      return em if em
    rescue
      next
    end
  end
  find_email_in(html)
end

def scrape_festival(url)
  html = fetch(url)
  doc  = Nokogiri::HTML(html)

  name = doc.at('h1')&.text&.strip || doc.at('title')&.text&.strip
  website = external_site_from(doc, url)

  # best-effort director/location from visible text
  director = doc.xpath("//*[contains(translate(.,'DIRECTOR','director'),'director')]").map{_1.text.strip}.find{|t| t =~ /director/i}
  location = doc.xpath("//*[contains(translate(.,'LOCATION','location'),'location')]").map{_1.text.strip}.find{|t| t =~ /location/i}

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

# ---- run a few sample pages (swap in your list or a discovery step) ----
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
