require 'scraperwiki'
require 'nokogiri'

ScraperWiki.save_sqlite(['name'], { name: 'ping', t: Time.now.to_i })
puts "ok: wrote row to data.sqlite"
