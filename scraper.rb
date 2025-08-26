require 'net/http'
require 'uri'

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
$cookie = nil

def http_get(url, referer: "https://filmfreeway.com/")
  uri = URI(url)
  tries = 0
  begin
    req = Net::HTTP::Get.new(uri)
    req['User-Agent']      = UA
    req['Accept']          = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    req['Accept-Language'] = 'en-US,en;q=0.9'
    req['Referer']         = referer
    req['Connection']      = 'keep-alive'
    req['Cookie']          = $cookie if $cookie

    res = Net::HTTP.start(uri.host, uri.port, use_ssl: uri.scheme == 'https') { |h| h.request(req) }

    # Track cookies
    if res.get_fields('set-cookie')
      fresh = res.get_fields('set-cookie').map { |c| c.split(';').first }.join('; ')
      $cookie = [$cookie, fresh].compact.join('; ')
    end

    case res
    when Net::HTTPRedirection
      loc = URI.join(uri, res['location']).to_s
      return http_get(loc, referer: uri.to_s)
    else
      code = res.code.to_i
      if code == 403 && tries == 0
        # Warm up session/cookies, then retry once
        http_get('https://filmfreeway.com/festivals', referer: 'https://filmfreeway.com/')
        tries += 1
        sleep 1
        return http_get(url, referer: referer)
      end
      raise "HTTP #{code}" if code >= 400
      return res.body
    end
  rescue => e
    tries += 1
    raise e if tries > 3
    sleep(1 * tries)
    retry
  end
end
