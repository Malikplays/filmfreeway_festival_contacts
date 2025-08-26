import time
import scraperwiki  # creates ./data.sqlite on save

row = {
    "source_url": "https://filmfreeway.com/ExampleFestival",
    "name": "Example Festival",
    "website": "https://examplefest.org",
    "email": "info@examplefest.org",
    "director": "Jane Doe",
    "location": "City, Country",
    "scraped_at": int(time.time()),
}

scraperwiki.sqlite.save(unique_keys=["source_url"], data=row)
print("Wrote 1 test row to data.sqlite")
