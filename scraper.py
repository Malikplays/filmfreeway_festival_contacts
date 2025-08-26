import time, sqlite3

conn = sqlite3.connect('data.sqlite')
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS festivals (
  source_url TEXT PRIMARY KEY,
  name TEXT,
  website TEXT,
  email TEXT,
  director TEXT,
  location TEXT,
  scraped_at INTEGER
)
""")

row = (
  "https://filmfreeway.com/ExampleFestival",
  "Example Festival",
  "https://examplefest.org",
  "info@examplefest.org",
  "Jane Doe",
  "City, Country",
  int(time.time())
)

cur.execute("""
INSERT OR REPLACE INTO festivals
(source_url, name, website, email, director, location, scraped_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
""", row)

conn.commit()
conn.close()

print("Wrote 1 test row to data.sqlite")
