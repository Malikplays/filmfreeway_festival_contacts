import time, sqlite3

# Create data.sqlite and a simple `data` table (morph.io shows this first)
conn = sqlite3.connect('data.sqlite')
cur = conn.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS data (
  name TEXT PRIMARY KEY,
  t INTEGER
)
""")
cur.execute("INSERT OR REPLACE INTO data(name, t) VALUES (?, ?)",
            ("ping", int(time.time())))
conn.commit()
conn.close()

print("ok: wrote row to data.sqlite")
