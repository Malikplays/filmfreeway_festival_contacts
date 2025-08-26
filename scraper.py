# scraper.py (root)
import time, sqlite3
conn = sqlite3.connect('data.sqlite')
c = conn.cursor()
c.execute("""CREATE TABLE IF NOT EXISTS data(name TEXT PRIMARY KEY, t INTEGER)""")
c.execute("INSERT OR REPLACE INTO data(name,t) VALUES(?,?)", ("ping", int(time.time())))
conn.commit(); conn.close()
print("ok: wrote row to data.sqlite")
