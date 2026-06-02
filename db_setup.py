import sqlite3
import os
from dotenv import load_dotenv

load_dotenv()
DB_PATH = os.path.expanduser(os.getenv("DB_PATH", "memory.db"))

con = sqlite3.connect(DB_PATH)
con.execute("PRAGMA journal_mode=WAL")
cur = con.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS pages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT UNIQUE,
        title TEXT,
        status INTEGER,
        host TEXT,
        fetched_at TEXT
    )
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS links (
        from_url TEXT,
        to_url TEXT,
        UNIQUE(from_url, to_url)
    )
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS frontier (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT UNIQUE,
        host TEXT,
        added_at TEXT,
        status TEXT DEFAULT 'pending'
    )
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS hosts (
        host TEXT PRIMARY KEY,
        new_links INTEGER DEFAULT 0,
        total_links INTEGER DEFAULT 0
    )
""")

cur.execute("CREATE INDEX IF NOT EXISTS idx_frontier_status ON frontier(status)")
cur.execute("CREATE INDEX IF NOT EXISTS idx_frontier_host ON frontier(host)")
cur.execute("CREATE INDEX IF NOT EXISTS idx_pages_host ON pages(host)")

con.commit()
con.close()
print("Database initialized.")

