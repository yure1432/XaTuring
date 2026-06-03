import sqlite3
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
DB_PATH = os.path.expanduser(os.getenv("DB_PATH", "memory.db"))


def table_exists(cur, name):
    return cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def columns(cur, table):
    return [row[1] for row in cur.execute(f"PRAGMA table_info({table})")]


def to_epoch(s):
    """Old timestamps were datetime.isoformat() strings; the new schema stores
    unix-epoch integers. Robust to None and to already-numeric values."""
    if not s:
        return None
    try:
        return int(datetime.fromisoformat(s).timestamp())
    except (ValueError, TypeError):
        try:
            return int(float(s))
        except (ValueError, TypeError):
            return None


def ensure_schema(cur):
    """Create the interned schema if absent; idempotent. URL text lives in exactly
    one place (urls); everything else references it by integer id."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS urls (
            id  INTEGER PRIMARY KEY,
            url TEXT UNIQUE NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pages (
            url_id     INTEGER PRIMARY KEY,
            title      TEXT,
            status     INTEGER,
            host       TEXT,
            fetched_at INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS links (
            from_id INTEGER,
            to_id   INTEGER,
            PRIMARY KEY (from_id, to_id)
        ) WITHOUT ROWID
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS frontier (
            url_id      INTEGER PRIMARY KEY,
            host        TEXT,
            added_at    INTEGER,
            status      TEXT DEFAULT 'pending',
            retry_count INTEGER DEFAULT 0
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hosts (
            host        TEXT PRIMARY KEY,
            new_links   INTEGER DEFAULT 0,
            total_links INTEGER DEFAULT 0
        )
    """)
    # the composite (status, host) serves the claim step, has_pending, and the
    # claimed-reset; standalone status / host indexes are redundant prefixes of it.
    cur.execute("CREATE INDEX IF NOT EXISTS idx_frontier_status_host ON frontier(status, host)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pages_host ON pages(host)")
    cur.execute("DROP INDEX IF EXISTS idx_frontier_status")
    cur.execute("DROP INDEX IF EXISTS idx_frontier_host")


def migrate_to_interned(con, cur):
    """Rebuild a legacy string-keyed database into the interned schema, preserving
    all accumulated memory. Ends by VACUUMing, which both applies auto_vacuum and
    reclaims the old bloat — so the migration itself shrinks the file."""
    print("Old schema detected — migrating to interned URLs (preserving memory)...")
    con.create_function("to_epoch", 1, to_epoch)

    # clean any half-finished migration from a previous interrupted run
    for t in ("pages_new", "links_new", "frontier_new"):
        cur.execute(f"DROP TABLE IF EXISTS {t}")

    # 1. one row per distinct URL anywhere in the old database
    cur.execute("CREATE TABLE IF NOT EXISTS urls (id INTEGER PRIMARY KEY, url TEXT UNIQUE NOT NULL)")
    cur.execute("""
        INSERT OR IGNORE INTO urls(url)
        SELECT url FROM pages    WHERE url IS NOT NULL
        UNION SELECT url      FROM frontier WHERE url IS NOT NULL
        UNION SELECT from_url FROM links    WHERE from_url IS NOT NULL
        UNION SELECT to_url   FROM links    WHERE to_url IS NOT NULL
    """)

    # 2. rebuild each table id-referenced; resolve ids via join, timestamps via to_epoch
    cur.execute("CREATE TABLE pages_new (url_id INTEGER PRIMARY KEY, title TEXT, status INTEGER, host TEXT, fetched_at INTEGER)")
    cur.execute("""
        INSERT OR IGNORE INTO pages_new (url_id, title, status, host, fetched_at)
        SELECT u.id, p.title, p.status, p.host, to_epoch(p.fetched_at)
        FROM pages p JOIN urls u ON u.url = p.url
    """)

    cur.execute("CREATE TABLE links_new (from_id INTEGER, to_id INTEGER, PRIMARY KEY(from_id, to_id)) WITHOUT ROWID")
    cur.execute("""
        INSERT OR IGNORE INTO links_new (from_id, to_id)
        SELECT uf.id, ut.id
        FROM links l JOIN urls uf ON uf.url = l.from_url
                     JOIN urls ut ON ut.url = l.to_url
    """)

    retry_expr = "COALESCE(f.retry_count, 0)" if "retry_count" in columns(cur, "frontier") else "0"
    cur.execute("CREATE TABLE frontier_new (url_id INTEGER PRIMARY KEY, host TEXT, added_at INTEGER, status TEXT DEFAULT 'pending', retry_count INTEGER DEFAULT 0)")
    cur.execute(f"""
        INSERT OR IGNORE INTO frontier_new (url_id, host, added_at, status, retry_count)
        SELECT u.id, f.host, to_epoch(f.added_at), f.status, {retry_expr}
        FROM frontier f JOIN urls u ON u.url = f.url
    """)

    # 3. swap new tables into place (hosts already has the right shape)
    for old, new in (("pages", "pages_new"), ("links", "links_new"), ("frontier", "frontier_new")):
        cur.execute(f"DROP TABLE {old}")
        cur.execute(f"ALTER TABLE {new} RENAME TO {old}")


con = sqlite3.connect(DB_PATH)
# auto_vacuum must be chosen before the database is first written to, so set it
# BEFORE journal_mode (which itself writes the header). Incremental auto-vacuum lets
# memory decay's deletions return space to the OS via PRAGMA incremental_vacuum.
# On a pre-existing DB this only takes effect at the migration VACUUM below.
con.execute("PRAGMA auto_vacuum=INCREMENTAL")
con.execute("PRAGMA journal_mode=WAL")
cur = con.cursor()

legacy = (
    table_exists(cur, "pages")
    and not table_exists(cur, "urls")
    and "url" in columns(cur, "pages")
)

if legacy:
    migrate_to_interned(con, cur)
    ensure_schema(cur)
    con.commit()
    con.execute("VACUUM")        # apply auto_vacuum + reclaim the old bloat
    print("Migration complete.")
else:
    ensure_schema(cur)
    con.commit()

con.close()
print("Database initialized.")
