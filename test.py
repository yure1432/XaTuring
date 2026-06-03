"""The wake — a read-only snapshot of the animal's state.

Prints what XaTuring has seen and where it has lingered: raw counts, the frontier
broken down by status, and the gait made visible — the richest and thinnest hosts
by link novelty. Touches nothing; just looks.

    python test.py
"""
import os
import sqlite3
from dotenv import load_dotenv

load_dotenv()
DB_PATH = os.path.expanduser(os.getenv("DB_PATH", "memory.db"))

TOP_N = 10

con = sqlite3.connect(DB_PATH)
cur = con.cursor()


def scalar(sql, params=()):
    return cur.execute(sql, params).fetchone()[0]


print(f"\n  XaTuring — the wake   ({DB_PATH})")
print("  " + "─" * 48)

# --- raw counts + footprint ---
pages = scalar("SELECT COUNT(*) FROM pages")
links = scalar("SELECT COUNT(*) FROM links")
hosts = scalar("SELECT COUNT(*) FROM hosts")
urls = scalar("SELECT COUNT(*) FROM urls")
db_bytes = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
print(f"  pages visited : {pages:>10,}")
print(f"  links (edges) : {links:>10,}")
print(f"  urls known    : {urls:>10,}")
print(f"  hosts known   : {hosts:>10,}")
print(f"  memory size   : {db_bytes/1_048_576:>9.1f} MB")

# --- frontier by status ---
print("\n  frontier")
status_rows = cur.execute("""
    SELECT status, COUNT(*) FROM frontier GROUP BY status ORDER BY COUNT(*) DESC
""").fetchall()
if status_rows:
    for status, n in status_rows:
        print(f"    {status:<10} {n:>10,}")
else:
    print("    (empty)")

# --- the gait: richest and thinnest hosts by novelty ratio ---
# only hosts with some links seen carry a meaningful ratio.
richest = cur.execute("""
    SELECT host,
           CAST(new_links AS REAL) / total_links AS novelty,
           new_links, total_links
      FROM hosts
     WHERE total_links > 0
     ORDER BY novelty DESC, total_links DESC
     LIMIT ?
""", (TOP_N,)).fetchall()

thinnest = cur.execute("""
    SELECT host,
           CAST(new_links AS REAL) / total_links AS novelty,
           new_links, total_links
      FROM hosts
     WHERE total_links > 0
     ORDER BY novelty ASC, total_links DESC
     LIMIT ?
""", (TOP_N,)).fetchall()


def show_hosts(title, rows):
    print(f"\n  {title}")
    if not rows:
        print("    (none yet)")
        return
    for host, novelty, new, total in rows:
        print(f"    {novelty:6.0%}  {host[:40]:<40} ({new:,}/{total:,})")


show_hosts("richest water (most novel)", richest)
show_hosts("thinnest water (most exhausted)", thinnest)

# --- where it lingered: hosts with the most pages read ---
lingered = cur.execute("""
    SELECT host, COUNT(*) AS n FROM pages
     GROUP BY host ORDER BY n DESC LIMIT ?
""", (TOP_N,)).fetchall()
print("\n  where it lingered (pages read per host)")
if lingered:
    for host, n in lingered:
        print(f"    {n:>6,}  {host[:50]}")
else:
    print("    (nowhere yet)")

print()
con.close()
