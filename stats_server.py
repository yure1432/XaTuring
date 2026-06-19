"""XaTuring stats server — a tiny always-on HTTP endpoint that returns the
animal's live wake as JSON, for a Glance custom-api widget to render.

Queries the database fresh on every request (read-only, WAL mode lets it read
while the crawler writes). No graph layout, no heavy compute — just numbers.

Run as a systemd service alongside the crawler. Listens on STATS_PORT.
"""
import os
import json
import sqlite3
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from dotenv import load_dotenv

load_dotenv()
DB_PATH = os.path.expanduser(os.getenv("DB_PATH", "memory.db"))
STATS_PORT = int(os.getenv("STATS_PORT", "8900"))


def gather_stats():
    # read-only connection, fresh each request; WAL lets this read while the
    # crawler writes without blocking either side.
    con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    cur = con.cursor()

    def scalar(q, params=()):
        row = cur.execute(q, params).fetchone()
        return row[0] if row and row[0] is not None else 0

    pages = scalar("SELECT COUNT(*) FROM pages")
    links = scalar("SELECT COUNT(*) FROM links")
    urls = scalar("SELECT COUNT(*) FROM urls")
    hosts = scalar("SELECT COUNT(*) FROM hosts")

    # frontier state breakdown
    frontier = {}
    for status, count in cur.execute(
        "SELECT status, COUNT(*) FROM frontier GROUP BY status"
    ):
        frontier[status] = count

    # db file size on disk (MB)
    try:
        size_mb = round(os.path.getsize(DB_PATH) / (1024 * 1024), 1)
    except OSError:
        size_mb = 0

    # richest hosts (where the animal lingers) — needs enough data to be meaningful
    richest = []
    for host, nl, tl in cur.execute("""
        SELECT host, new_links, total_links FROM hosts
        WHERE total_links >= 5
        ORDER BY CAST(new_links AS REAL) / total_links DESC
        LIMIT 8
    """):
        richest.append({"host": host, "richness": round(nl / tl, 2)})

    # most recently crawled pages (what it's doing right now)
    recent = []
    for host, title, fetched_at in cur.execute("""
        SELECT host, title, fetched_at FROM pages
        ORDER BY fetched_at DESC LIMIT 8
    """):
        t = title if title and len(title) <= 50 else (title[:47] + "...") if title else "—"
        recent.append({"host": host, "title": t})

    con.close()

    return {
        "pages": pages,
        "links": links,
        "urls": urls,
        "hosts": hosts,
        "frontier": frontier,
        "size_mb": size_mb,
        "richest": richest,
        "recent": recent,
    }


class StatsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            data = gather_stats()
            body = json.dumps(data).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except Exception as e:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(json.dumps({"error": str(e)}).encode())

    def log_message(self, *args):
        pass    # silence per-request logging


if __name__ == "__main__":
    server = ThreadingHTTPServer(("0.0.0.0", STATS_PORT), StatsHandler)
    print(f"XaTuring stats server on :{STATS_PORT}")
    server.serve_forever()
