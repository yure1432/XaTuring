import os
import signal
from dotenv import load_dotenv
from urllib.robotparser import RobotFileParser
from urllib.parse import urljoin, urlparse, urldefrag
from datetime import datetime
import sqlite3
import logging
import asyncio
import httpx
from bs4 import BeautifulSoup

load_dotenv()
DB_PATH = os.path.expanduser(os.getenv("DB_PATH", "memory.db"))
LOG_PATH = os.path.expanduser(os.getenv("LOG_PATH", "xaturing.log"))
SEEDS_PATH = os.path.expanduser(os.getenv("SEEDS_PATH", "seeds.txt"))

# tunables
BATCH_SIZE = 20            # urls claimed and fetched per loop
COMMIT_EVERY = 20          # writer commits after this many items
MAX_PAGES_PER_HOST = 500   # per-host crawl cap (trap defense)

con = sqlite3.connect(DB_PATH)
con.execute("PRAGMA journal_mode=WAL")
cur = con.cursor()

logging.basicConfig(
    filename=LOG_PATH,
    level=logging.ERROR,
    format='%(asctime)s %(message)s'
)

shutdown = False
def handle_sigterm(signum, frame):
    global shutdown
    shutdown = True

signal.signal(signal.SIGTERM, handle_sigterm)


def normalise_url(url):
    url = urldefrag(url)[0]
    parsed = urlparse(url)
    scheme = "https"                       # force https so http/https don't split a host
    host = parsed.netloc.lower()
    path = parsed.path.rstrip('/')
    normalised = scheme + "://" + host + path
    if parsed.query:
        normalised += "?" + parsed.query
    return normalised


def parse_response(response, base_url):
    soup = BeautifulSoup(response.text, "lxml")
    title_tag = soup.find('title')
    title = title_tag.get_text(strip=True) if title_tag else "No title"
    found_links = []
    for link in soup.find_all("a"):
        href = link.get("href")
        if href and isinstance(href, str):
            if href.startswith(("mailto:", "tel:", "javascript:")):
                continue
            full_url = urljoin(base_url, href)
            full_url = normalise_url(full_url)
            if full_url.startswith(("http://", "https://")):
                found_links.append(full_url)
    return title, found_links


robots_cache = {}

async def can_fetch(client, url, user_agent="XaTuring"):
    host = urlparse(url).netloc
    scheme = urlparse(url).scheme
    if host not in robots_cache:
        rp = RobotFileParser()
        try:
            r = await client.get(
                f"{scheme}://{host}/robots.txt",
                timeout=10,
                follow_redirects=True,
            )
            rp.parse(r.text.splitlines())
            robots_cache[host] = rp
        except Exception:
            robots_cache[host] = None
            return True
    rp = robots_cache[host]
    if rp is None:
        return True
    return rp.can_fetch(user_agent, url)


def host_at_cap(host):
    count = cur.execute(
        "SELECT COUNT(*) FROM pages WHERE host = ?", (host,)
    ).fetchone()[0]
    return count >= MAX_PAGES_PER_HOST


def claim_batch(number):
    rows = cur.execute("""
        SELECT f.url FROM frontier f
        LEFT JOIN hosts h ON f.host = h.host
        WHERE f.status = 'pending'
        ORDER BY
            CASE
                WHEN h.total_links > 0
                THEN CAST(h.new_links AS REAL) / h.total_links
                ELSE 0.5
            END DESC
        LIMIT ?
    """, (number,)).fetchall()
    urls = [row[0] for row in rows]
    for url in urls:
        cur.execute("UPDATE frontier SET status='claimed' WHERE url=?", (url,))
    con.commit()
    return urls


async def get_response(client, url):
    if not await can_fetch(client, url):
        logging.error(f"Blocked by robots.txt: {url}")
        return
    try:
        response = await client.get(url, timeout=10, follow_redirects=True)
        response.raise_for_status()
    except httpx.HTTPError as e:
        logging.error(f"Failed to fetch {url}: {e}")
        return
    content_type = response.headers.get("Content-Type", "")
    if "text/html" not in content_type:
        logging.error(f"Not HTML: {url}")
        return
    return response


async def fetcher(client, url, queue):
    host = urlparse(url).netloc
    if host_at_cap(host):
        cur.execute("UPDATE frontier SET status='visited' WHERE url=?", (url,))
        return
    response = await get_response(client, url)
    if response:
        title, found_links = parse_response(response, url)
        result = {
            "url": url,
            "title": title,
            "status": response.status_code,
            "found_links": found_links,
            "fetched_at": datetime.now().isoformat(),
        }
        await queue.put(result)
    else:
        cur.execute("UPDATE frontier SET status='pending' WHERE url=?", (url,))


async def writer(queue):
    items_since_commit = 0
    while True:
        item = await queue.get()
        if item is None:
            con.commit()
            break

        url = item["url"]
        title = item["title"]
        status = item["status"]
        found_links = item["found_links"]
        fetched_at = item["fetched_at"]
        host = urlparse(url).netloc

        cur.execute(
            "INSERT OR IGNORE INTO pages (url, title, status, host, fetched_at) VALUES (?, ?, ?, ?, ?)",
            (url, title, status, host, fetched_at)
        )

        new_count = 0
        total_count = 0
        for link in found_links:
            link_host = urlparse(link).netloc
            cur.execute(
                "INSERT OR IGNORE INTO links (from_url, to_url) VALUES (?, ?)",
                (url, link)
            )
            cur.execute(
                "INSERT OR IGNORE INTO frontier (url, host, added_at, status) VALUES (?, ?, ?, 'pending')",
                (link, link_host, datetime.now().isoformat())
            )
            total_count += 1
            if cur.rowcount == 1:
                new_count += 1

        cur.execute("""
            INSERT INTO hosts (host, new_links, total_links)
            VALUES (?, ?, ?)
            ON CONFLICT(host) DO UPDATE SET
                new_links = new_links + ?,
                total_links = total_links + ?
        """, (host, new_count, total_count, new_count, total_count))

        cur.execute(
            "UPDATE frontier SET status = 'visited' WHERE url = ?",
            (url,)
        )

        items_since_commit += 1
        if items_since_commit >= COMMIT_EVERY:
            con.commit()
            items_since_commit = 0

        queue.task_done()
        print(f"  ↳ {host} — {title}")


def has_pending():
    return cur.execute(
        "SELECT 1 FROM frontier WHERE status='pending' LIMIT 1"
    ).fetchone() is not None


def seed_from_file():
    """Seed the frontier from seeds.txt (one url per line, # for comments)."""
    if not os.path.exists(SEEDS_PATH):
        print(f"No seeds file at {SEEDS_PATH} and frontier is empty. Nothing to crawl.")
        return
    count = 0
    with open(SEEDS_PATH) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            seed = normalise_url(line)
            cur.execute(
                "INSERT OR IGNORE INTO frontier (url, host, added_at, status) VALUES (?, ?, ?, 'pending')",
                (seed, urlparse(seed).netloc, datetime.now().isoformat())
            )
            count += 1
    con.commit()
    print(f"Seeded {count} urls from {SEEDS_PATH}")


async def main():
    queue = asyncio.Queue(maxsize=100)

    # reset any URLs stranded as 'claimed' from a previous crash
    cur.execute("UPDATE frontier SET status='pending' WHERE status='claimed'")
    con.commit()

    # seed from file only if frontier has nothing pending (first run / fresh db)
    if not has_pending():
        seed_from_file()

    async with httpx.AsyncClient() as client:
        writer_task = asyncio.create_task(writer(queue))
        try:
            while not shutdown:
                batch = claim_batch(BATCH_SIZE)
                if not batch:
                    # frontier may only be momentarily empty (fetches in flight)
                    await asyncio.sleep(2)
                    if not has_pending():
                        break
                    continue
                async with asyncio.TaskGroup() as tg:
                    for url in batch:
                        tg.create_task(fetcher(client, url, queue))
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass
        finally:
            await queue.put(None)
            await writer_task

    con.commit()
    con.close()

asyncio.run(main())
