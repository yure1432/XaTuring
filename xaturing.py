import os
import time
import signal
from dotenv import load_dotenv
from urllib.robotparser import RobotFileParser
from urllib.parse import urljoin, urlparse, urldefrag
import sqlite3
import logging
import asyncio
import httpx
from bs4 import BeautifulSoup

load_dotenv()
DB_PATH = os.path.expanduser(os.getenv("DB_PATH", "memory.db"))
LOG_PATH = os.path.expanduser(os.getenv("LOG_PATH", "xaturing.log"))
SEEDS_PATH = os.path.expanduser(os.getenv("SEEDS_PATH", "seeds.txt"))

# how the animal announces itself — the same identity it checks robots against
USER_AGENT = "XaTuring/1.0 (+https://github.com/yure1432/XaTuring; a solitary digital animal)"

# tunables
BATCH_SIZE = 20            # urls claimed and fetched per loop
COMMIT_EVERY = 20          # writer commits after this many items
MAX_PAGES_PER_HOST = 500   # per-host crawl cap (trap defense)
MAX_RETRIES = 3            # transient failures retried this many times before giving up
MIN_HOST_DELAY = 1.0       # seconds the animal waits between touches of the same host

# memory decay — the animal forgets the oldest territory that also went dry,
# but keeps hubs (well-linked landmarks) regardless of age.
FORGET_AFTER_DAYS = 30        # territory untouched longer than this is a candidate
DRY_NOVELTY_THRESHOLD = 0.05  # lifetime new/total below this counts as dry water
HUB_MIN_INDEGREE = 5          # linked to by >= this many distinct hosts = hub (spared)
DECAY_EVERY_SECONDS = 6 * 3600

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

# per-host politeness: one request in flight per host, spaced out in time.
host_locks = {}        # host -> asyncio.Lock (held across a host's fetch)
host_last_hit = {}     # host -> loop.time() of the last request to that host


def host_delay(host):
    """How long to wait between touches of this host: at least MIN_HOST_DELAY,
    or the host's robots.txt Crawl-delay if it asks for more."""
    rp = robots_cache.get(host)
    if rp is not None:
        try:
            cd = rp.crawl_delay(USER_AGENT)
            if cd:
                return max(MIN_HOST_DELAY, float(cd))
        except Exception:
            pass
    return MIN_HOST_DELAY


async def can_fetch(client, url, user_agent=USER_AGENT):
    parsed = urlparse(url)
    host = parsed.netloc
    scheme = parsed.scheme
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
    # con.execute spins up its own cursor, so this read never shares state with
    # the writer's cursor.
    count = con.execute(
        "SELECT COUNT(*) FROM pages WHERE host = ?", (host,)
    ).fetchone()[0]
    return count >= MAX_PAGES_PER_HOST


def intern_urls(urls):
    """Ensure every URL has a row in the urls table; return {url: id}. URL text is
    stored exactly once here — everything else references the integer id."""
    unique = list(set(urls))
    if not unique:
        return {}
    cur.executemany("INSERT OR IGNORE INTO urls(url) VALUES (?)", [(u,) for u in unique])
    idmap = {}
    for i in range(0, len(unique), 800):          # stay under SQLite's variable limit
        chunk = unique[i:i + 800]
        placeholders = ",".join("?" * len(chunk))
        for url, uid in cur.execute(
            f"SELECT url, id FROM urls WHERE url IN ({placeholders})", chunk
        ):
            idmap[url] = uid
    return idmap


def claim_batch(number):
    # Phase 1 — rank the *hosts* that still hold pending water by novelty. There are
    # far fewer hosts than URLs, so ordering this small set is cheap; sorting the
    # whole pending frontier on a computed ratio is not. Unknown hosts keep the
    # neutral 0.5 default so the unexplored stays worth investigating.
    hosts = cur.execute("""
        SELECT f.host
        FROM (SELECT DISTINCT host FROM frontier WHERE status = 'pending') f
        LEFT JOIN hosts h ON f.host = h.host
        ORDER BY
            CASE
                WHEN h.total_links > 0
                THEN CAST(h.new_links AS REAL) / h.total_links
                ELSE 0.5
            END DESC
        LIMIT ?
    """, (number,)).fetchall()

    # Phase 2 — take one pending URL from each of the richest hosts. Spreading the
    # batch across distinct hosts lets the fetchers run in parallel under the
    # per-host politeness lock, instead of a single rich host serialising the batch.
    claimed = []   # (url, url_id) — the string is for fetching, the id for DB writes
    for (host,) in hosts:
        row = cur.execute(
            "SELECT u.url, f.url_id FROM frontier f JOIN urls u ON u.id = f.url_id "
            "WHERE f.status = 'pending' AND f.host = ? LIMIT 1",
            (host,)
        ).fetchone()
        if row:
            claimed.append((row[0], row[1]))

    if claimed:
        ids = [url_id for _, url_id in claimed]
        placeholders = ",".join("?" * len(ids))
        cur.execute(
            f"UPDATE frontier SET status = 'claimed' WHERE url_id IN ({placeholders})",
            ids
        )
        con.commit()
    return claimed


async def get_response(client, url):
    """Fetch a URL, returning (disposition, response).

    disposition is one of:
      "ok"    — an HTML response worth parsing (response is the httpx.Response)
      "skip"  — permanently uninteresting (robots-blocked, non-HTML, 4xx); never retry
      "retry" — a transient failure (timeout, connection error, 5xx); worth another go
    """
    if not await can_fetch(client, url):
        logging.error(f"Blocked by robots.txt: {url}")
        return "skip", None
    try:
        response = await client.get(url, timeout=10, follow_redirects=True)
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        status = e.response.status_code
        logging.error(f"HTTP {status} for {url}")
        return ("retry" if status >= 500 else "skip"), None
    except (httpx.InvalidURL, httpx.UnsupportedProtocol) as e:
        logging.warning(f"skipping malformed URL {url!r}: {e}")
        return "skip", None
    except httpx.HTTPError as e:
        # timeouts, connection errors, malformed responses — transient
        logging.error(f"Failed to fetch {url}: {e}")
        return "retry", None
    content_type = response.headers.get("Content-Type", "")
    if "text/html" not in content_type:
        logging.error(f"Not HTML: {url}")
        return "skip", None
    return "ok", response


async def fetcher(client, url, url_id, queue):
    """Pure producer: fetches and parses, then hands a result (with a disposition)
    to the writer. It never writes to the database itself — every frontier write
    happens serially in the writer. url_id rides along so the writer never has to
    look the URL back up."""
    host = urlparse(url).netloc
    if host_at_cap(host):
        await queue.put({"url": url, "url_id": url_id, "disposition": "capped"})
        return

    # politeness: hold the host's lock across the whole fetch so only one request
    # is ever in flight to a given host, and space requests out in time.
    lock = host_locks.setdefault(host, asyncio.Lock())
    async with lock:
        loop = asyncio.get_event_loop()
        wait = host_delay(host) - (loop.time() - host_last_hit.get(host, 0.0))
        if wait > 0:
            await asyncio.sleep(wait)
        disposition, response = await get_response(client, url)
        host_last_hit[host] = loop.time()

    if disposition == "ok":
        title, found_links = parse_response(response, url)
        await queue.put({
            "url": url,
            "url_id": url_id,
            "disposition": "ok",
            "title": title,
            "status": response.status_code,
            "found_links": found_links,
            "fetched_at": int(time.time()),
        })
    else:
        # "skip" (terminal) or "retry" (transient) — the writer decides the
        # resulting frontier status.
        await queue.put({"url": url, "url_id": url_id, "disposition": disposition})


async def writer(queue):
    items_since_commit = 0
    while True:
        item = await queue.get()
        if item is None:
            con.commit()
            break

        url = item["url"]
        url_id = item["url_id"]
        disposition = item["disposition"]

        # terminal: robots-blocked, non-HTML, 4xx, or host over its page cap.
        if disposition in ("skip", "capped"):
            cur.execute("UPDATE frontier SET status='skipped' WHERE url_id=?", (url_id,))
            items_since_commit += 1
            if items_since_commit >= COMMIT_EVERY:
                con.commit()
                items_since_commit = 0
            queue.task_done()
            continue

        # transient: count the attempt; give up (failed) once retries are exhausted,
        # otherwise return it to the frontier to be tried again later.
        if disposition == "retry":
            cur.execute("""
                UPDATE frontier
                   SET retry_count = retry_count + 1,
                       status = CASE WHEN retry_count + 1 >= ? THEN 'failed'
                                     ELSE 'pending' END
                 WHERE url_id = ?
            """, (MAX_RETRIES, url_id))
            items_since_commit += 1
            if items_since_commit >= COMMIT_EVERY:
                con.commit()
                items_since_commit = 0
            queue.task_done()
            continue

        # disposition == "ok": a real page came back.
        title = item["title"]
        status = item["status"]
        found_links = item["found_links"]
        fetched_at = item["fetched_at"]
        host = urlparse(url).netloc

        cur.execute(
            "INSERT OR IGNORE INTO pages (url_id, title, status, host, fetched_at) VALUES (?, ?, ?, ?, ?)",
            (url_id, title, status, host, fetched_at)
        )

        # intern the outlinks once, then batch the per-link writes as id pairs: one
        # executemany each instead of two execute() calls per link, so a link-dense
        # page doesn't stall the event loop (and ids are far smaller than URL text).
        link_ids = intern_urls(found_links)
        cur.executemany(
            "INSERT OR IGNORE INTO links (from_id, to_id) VALUES (?, ?)",
            [(url_id, link_ids[link]) for link in found_links]
        )
        # new links = how many frontier rows were actually inserted (dupes are
        # ignored), read off the connection's running change counter.
        before = con.total_changes
        cur.executemany(
            "INSERT OR IGNORE INTO frontier (url_id, host, added_at, status) VALUES (?, ?, ?, 'pending')",
            [(link_ids[link], urlparse(link).netloc, fetched_at) for link in found_links]
        )
        new_count = con.total_changes - before
        total_count = len(found_links)

        cur.execute("""
            INSERT INTO hosts (host, new_links, total_links)
            VALUES (?, ?, ?)
            ON CONFLICT(host) DO UPDATE SET
                new_links = new_links + ?,
                total_links = total_links + ?
        """, (host, new_count, total_count, new_count, total_count))

        cur.execute(
            "UPDATE frontier SET status = 'visited' WHERE url_id = ?",
            (url_id,)
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
    seeds = []
    with open(SEEDS_PATH) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            seeds.append(normalise_url(line))
    ids = intern_urls(seeds)
    now = int(time.time())
    for seed in seeds:
        cur.execute(
            "INSERT OR IGNORE INTO frontier (url_id, host, added_at, status) VALUES (?, ?, ?, 'pending')",
            (ids[seed], urlparse(seed).netloc, now)
        )
    con.commit()
    print(f"Seeded {len(seeds)} urls from {SEEDS_PATH}")


def decay():
    """Memory decay — forget the oldest territory that also went dry, the way an
    animal's memory is lossy. A *host* is forgotten only when it is all of:
      old  — last touched longer ago than FORGET_AFTER_DAYS,
      dry  — lifetime novelty below DRY_NOVELTY_THRESHOLD (never-foraged counts as dry),
      not a hub — fewer than HUB_MIN_INDEGREE distinct hosts link to it.
    Hubs are kept regardless of age. Inbound edges from others are left behind, so a
    forgotten landmark still shows faintly in the graph — others remember it."""
    cutoff = int(time.time()) - FORGET_AFTER_DAYS * 86400

    # age per host: the most recent time we touched it (a page fetch or a discovery)
    last_seen = {}
    for host, ts in cur.execute("""
        SELECT host, MAX(t) FROM (
            SELECT host, fetched_at AS t FROM pages
            UNION ALL
            SELECT host, added_at  AS t FROM frontier
        ) GROUP BY host
    """):
        if host is not None:
            last_seen[host] = ts or 0

    # dryness per host; hosts we have never foraged have no novelty yet → treat as dry
    novelty = {}
    for host, nl, tl in cur.execute("SELECT host, new_links, total_links FROM hosts"):
        novelty[host] = (nl / tl) if tl else 0.0

    candidates = [
        h for h, ts in last_seen.items()
        if ts < cutoff and novelty.get(h, 0.0) < DRY_NOVELTY_THRESHOLD
    ]
    if not candidates:
        return

    # spare hubs: in-degree = how many distinct other hosts link to this one
    indegree = {}
    for host, deg in cur.execute("""
        SELECT tgt.host, COUNT(DISTINCT src.host)
        FROM links l
        JOIN frontier src ON src.url_id = l.from_id
        JOIN frontier tgt ON tgt.url_id = l.to_id
        WHERE src.host <> tgt.host
        GROUP BY tgt.host
    """):
        indegree[host] = deg

    forget = [h for h in candidates if indegree.get(h, 0) < HUB_MIN_INDEGREE]
    if not forget:
        return

    for i in range(0, len(forget), 800):
        chunk = forget[i:i + 800]
        ph = ",".join("?" * len(chunk))
        # delete the host's outgoing edges first (identified via its frontier rows),
        # then its pages, frontier entries, and novelty stats.
        cur.execute(
            f"DELETE FROM links WHERE from_id IN "
            f"(SELECT url_id FROM frontier WHERE host IN ({ph}))", chunk)
        cur.execute(f"DELETE FROM pages    WHERE host IN ({ph})", chunk)
        cur.execute(f"DELETE FROM frontier WHERE host IN ({ph})", chunk)
        cur.execute(f"DELETE FROM hosts    WHERE host IN ({ph})", chunk)

    # sweep URLs no longer referenced by any page, frontier row, or edge
    cur.execute("""
        DELETE FROM urls WHERE id NOT IN (SELECT url_id FROM frontier)
                           AND id NOT IN (SELECT url_id FROM pages)
                           AND id NOT IN (SELECT from_id FROM links)
                           AND id NOT IN (SELECT to_id   FROM links)
    """)
    con.commit()
    con.execute("PRAGMA incremental_vacuum")   # hand the freed pages back to the OS
    con.commit()
    print(f"  ~ decay: forgot {len(forget)} old, dry, non-hub hosts")


async def main():
    queue = asyncio.Queue(maxsize=100)
    last_decay = time.monotonic()

    # reset any URLs stranded as 'claimed' from a previous crash
    cur.execute("UPDATE frontier SET status='pending' WHERE status='claimed'")
    con.commit()

    # seed from file only if frontier has nothing pending (first run / fresh db)
    if not has_pending():
        seed_from_file()

    async with httpx.AsyncClient(headers={"User-Agent": USER_AGENT}) as client:
        writer_task = asyncio.create_task(writer(queue))
        try:
            while not shutdown:
                if time.monotonic() - last_decay >= DECAY_EVERY_SECONDS:
                    decay()
                    last_decay = time.monotonic()
                batch = claim_batch(BATCH_SIZE)
                if not batch:
                    # frontier may only be momentarily empty (fetches in flight)
                    await asyncio.sleep(2)
                    if not has_pending():
                        break
                    continue
                async with asyncio.TaskGroup() as tg:
                    for url, url_id in batch:
                        tg.create_task(fetcher(client, url, url_id, queue))
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass
        finally:
            await queue.put(None)
            await writer_task

    con.commit()
    con.close()

asyncio.run(main())
