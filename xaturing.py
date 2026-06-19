import os
from collections import OrderedDict
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

# platforms whose subdomains are all the same "place" for capping purposes — the
# animal may visit and forage them, but shouldn't sink its life into one giant blog
# farm. All *.tumblr.com share ONE page budget, not 500 each. Neocities is
# deliberately NOT here: username.neocities.org sites are the indie web we want,
# so each keeps its own per-host budget.
PLATFORM_DOMAINS = {
    "tumblr.com",
    "wordpress.com",
    "blogspot.com",
    "medium.com",
    "substack.com",
    "wixsite.com",
    "blogger.com",
}
MAX_PAGES_PER_PLATFORM = 300   # total pages across ALL subdomains of a platform

# bounded in-memory host tracking — without a cap these dicts grow forever
# (every *.tumblr.com is a distinct host), leaking memory until OOM-killed.
MAX_TRACKED_HOSTS = 2000

# memory decay — the animal forgets the oldest territory that also went dry,
# but keeps hubs (well-linked landmarks) regardless of age.
FORGET_AFTER_DAYS = 30        # territory untouched longer than this is a candidate
DRY_NOVELTY_THRESHOLD = 0.05  # lifetime new/total below this counts as dry water
HUB_MIN_INDEGREE = 5          # linked to by >= this many distinct hosts = hub (spared)
DECAY_EVERY_SECONDS = 6 * 3600
CHECKPOINT_EVERY_SECONDS = 300   # flush + truncate WAL every 5 min

# scheme handling: the indie/old web has genuinely http-only hosts (personal sites,
# old neocities pages). Remember which hosts forced us down to http so we don't keep
# paying a failed-https round trip + retries for every URL on them.
http_only_hosts = OrderedDict()   # host -> True


def open_db(path):
    """Open a connection with the pragmas every connection in this process needs.
    Each thread (main loop, decay thread) gets its OWN connection from this — a
    sqlite3 connection must not be shared across threads."""
    c = sqlite3.connect(path, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA wal_autocheckpoint=1000")      # checkpoint every ~1000 pages
    c.execute("PRAGMA journal_size_limit=67108864")  # truncate WAL back to <=64MB
    c.execute("PRAGMA auto_vacuum=INCREMENTAL")
    c.execute("PRAGMA synchronous=NORMAL")           # durable enough under WAL, faster
    c.execute("PRAGMA busy_timeout=30000")           # wait, don't error, on lock contention
    return c


def ensure_schema(c):
    """Create tables and indexes if they don't exist. Idempotent — safe every run.
    page_count columns on hosts/platforms let host_at_cap do indexed lookups instead
    of COUNT(*) scans (the old LIKE '%.tumblr.com' could not use an index at all).
    host_links is a host->host rollup that survives URL deletion in decay(), so hub
    in-degree stays correct across decay cycles."""
    c.executescript("""
        CREATE TABLE IF NOT EXISTS urls (
            id  INTEGER PRIMARY KEY,
            url TEXT UNIQUE
        );
        CREATE TABLE IF NOT EXISTS pages (
            url_id     INTEGER PRIMARY KEY,
            title      TEXT,
            status     INTEGER,
            host       TEXT,
            fetched_at INTEGER
        );
        CREATE TABLE IF NOT EXISTS frontier (
            url_id      INTEGER PRIMARY KEY,
            host        TEXT,
            added_at    INTEGER,
            status      TEXT DEFAULT 'pending',
            retry_count INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS links (
            from_id INTEGER,
            to_id   INTEGER,
            PRIMARY KEY (from_id, to_id)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS hosts (
            host        TEXT PRIMARY KEY,
            new_links   INTEGER DEFAULT 0,
            total_links INTEGER DEFAULT 0,
            page_count  INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS platforms (
            platform   TEXT PRIMARY KEY,
            page_count INTEGER DEFAULT 0
        );
        -- host-level edge rollup; survives URL deletion so hub detection is stable
        CREATE TABLE IF NOT EXISTS host_links (
            from_host TEXT,
            to_host   TEXT,
            PRIMARY KEY (from_host, to_host)
        ) WITHOUT ROWID;

        CREATE INDEX IF NOT EXISTS idx_frontier_status ON frontier(status);
        CREATE INDEX IF NOT EXISTS idx_frontier_host   ON frontier(host);
        CREATE INDEX IF NOT EXISTS idx_pages_host      ON pages(host);
        CREATE INDEX IF NOT EXISTS idx_pages_fetched   ON pages(fetched_at);
        CREATE INDEX IF NOT EXISTS idx_frontier_added  ON frontier(added_at);
        CREATE INDEX IF NOT EXISTS idx_hostlinks_to    ON host_links(to_host);
    """)
    c.commit()


con = open_db(DB_PATH)
ensure_schema(con)
cur = con.cursor()
# the writer gets its own cursor so its stateful iteration never interleaves with
# claim_batch's cursor use on the shared connection at an await boundary.
writer_cur = con.cursor()

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


def normalise_url(url, prefer_scheme=None):
    """Normalise a URL. By default we keep the URL's own scheme (the old web has
    real http-only hosts — forcing https there just burns retries). To keep one host
    from splitting into http:// and https:// duplicates, callers pass prefer_scheme
    once they've learned which the host actually serves."""
    url = urldefrag(url)[0]
    parsed = urlparse(url)
    scheme = prefer_scheme or (parsed.scheme if parsed.scheme in ("http", "https") else "https")
    host = parsed.netloc.lower()
    path = parsed.path.rstrip('/')
    normalised = scheme + "://" + host + path
    if parsed.query:
        normalised += "?" + parsed.query
    return normalised


def host_of(url):
    return urlparse(url).netloc


def parse_response(response, base_url):
    soup = BeautifulSoup(response.text, "lxml")
    title_tag = soup.find('title')
    title = title_tag.get_text(strip=True)[:256] if title_tag else "No title"
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


robots_cache = OrderedDict()    # host -> RobotFileParser (the heavy one)
host_locks = OrderedDict()      # host -> asyncio.Lock
host_last_hit = OrderedDict()   # host -> last hit time


def _evict_if_needed(d):
    while len(d) > MAX_TRACKED_HOSTS:
        d.popitem(last=False)   # remove oldest (FIFO end)


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
    # single exit point: every lookup refreshes recency and evicts, so adding an
    # early return in the block above can't silently disable eviction.
    rp = robots_cache[host]
    robots_cache.move_to_end(host)
    _evict_if_needed(robots_cache)
    if rp is None:
        return True
    return rp.can_fetch(user_agent, url)


def platform_of(host):
    """If this host is a subdomain of a known platform, return the platform domain
    (so all its subdomains share one page budget). Otherwise None."""
    for domain in PLATFORM_DOMAINS:
        if host == domain or host.endswith("." + domain):
            return domain
    return None


def host_at_cap(host):
    """Indexed lookups against maintained counters — no COUNT(*) scan, and crucially
    no LIKE '%.platform' scan (a leading wildcard can't use an index). con.execute
    spins up its own short-lived cursor, so this read never disturbs the writer."""
    platform = platform_of(host)
    if platform:
        row = con.execute(
            "SELECT page_count FROM platforms WHERE platform = ?", (platform,)
        ).fetchone()
        return row is not None and row[0] >= MAX_PAGES_PER_PLATFORM
    row = con.execute(
        "SELECT page_count FROM hosts WHERE host = ?", (host,)
    ).fetchone()
    return row is not None and row[0] >= MAX_PAGES_PER_HOST


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
    """Fetch a URL, returning (disposition, response, final_url).
    disposition is one of:
      "ok"    — an HTML response worth parsing (response is the httpx.Response)
      "skip"  — permanently uninteresting (robots-blocked, non-HTML, 4xx); never retry
      "retry" — a transient failure (timeout, connection error, 5xx); worth another go
    final_url is the URL actually fetched (may differ from url if we fell back to http).
    """
    if not await can_fetch(client, url):
        logging.error(f"Blocked by robots.txt: {url}")
        return "skip", None, url

    host = host_of(url)
    # if we've already learned this host is http-only, go straight to http
    if host in http_only_hosts and url.startswith("https://"):
        url = "http://" + url[len("https://"):]

    async def _attempt(target):
        response = await client.get(target, timeout=10, follow_redirects=True)
        response.raise_for_status()
        return response

    try:
        response = await _attempt(url)
    except (httpx.ConnectError, httpx.ConnectTimeout) as e:
        # https may simply not be served here — try http once before deciding.
        if url.startswith("https://"):
            http_url = "http://" + url[len("https://"):]
            try:
                response = await _attempt(http_url)
                http_only_hosts[host] = True
                http_only_hosts.move_to_end(host)
                _evict_if_needed(http_only_hosts)
                url = http_url
            except httpx.HTTPError as e2:
                logging.error(f"Failed (http fallback) {url}: {e2}")
                return "retry", None, url
        else:
            logging.error(f"Failed to connect {url}: {e}")
            return "retry", None, url
    except httpx.HTTPStatusError as e:
        status = e.response.status_code
        logging.error(f"HTTP {status} for {url}")
        return ("retry" if status >= 500 else "skip"), None, url
    except (httpx.InvalidURL, httpx.UnsupportedProtocol) as e:
        logging.warning(f"skipping malformed URL {url!r}: {e}")
        return "skip", None, url
    except httpx.HTTPError as e:
        logging.error(f"Failed to fetch {url}: {e}")
        return "retry", None, url
    except Exception as e:
        # anything else (e.g. idna rejecting emoji hostnames) — terminal, never retry.
        logging.error(f"Unexpected error fetching {url}: {e}")
        return "skip", None, url

    content_type = response.headers.get("Content-Type", "")
    if "text/html" not in content_type:
        logging.error(f"Not HTML: {url}")
        return "skip", None, url
    if len(response.content) > 2_000_000:    # 2 MB cap — don't parse a huge tree
        logging.error(f"Too large: {url}")
        return "skip", None, url
    return "ok", response, url


async def fetcher(client, url, url_id, queue):
    """Pure producer: fetches and parses, then hands a result (with a disposition)
    to the writer. It never writes to the database itself — every frontier write
    happens serially in the writer. url_id rides along so the writer never has to
    look the URL back up."""
    host = host_of(url)
    if host_at_cap(host):
        await queue.put({"url": url, "url_id": url_id, "disposition": "capped"})
        return
    # politeness: hold the host's lock across the whole fetch so only one request
    # is ever in flight to a given host, and space requests out in time.
    lock = host_locks.setdefault(host, asyncio.Lock())
    host_locks.move_to_end(host)
    _evict_if_needed(host_locks)
    async with lock:
        loop = asyncio.get_event_loop()
        wait = host_delay(host) - (loop.time() - host_last_hit.get(host, 0.0))
        if wait > 0:
            await asyncio.sleep(wait)
        disposition, response, _final = await get_response(client, url)
        host_last_hit[host] = loop.time()
        host_last_hit.move_to_end(host)
        _evict_if_needed(host_last_hit)

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
            writer_cur.execute("UPDATE frontier SET status='skipped' WHERE url_id=?", (url_id,))
            items_since_commit += 1
            if items_since_commit >= COMMIT_EVERY:
                con.commit()
                items_since_commit = 0
            queue.task_done()
            continue

        # transient: count the attempt; give up (failed) once retries are exhausted,
        # otherwise return it to the frontier to be tried again later.
        if disposition == "retry":
            writer_cur.execute("""
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
        host = host_of(url)

        # was this page actually new? (controls whether we bump page counters)
        page_added = writer_cur.execute(
            "INSERT OR IGNORE INTO pages (url_id, title, status, host, fetched_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (url_id, title, status, host, fetched_at)
        ).rowcount == 1

        # intern the outlinks once; reuse the parsed host per link instead of
        # re-parsing each URL twice down below.
        link_ids = intern_urls(found_links)
        link_hosts = {link: host_of(link) for link in found_links}

        writer_cur.executemany(
            "INSERT OR IGNORE INTO links (from_id, to_id) VALUES (?, ?)",
            [(url_id, link_ids[link]) for link in found_links]
        )
        # host->host rollup that survives URL deletion in decay()
        writer_cur.executemany(
            "INSERT OR IGNORE INTO host_links (from_host, to_host) VALUES (?, ?)",
            [(host, link_hosts[link]) for link in found_links if link_hosts[link] != host]
        )

        # new links = how many frontier rows were actually inserted (dupes ignored)
        before = con.total_changes
        writer_cur.executemany(
            "INSERT OR IGNORE INTO frontier (url_id, host, added_at, status) "
            "VALUES (?, ?, ?, 'pending')",
            [(link_ids[link], link_hosts[link], fetched_at) for link in found_links]
        )
        new_count = con.total_changes - before
        total_count = len(found_links)

        writer_cur.execute("""
            INSERT INTO hosts (host, new_links, total_links, page_count)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(host) DO UPDATE SET
                new_links   = new_links + ?,
                total_links = total_links + ?,
                page_count  = page_count + ?
        """, (host, new_count, total_count, 1 if page_added else 0,
              new_count, total_count, 1 if page_added else 0))

        # platform budget: only bump when the page was genuinely new
        if page_added:
            platform = platform_of(host)
            if platform:
                writer_cur.execute("""
                    INSERT INTO platforms (platform, page_count) VALUES (?, 1)
                    ON CONFLICT(platform) DO UPDATE SET page_count = page_count + 1
                """, (platform,))

        writer_cur.execute(
            "UPDATE frontier SET status = 'visited' WHERE url_id = ?", (url_id,)
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
            "INSERT OR IGNORE INTO frontier (url_id, host, added_at, status) "
            "VALUES (?, ?, ?, 'pending')",
            (ids[seed], host_of(seed), now)
        )
    con.commit()
    print(f"Seeded {len(seeds)} urls from {SEEDS_PATH}")


def decay():
    """Memory decay — forget the oldest territory that also went dry, the way an
    animal's memory is lossy. A *host* is forgotten only when it is all of:
      old  — last touched longer ago than FORGET_AFTER_DAYS,
      dry  — lifetime novelty below DRY_NOVELTY_THRESHOLD (never-foraged counts as dry),
      not a hub — fewer than HUB_MIN_INDEGREE distinct hosts link to it.
    Hubs are kept regardless of age. Inbound host_links edges are left behind, so a
    forgotten landmark still shows faintly in the graph — others remember it.

    Runs in its own thread (via asyncio.to_thread) on its OWN connection, so the
    multi-table deletes and incremental vacuum never freeze the crawl loop."""
    dcon = open_db(DB_PATH)
    try:
        cutoff = int(time.time()) - FORGET_AFTER_DAYS * 86400

        # age per host: most recent time we touched it (page fetch or discovery)
        last_seen = {}
        for host, ts in dcon.execute("""
            SELECT host, MAX(t) FROM (
                SELECT host, fetched_at AS t FROM pages
                UNION ALL
                SELECT host, added_at  AS t FROM frontier
            ) GROUP BY host
        """):
            if host is not None:
                last_seen[host] = ts or 0

        # dryness per host; never-foraged hosts have no novelty yet → treat as dry
        novelty = {}
        for host, nl, tl in dcon.execute("SELECT host, new_links, total_links FROM hosts"):
            novelty[host] = (nl / tl) if tl else 0.0

        candidates = [
            h for h, ts in last_seen.items()
            if ts < cutoff and novelty.get(h, 0.0) < DRY_NOVELTY_THRESHOLD
        ]
        if not candidates:
            return

        # hub in-degree from the host_links rollup — stable across decay cycles,
        # since it doesn't depend on frontier/url rows that decay deletes.
        indegree = {}
        for host, deg in dcon.execute("""
            SELECT to_host, COUNT(DISTINCT from_host)
            FROM host_links
            GROUP BY to_host
        """):
            indegree[host] = deg

        forget = [h for h in candidates if indegree.get(h, 0) < HUB_MIN_INDEGREE]
        if not forget:
            return

        for i in range(0, len(forget), 800):
            chunk = forget[i:i + 800]
            ph = ",".join("?" * len(chunk))
            # outgoing per-URL edges (found via the host's frontier rows), then the
            # host's pages, frontier entries, novelty stats, and platform-agnostic
            # host_links FROM this host (inbound TO it is deliberately kept).
            dcon.execute(
                f"DELETE FROM links WHERE from_id IN "
                f"(SELECT url_id FROM frontier WHERE host IN ({ph}))", chunk)
            dcon.execute(f"DELETE FROM pages      WHERE host IN ({ph})", chunk)
            dcon.execute(f"DELETE FROM frontier   WHERE host IN ({ph})", chunk)
            dcon.execute(f"DELETE FROM hosts      WHERE host IN ({ph})", chunk)
            dcon.execute(f"DELETE FROM host_links WHERE from_host IN ({ph})", chunk)

        # sweep orphaned URL text: rows no page, frontier entry, or edge references.
        # NOT EXISTS with the indexes/PKs beats four NOT IN full-table scans.
        dcon.execute("""
            DELETE FROM urls
            WHERE NOT EXISTS (SELECT 1 FROM frontier f WHERE f.url_id = urls.id)
              AND NOT EXISTS (SELECT 1 FROM pages    p WHERE p.url_id = urls.id)
              AND NOT EXISTS (SELECT 1 FROM links   l1 WHERE l1.from_id = urls.id)
              AND NOT EXISTS (SELECT 1 FROM links   l2 WHERE l2.to_id   = urls.id)
        """)
        dcon.commit()
        dcon.execute("PRAGMA incremental_vacuum")
        dcon.commit()
        print(f"  ~ decay: forgot {len(forget)} old, dry, non-hub hosts")
    finally:
        dcon.close()


async def main():
    queue = asyncio.Queue(maxsize=100)
    last_decay = time.monotonic()
    last_checkpoint = time.monotonic()

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
                    # off the event loop, on its own connection — no crawl freeze
                    await asyncio.to_thread(decay)
                    last_decay = time.monotonic()

                if time.monotonic() - last_checkpoint >= CHECKPOINT_EVERY_SECONDS:
                    con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    last_checkpoint = time.monotonic()

                batch = claim_batch(BATCH_SIZE)
                if not batch:
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
