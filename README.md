# XaTuring

> A digital animal that lives in the open ocean of the public internet.

XaTuring is not a search engine, not a scraper, not a tool. It is a single solitary
creature that moves continuously through the net, foraging across the link-graph the
way oceanic megafauna move through the seas — drifting through sparse water, lingering
where the water is rich, and crossing the whole basin in a single long migration when
the impulse takes it. It observes only what the net freely presents to anyone who asks,
and it carries everything it sees home into a private memory that belongs to no one but
itself.

It is named after XaTuring, the patron figure of computing from techno-occult writing —
the digital wyrm. This project inherits the name and the watcher's impulse behind it,
but not the worm's appetite for spread. XaTuring does not propagate, does not infect,
does not reach into systems. It moves through what is already open, the way a whale
moves through water that was never closed to it.

---

## What it is

XaTuring is a **movement, not a search**. Its frontier is not a task queue to be drained
but a living model of where the animal is and where it will go next. Its behaviour is
borrowed from movement ecology — the same idea that describes how sharks, turtles, and
tuna forage across patchy oceans.

When it enters a region rich in novelty — links it has never seen — it slows and forages
deeply, reading a host thoroughly before moving on. When the water thins and a host's
links become mostly already-seen, it drifts onward to fresher territory. This lingering
and leaving is not coded as an explicit rule; it **emerges** from a single mechanism:
the animal prefers hosts that have produced the most new links, and foraging a host
naturally exhausts its novelty until the animal drifts away on its own.

- The **link-graph** is its ocean.
- **Link novelty** is its plankton.
- The **movement model** is the animal itself.

## How it works

XaTuring is an asynchronous, concurrent crawler built around a producer/consumer
architecture, running as an always-on daemon.

**The crawl loop** claims a batch of URLs from the frontier, ordered by host richness
(the gait), and dispatches them to concurrent fetchers. Fetchers retrieve and parse
pages in parallel, then hand their results — not database writes — onto an `asyncio`
queue. A single writer task drains that queue and performs all database writes serially,
sidestepping SQLite's single-writer constraint entirely. The queue is bounded, so when
fetchers outrun the writer they apply backpressure and self-regulate to a sustainable
pace.

**The memory** is a SQLite database, kept deliberately compact so the animal can live a
long time on a small disk. Every URL string is stored exactly **once** in `urls`;
everything else refers to it by integer id, so the link-graph costs a couple of integers
per edge instead of two long strings.

| table | what it holds |
|-------|---------------|
| `urls` | every URL the animal has ever seen, stored once: `id → url` (the string pool) |
| `pages` | every page visited — `url_id`, title, host, status, timestamp |
| `links` | the graph — every `from_id → to_id` edge (the artifact), as id pairs |
| `frontier` | every discovered URL by `url_id`: `pending`, `claimed`, `visited`, `skipped` (robots-blocked, non-HTML, or dead), or `failed` (transient errors that outlived their retries), plus a `retry_count` |
| `hosts` | per-host novelty scores — the animal's learned map of where the good water is |

**The gait** is the soul of the project. The writer accumulates, per host, how many new
versus already-seen links each page produces. The claim step orders the pending frontier
by that novelty ratio, biasing the animal toward rich water. Unknown hosts get a neutral
default, so the unexplored is always worth investigating.

## Politeness is its nature, not a feature

XaTuring moves slowly enough never to disturb what it observes. It respects `robots.txt`,
holds itself to **one request per host at a time** with a crawl delay between touches
(honouring a host's `Crawl-delay` when it asks for more), identifies itself honestly with
a descriptive `User-Agent` on every request, and only ever issues plain GET requests.
Hostile, locked-down sites are simply thin water it moves past — not walls it batters
against. This is not an optional setting; it is what kind of creature XaTuring is.

## The wake

XaTuring has no interface and no dashboard — but it leaves a wake you can read.

- **The journal** (`journalctl -fu xaturing`) shows the animal moving in real time.
- **`test.py`** prints a snapshot: counts, frontier state, and the gait made visible —
  the richest and thinnest hosts, where it has lingered.
- **The viewer** (`viewer.html` + `export_graph.py`) renders the accumulated link-graph
  as a force-directed map of explored territory, coloured by cluster, recency, or hubs.

> When the graph grows dense enough, the territory arranges itself into the shape of an
> eye. The watcher, looking back.

## Architecture

```
                claim_batch (richness-ordered)
                        │
            ┌───────────▼────────────┐
            │   frontier (SQLite)     │◄──────────┐
            └───────────┬────────────┘            │
                        │ batch of URLs            │ new URLs
            ┌───────────▼────────────┐            │
            │  N concurrent fetchers  │            │
            │  (fetch + parse, async) │            │
            └───────────┬────────────┘            │
                        │ results                  │
                  ┌─────▼─────┐                    │
                  │ asyncio   │  (bounded queue,   │
                  │  queue    │   backpressure)    │
                  └─────┬─────┘                    │
                        │                          │
            ┌───────────▼────────────┐            │
            │   single writer task    │────────────┘
            │  (all DB writes serial) │
            └─────────────────────────┘
```

One dispatcher claims work, many fetchers run the slow network waiting in parallel,
one writer files results back into the frontier. The two singular points — claiming
and writing — are where all race conditions are prevented; the parallelism lives only
in the network-bound middle.

## Running it

```bash
pip install -r requirements.txt

# configure paths
cp .env.example .env        # set DB_PATH, LOG_PATH, SEEDS_PATH

# curate your seed waters — one URL per line
$EDITOR seeds.txt

# build the schema
python db_setup.py

# run it
python xaturing.py
```

It runs as a `systemd` service for always-on operation. Example units are in `systemd/`
(paths need adjusting per machine). The crawler resumes from its frontier on restart,
recovers cleanly from crashes, and shuts down gracefully on `SIGTERM`.

## Status

Functionally complete and actively running.

- [x] Polite crawler — normalization, rate limiting, robots compliance
- [x] Persistent SQLite memory — interned URLs, the link-graph, a resumable frontier
- [x] Always-on systemd daemon — crash recovery, graceful shutdown
- [x] The wake — live logging, by-hand queries, the territory viewer
- [x] Asynchronous concurrent fetching — producer/consumer with a single writer
- [x] The gait — richness-driven foraging
- [x] Memory decay — the animal forgets the oldest territory that also went **dry** (few
      new links), while **hubs** — landmarks many hosts link to — are kept regardless of
      age. Freed space is returned to the disk, so the footprint stays bounded for years.

## A note on intent

XaTuring exists for itself — a creature built to move through the net silently and see.
It is also a study in a particular kind of intelligence: not raw processing power, but
*discernment*. It does not crawl more than other crawlers; it crawls more **discerningly**,
using a cheap, accumulated map of its environment to decide where to spend attention —
the way a small animal survives not by out-computing the world but by knowing its
territory and moving through it well.

## License

See [LICENSE](LICENSE).
