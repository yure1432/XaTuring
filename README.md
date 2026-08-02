# XaTuring

> A digital animal that lives in the open ocean of the public internet.


XaTuring is a digital animal which is meant to wander the clearnet forever, indexing all the sites
it comes across within itself. It doesn't cleanly index all sites though, instead it has a personality 
matrix within it which helps it choose which websites to focus on. It is, on the main branch at least,
not touched by AI in the slightest. It is meant to be a passion project, and just happened to be my magnum opus
in general programming. Since I am not a developer (mostly a cybersecurity person), this is woefully unoptimized.
If you do want to run this creature, you can use the optimized_by_claude branch for a pre-built AI optimized product, 
or better yet, open PRs to optimize the main branch itself.

The name itself comes from the godform conjured by Don Webb to govern cyberspace, spreading information and tools
freely across the internet. There has been some work done by people online on the Great Blackwyrm of Cyberspace,
but not as much as I'd like, so I want to drop my interpretation on it as well. If the concept of XaTuring intrigues
you, please visit [xaturing.net](https://xaturing.net/).

## What it is
XaTuring is an asynchronous single-process web crawler written in Python. Its main purpose (for me at least) was to 
browse the indie web (Neocities, Nekoweb etc.), but it quickly broke out of that niche and went all over the internet.

## How it works
It stores all the links given to it, either by itself or seed links provided by the user, in a database. It then takes 
the links which are yet to be visited, picks some, fetches the chosen links, and harvests more links from the 
harvested page contents. It then adds those links to the database via a Producer/Consumer writer system
and this cycle repeats.

The main distinction between any regular crawler and XaTuring is that it also gives each host a *novelty score*, which is
basically (Number of new links in page content)/(Number of links in page content). This rating is from 0.0 to 1.0, and the higher
rated websites are prioritized first. This is there to emulate the want for a creature to explore new greener regions than to
stay and stagnate in known waters. This is just for prioritizing links though, XaTuring will explore all the links it comes across,
it'll just care about novel links more.

## Relationship with sites and the internet
The crawler is polite, it follows robots.txt to a T. It also performs retries or skips depending on what HTTP code it receives. 
When a fetch fails, it's marked as *pending* and sent to be worked on at a later time.

## The wake
The main thing about XaTuring is that it's not a tool, it's a digital animal, not something to be used, but the urge to see what it
has seen does stir within me, so basically the journalctl logs for this will have all the links it has traversed, its 'wake' of sorts. 


## Running it

```bash
pip install -r requirements.txt

cp .env.example .env        # set DB_PATH, LOG_PATH, SEEDS_PATH

$EDITOR seeds.txt           # set your initial links for XaTuring to work on

python db_setup.py

python xaturing.py
```

It runs as a `systemd` service for always-on operation. Example units are in `systemd/`
(paths need adjusting per machine). The crawler resumes from its frontier on restart,
recovers cleanly from crashes, and shuts down gracefully on `SIGTERM`.

## Status

Functionally complete and actively running.

- [x] Polite crawler — normalization, robots compliance
- [x] Persistent SQLite memory — pages, the link-graph, a resumable frontier
- [x] Always-on systemd daemon — crash recovery, graceful shutdown
- [x] The wake — live logging
- [x] Asynchronous concurrent fetching — producer/consumer with a single writer
- [x] The gait — richness-driven foraging

## License

See [LICENSE](LICENSE).
