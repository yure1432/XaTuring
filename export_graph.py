import sqlite3
import os
import json
import networkx as nx
from urllib.parse import urlparse
from collections import defaultdict
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
DB_PATH = os.path.expanduser(os.getenv("DB_PATH", "memory.db"))

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

# --- host -> host edges ---
# links store integer url ids now; join through urls to recover the URL strings.
edge_weights = defaultdict(int)
for from_url, to_url in cur.execute("""
    SELECT uf.url, ut.url
    FROM links l
    JOIN urls uf ON uf.id = l.from_id
    JOIN urls ut ON ut.id = l.to_id
"""):
    from_host = urlparse(from_url).netloc
    to_host = urlparse(to_url).netloc
    if from_host and to_host and from_host != to_host:
        edge_weights[(from_host, to_host)] += 1

# --- most recent visit per host (for recency coloring) ---
last_seen = {}
for host, fetched_at in cur.execute(
    "SELECT host, MAX(fetched_at) FROM pages GROUP BY host"
):
    if host:
        last_seen[host] = fetched_at

con.close()

# --- build graph ---
G = nx.DiGraph()
for (f, t), w in edge_weights.items():
    G.add_edge(f, t, weight=w)

print(f"Computing layout for {G.number_of_nodes()} hosts...")
pos = nx.spring_layout(G, k=1.5, iterations=100, seed=42)

# --- community detection for cluster coloring ---
# greedy modularity works on undirected graphs
print("Detecting clusters...")
UG = G.to_undirected()
try:
    communities = nx.community.greedy_modularity_communities(UG)
    cluster_of = {}
    for i, comm in enumerate(communities):
        for node in comm:
            cluster_of[node] = i
    num_clusters = len(communities)
except Exception as e:
    print("Cluster detection failed:", e)
    cluster_of = {n: 0 for n in G.nodes()}
    num_clusters = 1

in_deg = dict(G.in_degree())

# --- recency: convert epoch timestamps to a 0..1 freshness score ---
times = [float(last_seen[h]) for h in G.nodes()
         if last_seen.get(h) is not None]

t_min = min(times) if times else 0
t_max = max(times) if times else 1
t_range = (t_max - t_min) or 1

def freshness(host):
    ts = last_seen.get(host)
    if ts is None:
        return 0.0
    return (float(ts) - t_min) / t_range   # 0 = oldest, 1 = most recent

SCALE = 1000
nodes = []
for host in G.nodes():
    x, y = pos[host]
    nodes.append({
        "id": host,
        "label": host,
        "x": x * SCALE,
        "y": y * SCALE,
        "size": min(2 + (in_deg.get(host, 0) ** 0.5) * 1.5, 15),
        "cluster": cluster_of.get(host, 0),
        "freshness": round(freshness(host), 3),
        "visited": host in last_seen   # was it actually crawled, or just linked-to
    })

edges = [
    {"source": f, "target": t, "weight": w}
    for (f, t), w in edge_weights.items()
]

graph = {"nodes": nodes, "edges": edges, "num_clusters": num_clusters, "generated_at": datetime.now().isoformat()}

out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "graph.json")
with open(out_path, "w") as f:
    json.dump(graph, f)

print(f"Exported {len(nodes)} hosts, {len(edges)} connections, {num_clusters} clusters to {out_path}")
