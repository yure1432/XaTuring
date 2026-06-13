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

# cap the rendered graph — full territory stays in the database untouched
TOP_N_HOSTS = 800

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

# --- host -> host edges (interned schema — join through urls table) ---
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

# --- score hosts by total connection weight, keep top N ---
host_degree = defaultdict(int)
for (f, t), w in edge_weights.items():
    host_degree[f] += w
    host_degree[t] += w

top_hosts = set(
    h for h, _ in sorted(host_degree.items(), key=lambda x: x[1], reverse=True)[:TOP_N_HOSTS]
)

# drop edges where either end isn't in the top set
edge_weights = {
    (f, t): w for (f, t), w in edge_weights.items()
    if f in top_hosts and t in top_hosts
}

# --- most recent visit per host (recency coloring) ---
last_seen = {}
for host, fetched_at in cur.execute(
    "SELECT host, MAX(fetched_at) FROM pages GROUP BY host"
):
    if host:
        last_seen[host] = fetched_at

con.close()

# --- build graph (capped) ---
G = nx.DiGraph()
for (f, t), w in edge_weights.items():
    G.add_edge(f, t, weight=w)

print(f"Computing layout for {G.number_of_nodes()} hosts (capped at {TOP_N_HOSTS} from full graph)...")
pos = nx.spring_layout(G, k=1.5, iterations=50, seed=42)

# --- community detection ---
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

# --- recency 0..1 ---
times = [float(last_seen[h]) for h in G.nodes() if last_seen.get(h) is not None]
t_min = min(times) if times else 0
t_max = max(times) if times else 1
t_range = (t_max - t_min) or 1

def freshness(host):
    ts = last_seen.get(host)
    if ts is None:
        return 0.0
    return (float(ts) - t_min) / t_range

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
        "visited": host in last_seen,
    })

edges = [
    {"source": f, "target": t, "weight": w}
    for (f, t), w in edge_weights.items()
]

graph = {
    "nodes": nodes,
    "edges": edges,
    "num_clusters": num_clusters,
    "generated_at": datetime.now().isoformat(),
}

out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "graph.json")
with open(out_path, "w") as f:
    json.dump(graph, f)

print(f"Exported {len(nodes)} hosts, {len(edges)} connections, {num_clusters} clusters to {out_path}")
