# dynakv

A Dynamo-style distributed key-value store in Go: leaderless, always writable, with tunable consistency. Every node can serve any request, data is replicated to N nodes, and concurrent writes are kept as siblings and merged by the client.

| Capability | Behavior |
| --- | --- |
| Put | `Put(key, value, context)` stores a new version once W nodes ack; stand-in nodes count |
| Get | Returns every concurrent version (siblings) plus one merged context |
| Delete | Writes a tombstone version; compaction purges it after the grace period |
| Tunable consistency | Defaults N=3, R=2, W=2; R and W can be overridden per request |
| Always writable | Down replicas are covered by sloppy quorum and hinted handoff |

## High-level design

```
                      +---------------------------+
   client  ---------> |  any node (coordinator)   |
 (ring-aware)         |  KV service               |
                      +-------------+-------------+
                                    | key -> preference list (N owners)
                                    v
                      +---------------------------+
                      |   hash ring, 256 vnodes   |<------ gossip (memberlist)
                      |   per node, zone-aware    |        join / alive / dead
                      +------+------+------+------+
                             |      |      |        Replica service (gRPC)
                    +--------+  +---+---+  +--------+
                    v           v       v           v
              +-----------+ +-----------+ +-----------+
              |  owner 1  | |  owner 2  | |  owner 3  |   zones a, b, c
              | storage   | | storage   | | storage   |
              | engine    | | engine    | | engine    |
              +-----------+ +-----------+ +-----------+
              (memory, or LSM: WAL -> memtable -> SSTables)
```

| Component | Job |
| --- | --- |
| Client | Reads the member list from a seed, refreshes it every 10 s, sends each request to the key's first live owner, falls back to a seed |
| KV service | Client-facing `Get`, `Put` and `Delete`; validates sizes and forwards requests for keys the node does not own |
| Coordinator | Stamps the vector clock, fans a request out to the owners in parallel, waits for R replies or W acks, reconciles versions |
| Hash ring | 64-bit xxhash ring with 256 virtual nodes per node; builds each key's preference list across zones |
| Membership | memberlist gossip: join, alive and dead states; rebuilds the ring when the member set changes |
| Replica service | Node-to-node `ReplicaPut`, `ReplicaGet` and `ReplayHint` on each node's local storage |
| Hints | Holds writes meant for a down node on a stand-in and replays them when the node is alive; expires them after `-hint-ttl` |
| Storage engine | `memory` (map) or `lsm` (WAL with group fsync, memtable, SSTables with Bloom filters, size-tiered compaction) |
| Vector clocks | `node id -> (counter, timestamp)`, at most 10 entries; order versions and detect concurrent writes |
| Metrics | Prometheus `/metrics` with request counts and latency by operation |

A key is hashed onto the ring and the next N distinct nodes clockwise, skipping a zone that is already used while unused zones remain, form its preference list.
The first live node in that list is the coordinator; a node outside the list forwards the request to it.

## How the components interact

### Put (healthy)

1. The client hashes the key, finds its first live owner and sends `Put` there.
2. The coordinator decodes the context (empty for a blind write) and sets its own vector clock entry to one more than both the context and anything it stored locally.
3. It sends the version to all N owners in parallel.
4. Each replica drops the versions the new one supersedes and keeps concurrent ones.
5. After W acks within `-timeout` the coordinator answers OK; slower replicas finish in the background. Fewer than W acks returns `Unavailable`.

### Put with replicas down (hinted handoff)

1. Gossip marks an owner dead.
2. For each dead owner the coordinator picks the next healthy node on the ring walk that is not already an owner, and sends it the write marked as a hint for that owner.
3. The stand-in stores the write under the reserved key prefix `\x00hint/<owner>\x00<key>` in its own engine; its ack counts toward W.
4. Every second the stand-in expires hints older than `-hint-ttl` and, for each owner that gossip reports alive, sends its hints with `ReplayHint`.
5. The owner applies each version; the stand-in removes the hint after the owner acks.

### Get (one version, siblings and merge)

1. The coordinator asks all N owners and waits for R replies.
2. It drops versions that another version supersedes, collapses duplicates and removes tombstones from the result.
3. One surviving version is returned alone. Several are concurrent siblings, returned together with one context that merges their clocks.
4. The client resolves siblings itself and calls `Put` with that context; the new clock supersedes every sibling, so the next `Get` returns one version.

### Read repair

1. After a read the coordinator compares each replica's reply with the reconciled result.
2. It writes the reconciled versions, in the background, to every replica that answered without them or with older ones.
3. The repair is logged as `read repair` with the key and node.

### Delete

1. `Delete` takes the normal write path and stores a tombstone version whose clock supersedes the context.
2. `Get` returns `NotFound` when only tombstones remain.
3. Compaction drops tombstones older than `-tombstone-grace`.

### Node join and failure detection

1. A node binds its gossip port and joins the seeds in `-join`; its metadata carries its gRPC address and zone.
2. memberlist spreads the join; every node adds the member and rebuilds its ring.
3. A node that leaves cleanly or stops answering probes is marked dead and logged; it stays on the ring so key ownership does not move.
4. When the node returns it is marked alive again. No data is streamed on join or leave: hints and read repair bring it up to date.

## Failure behavior

| Situation | Behavior |
| --- | --- |
| One owner down | Reads and writes succeed with the two other owners; a stand-in keeps a hint and replays it when the owner returns |
| Two owners down | Writes succeed through stand-ins (W counts them) and are replayed later; reads with R=2 return `Unavailable` until an owner returns |
| Node back after hints expired | The expired writes are not replayed; the node serves stale data until a read repairs it or a new write reaches it |
| Network partition | Each side keeps accepting writes through stand-ins; concurrent updates become siblings and merge on the next read after the partition heals |
| Node crash | With `-engine lsm` the WAL is replayed on start and acknowledged writes are kept; with `-engine memory` the node starts empty |
| One zone down | The preference list holds at most one owner per zone, so a key loses at most one owner and R=2, W=2 still succeed |

## API

### gRPC (`api/kv.proto`)

| Service | RPC | Result |
| --- | --- | --- |
| `KV` | `Get{key, r}` | `{versions[], context}`, `NotFound`, `Unavailable`, `InvalidArgument` |
| `KV` | `Put{key, value, context, w}` | `OK`, `Unavailable`, `InvalidArgument` |
| `KV` | `Delete{key, context, w}` | `OK`, `Unavailable`, `InvalidArgument` |
| `Replica` (internal) | `ReplicaPut{key, version, hint_for}`, `ReplicaGet{key}`, `ReplayHint{key, version}` | `Ack`, `versions[]` |
| `Ring` | `Status{}` | members `{id, addr, zone, alive}` |
| `Ring` | `Owners{key}` | the key's preference list |

`r` or `w` of 0 means the node default. Keys are at most 256 bytes and values at most 1 MB; larger requests return `InvalidArgument`. There is no authentication; nodes and clients are expected on a private network.

### CLI

```
dynakv-cli -seeds localhost:7001 put cart milk          # prints OK
dynakv-cli -seeds localhost:7001 get cart               # prints each sibling, then "context: <base64>"
dynakv-cli -seeds localhost:7001 put cart milk,eggs -ctx <base64>   # supersedes the siblings the context came from
dynakv-cli -seeds localhost:7001 delete cart
echo -n big-value | dynakv-cli -seeds localhost:7001 put key -      # "-" reads the value from stdin
dynakv-cli -seeds localhost:7001 -r 3 get cart          # read quorum for this request
dynakv-cli -seeds localhost:7001 owners cart            # the key's preference list
dynakv-cli -seeds localhost:7001 status                 # every member and whether it is alive
dynakv-cli -node localhost:7002 get cart                # send to one node, skipping ring routing
```

Node flags:

| Flag | Default | Meaning |
| --- | --- | --- |
| `-id` | hostname | Node id, unique in the cluster |
| `-addr` | `:7000` | gRPC listen address; peers dial it, with `localhost` standing in for an empty host |
| `-gossip-addr` | `:7946` | memberlist listen address |
| `-metrics-addr` | `:9100` | Prometheus `/metrics` address |
| `-join` | empty | Comma-separated gossip addresses of existing members |
| `-zone` | `default` | Failure domain of the node |
| `-vnodes` | 256 | Virtual nodes per node on the ring |
| `-n`, `-r`, `-w` | 3, 2, 2 | Replication factor and default read and write quorums |
| `-timeout` | 100ms | How long a request waits for its replicas |
| `-engine` | `memory` | `memory` or `lsm` |
| `-data-dir` | `./data` | Directory of the lsm engine |
| `-memtable-size` | 64MB | Memtable size that triggers a flush |
| `-hint-ttl` | 3h | How long a stand-in keeps a hint |
| `-tombstone-grace` | 240h | How long compaction keeps a tombstone |

CLI flags: `-seeds` (default `localhost:7000`), `-node`, `-r`, `-w` and `-ctx`.

## Run locally

Requires Go.

```
go build ./...
```

Start five nodes, one terminal each (`i` is 1 to 5, zones a, a, b, b, c). macOS uses port 7000 for AirPlay Receiver, so the example ports start at 7001.

```
go run -race ./cmd/dynakv -id n<i> -addr :700<i> -gossip-addr :780<i> -metrics-addr :910<i> -zone <a|a|b|b|c> -join localhost:7801
```

Then, in another terminal:

```
go run ./cmd/dynakv-cli -seeds localhost:7001 status
go run ./cmd/dynakv-cli -seeds localhost:7001 put k1 v1
go run ./cmd/dynakv-cli -seeds localhost:7001 get k1
```

To use the durable engine add `-engine lsm -data-dir <dir>` with a separate directory per node.

## Docker Compose

`deploy/docker-compose.yml` starts five nodes in zones a, a, b, b, c with the lsm engine, one named volume each, all joining through `node1`. Metrics are on host ports 9101 to 9105.

```
docker compose -f deploy/docker-compose.yml up --build -d
docker compose -f deploy/docker-compose.yml exec node1 /dynakv-cli -seeds node1:7000 status
docker compose -f deploy/docker-compose.yml exec node1 /dynakv-cli -seeds node1:7000 put k1 v1
docker compose -f deploy/docker-compose.yml stop node3
docker compose -f deploy/docker-compose.yml down -v
```

## Kubernetes

`deploy/k8s` runs three StatefulSets, one per zone (a: 2 pods, b: 2, c: 1), each pod with its own PersistentVolumeClaim, a headless Service for gossip seeds and a ClusterIP Service for clients. Probes are TCP checks on the gRPC port. Each pod listens on its pod IP, so the CLI inside a pod connects through the `dynakv-client` Service rather than `localhost`.

```
kind create cluster --name dynakv
docker build -f deploy/Dockerfile -t dynakv:local .
kind load docker-image dynakv:local --name dynakv
kubectl apply -f deploy/k8s
kubectl get pods
kubectl exec dynakv-a-0 -- /dynakv-cli -seeds dynakv-client:7000 status
kubectl delete pod dynakv-b-0
```
