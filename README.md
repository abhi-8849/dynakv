# dynakv

A Dynamo-style distributed key-value store in Go: leaderless, always writable, with tunable consistency.

Nodes gossip membership, place keys on a hash ring, and replicate each write to N nodes.

Build with `go build ./...` and check with `make lint`.
