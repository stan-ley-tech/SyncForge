# SyncForge

**Offline-first synchronization engine**, written in Go. A generic
infrastructure layer that lets applications keep working while
disconnected and safely, deterministically reconcile changes when
connectivity returns.

```
             ┌── Device A
             │
Server ──────┼── Device B
             │
             └── Device C
```

Three devices can go offline, independently edit the *same* record, and on
reconnect SyncForge deterministically resolves the conflict — every device
and the server converge on the identical value, regardless of sync order.
`test/integration` proves this automatically across all 6 possible
reconnect orders; `cmd/syncforge-demo` narrates it in plain English.

## Quickstart

Requires Go 1.25+. No CGO, no external database — pure Go throughout.

```sh
go build ./...
go test ./...              # full suite, including the convergence proof
go run ./cmd/syncforge-demo  # narrated three-device offline/reconnect demo
```

Run the server standalone:

```sh
go run ./cmd/syncforged -addr :8080 -db syncforged.db
```

Use the client SDK:

```go
c, err := client.Open("alice.db", "http://localhost:8080")
c.Register(ctx, "Alice's Laptop")

c.Put(ctx, "notes", "note-1", map[string]string{"title": "hello"}) // instant, local, works offline
c.Sync(ctx) // push local changes, pull remote ones, resolve any conflicts
```

## Features

- [x] Client synchronization protocol ([docs/PROTOCOL.md](docs/PROTOCOL.md))
- [x] Local operation log ([internal/oplog](internal/oplog), [internal/storage/sqlite](internal/storage/sqlite))
- [x] Server operation log (same storage layer, server-side instance)
- [x] Change tracking / incremental sync (`server_seq` checkpoints)
- [x] Conflict detection ([internal/vector](internal/vector) version vectors)
- [x] Conflict resolution ([internal/conflict](internal/conflict), deterministic via HLC)
- [x] Version vectors
- [x] Idempotent operations (op id = idempotency key)
- [x] Retry mechanisms ([pkg/client/retry.go](pkg/client/retry.go), exponential backoff)
- [x] Partial synchronization (`?collection=` filter)
- [x] Device registration ([internal/server/auth.go](internal/server/auth.go))
- [x] Multi-device support
- [x] Tombstones / deletions
- [x] Sync checkpoints
- [x] REST API ([docs/PROTOCOL.md](docs/PROTOCOL.md))
- [x] Client SDK ([pkg/client](pkg/client))
- [x] Three-device convergence proof ([test/integration](test/integration), [cmd/syncforge-demo](cmd/syncforge-demo))

## How conflict resolution works

Every write carries a **version vector** (did this write know about the
other one?) and a **Hybrid Logical Clock** timestamp (a total, deterministic
order across devices). When two writes to the same record are causally
unrelated — a real conflict — the write with the strictly greater HLC wins,
on every replica, regardless of what order they hear about it in. See
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full mechanism and why
it's provably order-independent.

## Layout

```
cmd/syncforged/          server binary
cmd/syncforge-demo/       three-device offline/reconnect demo CLI
internal/hlc/             hybrid logical clock
internal/vector/          version vectors (causality detection)
internal/oplog/           operation log types
internal/conflict/        deterministic conflict resolution engine
internal/storage/sqlite/  SQLite-backed storage (oplog + materialized records)
internal/server/          REST API handlers
pkg/api/                  wire protocol types (REST DTOs)
pkg/client/               client SDK
test/integration/         end-to-end multi-device tests
docs/                     architecture & protocol docs
```

## Docs

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — design, core primitives, trade-offs
- [docs/PROTOCOL.md](docs/PROTOCOL.md) — REST API reference

## License

[MIT](LICENSE)
