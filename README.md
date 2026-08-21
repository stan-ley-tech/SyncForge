# SyncForge

**Offline-first synchronization engine.** A generic infrastructure layer that lets
applications keep working while disconnected and safely, deterministically
reconcile changes when connectivity returns.

```
             ┌── Device A
             │
Server ──────┼── Device B
             │
             └── Device C
```

Three devices can go offline, independently edit the *same* record, and on
reconnect SyncForge deterministically resolves the conflict — every device and
the server converge on the identical value, regardless of sync order.

> Status: under active development. See [the build plan](docs/ARCHITECTURE.md)
> for design details as they land.

## Features

- [x] Project scaffolding
- [ ] Hybrid logical clock + version vectors
- [ ] Local & server operation logs
- [ ] Change tracking / incremental sync
- [ ] Conflict detection (version vectors) & deterministic resolution
- [ ] Idempotent operations & retry mechanisms
- [ ] Partial synchronization
- [ ] Device registration & multi-device support
- [ ] Tombstones / deletions
- [ ] Sync checkpoints
- [ ] REST API
- [ ] Client SDK
- [ ] Three-device convergence proof (automated test + demo CLI)

## Layout

```
cmd/syncforged/        server binary
cmd/syncforge-demo/     three-device offline/reconnect demo CLI
internal/hlc/           hybrid logical clock
internal/vector/        version vectors (causality detection)
internal/oplog/         operation log types
internal/conflict/      deterministic conflict resolution engine
internal/storage/sqlite/ SQLite-backed storage (oplog + materialized records)
internal/server/        REST API handlers
pkg/api/                wire protocol types (REST DTOs)
pkg/client/             client SDK
examples/                example programs
test/integration/        end-to-end multi-device tests
docs/                     architecture & protocol docs
```

## License

[MIT](LICENSE)
