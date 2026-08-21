# SyncForge Architecture

SyncForge is an offline-first synchronization engine: a library and server
that let an application read and write data locally at all times, and
reconcile with a server (and every other device) whenever connectivity
allows — deterministically, even when multiple devices changed the same
data while disconnected.

```
             ┌── Device A
             │
Server ──────┼── Device B
             │
             └── Device C
```

## Design goals

- **Local writes never block on the network.** Every read and write goes
  through a local SQLite store first. `Sync()` is a separate, explicit step.
- **Sync is data-model agnostic.** Records are `(collection, record_id) ->
  JSON`, not fixed application tables — SyncForge is infrastructure, not an
  app.
- **Conflicts are detected, not avoided, and resolved deterministically.**
  Any device (or the server) that sees the same set of concurrent writes
  computes the identical outcome, regardless of the order it sees them in.
- **Sync is safe to retry.** Every operation is idempotent; pull has no
  side effects.

## The core primitives

### Operation log (`internal/oplog`)

Every change — create, update, or delete — is one `Op`: a client-generated
id (the idempotency key), the collection/record it targets, a full JSON
snapshot of the new value (SyncForge resolves conflicts on whole records,
not field-level diffs — see [Trade-offs](#trade-offs-and-extension-points)),
a version vector, and a Hybrid Logical Clock timestamp. Both the server and
every client keep their own append-only log of every op they've accepted —
the "local operation log" and "server operation log" are the same data
structure, just different instances.

### Version vectors (`internal/vector`)

A version vector is `map[deviceID]counter`. Every write to a record
increments the writing device's own counter. Comparing two vectors answers
one question: *did one of these writes know about the other?*

- **Ancestor/Descendant** — one vector's writes are a strict subset of the
  other's. One write causally followed the other; no conflict, just a
  fast-forward.
- **Concurrent** — neither vector's writes are a subset of the other's.
  Both devices wrote without knowing about each other's change. This is a
  real conflict.

### Hybrid Logical Clock (`internal/hlc`)

An HLC timestamp combines wall-clock time with a logical counter and a
node-id tiebreaker, giving a value that is both roughly time-ordered *and*
a strict total order — no two timestamps a well-behaved clock produces are
ever equal, and clock skew between devices can't make time run backwards
for a single clock. `Clock.Now()` advances it for local events;
`Clock.Observe(remote)` folds in a timestamp seen from another node so
later local events sort after everything the device has seen. This is what
makes "greatest HLC wins" a valid, deterministic tiebreaker.

### Deterministic conflict resolution (`internal/conflict`)

`Resolve(existing, existsAlready, op)` is the single function shared by the
server and every client. Given the current materialized state of a record
and an incoming op:

1. **No prior state** → the op wins trivially (first write).
2. **Vectors are Equal or the existing state Dominates (Descendant)** → the
   op is a stale or already-seen replay. No-op.
3. **The op's vector strictly Extends the existing state (Ancestor)** → an
   ordinary sequential edit. Fast-forward, no conflict.
4. **Vectors are Concurrent** → a genuine conflict. The op with the
   strictly greater HLC wins; the resulting version vector is the
   component-wise merge of both, so the record now reflects having seen
   both writes. The losing op is *not* deleted — it stays in the oplog,
   and a `Conflict` audit row records which op won.

Because "pick the greater HLC" and "merge vectors" are both commutative and
associative, folding any number of pairwise-concurrent writes through
`Resolve` — in any order — converges on the single globally-greatest one.
That's the whole mechanism behind three devices reconnecting in any order
and landing on the same answer; `internal/conflict/conflict_test.go`
proves it directly (`TestResolveConvergesRegardlessOfOrder`), and
`test/integration` proves it again through the real HTTP server, SQLite
storage, and client SDK, across all 6 possible reconnect orders.

### Tombstones

A delete is just an `Op` with `Type == Delete` and no payload. It goes
through the exact same `Resolve` function as any other write — an edit
racing a delete is a Concurrent conflict like any other, broken by HLC.
The materialized record is soft-deleted (`Deleted: true`), not physically
removed, so the tombstone itself can propagate to every other device during
sync.

### Sync checkpoints and incremental/partial sync (`internal/storage/sqlite`)

The server assigns every accepted op a monotonically increasing
`server_seq` — the sync checkpoint. A client's `pull?since=<checkpoint>`
only needs to remember the last checkpoint it applied; `OpsSince` scans
forward from there. `?collection=` further restricts a pull to one
collection (partial sync), and `?limit=` paginates (`has_more` /
`next_checkpoint`) through large backlogs.

### Idempotency and retries (`pkg/client`)

An op's id is its idempotency key: `AppendOp` checks for an existing row
with the same id before inserting, so re-pushing an op (after a dropped
response, a retry, or a crash mid-sync) has no additional effect —
"accepted" is returned again, nothing changes. Pull has no side effects at
all. That's what makes it safe for the client SDK to wrap every sync
request in exponential backoff (`pkg/client/retry.go`) and retry network
errors and 5xx responses freely; 4xx responses are never retried, since the
server has already rejected the request as invalid.

## Package layout

```
cmd/syncforged/          server binary
cmd/syncforge-demo/       narrated three-device offline/reconnect demo
internal/hlc/             Hybrid Logical Clock
internal/vector/          version vectors (causality detection)
internal/oplog/           Op, the operation log entry type
internal/record/          Record, the materialized "current state" type
internal/conflict/        the deterministic resolution engine
internal/storage/sqlite/  shared storage: oplog, records, conflicts,
                           devices, small kv — used by both client and server
internal/server/          REST API: device registration, push/pull/status
pkg/api/                  wire protocol DTOs (the REST contract)
pkg/client/                the client SDK
test/integration/          the multi-device convergence proof
```

`internal/conflict` has no dependency on storage or HTTP — it is a pure
function over `record.Record` and `oplog.Op` — which is what lets both
`internal/server` (resolving a push) and `pkg/client` (applying a pull, or
even a purely local write) call the exact same code path.

## Trade-offs and extension points

- **Whole-record conflict resolution**, not field-level merges or CRDTs.
  This keeps "who wins" provably deterministic and easy to reason about,
  at the cost of losing a non-conflicting field-level edit when two writes
  to *different* fields of the same record happen to be concurrent. A v2
  could add per-field version vectors or adopt a CRDT payload type for
  collections that need field-level merging.
- **No tombstone garbage collection.** Deleted records stay in `records`
  forever (as tombstones) and their ops stay in the oplog forever. A
  production deployment would need a retention/GC policy once devices are
  known to have synced past a given checkpoint.
- **Single-writer-per-process storage.** `storage/sqlite.DB` pins a single
  connection (`SetMaxOpenConns(1)`), turning SQLite's file-level write
  serialization into ordinary Go mutual exclusion. That's the right trade
  for an embedded client store and for this demo's server; a
  higher-throughput server would want a proper multi-writer database.
