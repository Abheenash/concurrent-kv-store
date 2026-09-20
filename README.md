# concurrent-kv-store

A Redis-style in-memory key-value server in C++17 on raw POSIX sockets: a **sharded store**
with reader/writer locks and **per-key TTLs**, a newline-framed protocol with a real command
set, **append-only-file persistence** with replay and compaction, **two I/O models** you can
switch between at startup (thread-per-connection and a multi-reactor `poll()` event loop), a
**load generator** that reports throughput and latency percentiles, and tests that run clean
under ThreadSanitizer.

[![ci](https://github.com/Abheenash/concurrent-kv-store/actions/workflows/ci.yml/badge.svg)](https://github.com/Abheenash/concurrent-kv-store/actions/workflows/ci.yml)

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j && ctest --test-dir build
./build/kvserver --aof data.aof --fsync everysec                    # port 5555; --mode event|poll|thread
printf 'SET user:1 Abheenash Rajolu\nINCR visits\nPEXPIRE visits 60000\nTTL visits\nGET user:1\n' | nc localhost 5555
./build/kvbench --clients 50 --requests 1000000 --pipeline 32
```

## Headline numbers — Apple M4 (10 cores), loopback, 32-byte values, 90% GET

Full matrix in [`results/bench-apple-m4.csv`](results/bench-apple-m4.csv); reproduce with `scripts/bench.sh`.

| scenario | req/s | p50 | p99 |
| --- | --- | --- | --- |
| 50 clients, **pipeline 32**, 64 shards, thread-per-conn | **5.09 M** | 175 µs | 1.07 ms |
| 50 clients, pipeline 32, 64 shards, poll ×10 | 4.21 M | 195 µs | 1.08 ms |
| 50 clients, pipeline 32, **1 shard (global mutex — the v1 design)**, poll ×10 | 1.23 M | 545 µs | 9.0 ms |
| 50 clients, no pipelining, 64 shards, thread-per-conn | 215 K | 232 µs | 276 µs |
| 50 clients, no pipelining, 64 shards, poll ×10 | 204 K | 172 µs | 1.05 ms |
| **500 clients**, no pipelining, poll ×10 | 199 K | 1.8 ms | 9.1 ms |
| 500 clients, no pipelining, thread-per-conn | 209 K | 2.4 ms | 2.6 ms |
| 50% writes, **AOF `everysec`** | 184 K | 157 µs | 0.99 ms |
| 50% writes, **AOF `always`** (fsync per write) | 84 K | 389 µs | 1.78 ms |
| 50% writes, no AOF | 206 K | 171 µs | 0.94 ms |

**kqueue / epoll vs poll** (`--mode event`, added later; [`results/bench-event-apple-m4.csv`](results/bench-event-apple-m4.csv)):

| clients | poll req/s · p99 | kqueue req/s · p99 |
| --- | --- | --- |
| 50 | 199 K · 1.33 ms | 210 K · **0.63 ms** |
| 500 | 210 K · 12.3 ms | **238 K** · **7.7 ms** |
| 2,000 | 206 K · 39.6 ms | 201 K · **24.2 ms** |

Throughput barely moves — the loopback client is the ceiling either way — but the tail halves: `poll()` hands the reactor every descriptor to scan on every wake-up, `kqueue` hands it only the ready ones, so an idle client stops costing the busy ones latency. The `epoll` variant runs in CI on Linux; both share one connection-servicing step with the `poll` loop, so the framing and half-close semantics are tested once.

What those say:

- **Sharding is worth 3–8× once the store is the bottleneck.** With 32 commands per round trip
  the request path is no longer dominated by syscalls, and one global mutex caps the whole server
  at ~1.2 M ops/s with a 9 ms p99; 64 shards with `std::shared_mutex` reach 4–5 M ops/s.
- **Without pipelining, sharding doesn't matter** — every design lands at ~200 K req/s, because a
  request is one `read()` + one `write()` on each side and the kernel round trip (~230 µs on the
  client's side at 50 clients) is the cost. That's the honest reason Redis clients pipeline.
- **Thread-per-connection is not slower** at these client counts; its p99 is actually tighter
  (a blocked thread wakes exactly when its socket is readable). What the event loop buys is
  *capacity*: 500 idle clients cost 500 `pollfd`s, not 500 threads.
- **Durability has a price you can choose.** `everysec` (a background fsync per second, the Redis
  default) costs ~11%; `always` costs 2.5× — and is the only setting that survives a power cut
  with the last write intact.

## What's in it

**Store** (`src/store.*`) — keys hash to one of N shards (default 64), each with its own
`std::shared_mutex` and `unordered_map`. Readers of different shards never contend; readers of
the same shard run concurrently; a writer blocks only its shard. Expiry is lazy on read (an
expired key is deleted when next touched, upgrading to a write lock only then) plus a
background sweep so keys that are never read again don't leak. `INCR` is a read-modify-write
under the shard's exclusive lock, so 16 threads × 10,000 `INCR`s land on exactly 160,000.

**Protocol** (`src/protocol.*`) — `LineParser` turns an arbitrary byte stream into complete
lines: several commands in one packet, one command split across packets, `\r\n` from telnet —
the v1 server handled none of these (it assumed one `read()` = one command). Commands:
`GET SET SETNX DEL EXISTS INCR DECR INCRBY DECRBY EXPIRE PEXPIRE TTL PTTL PERSIST MGET KEYS
DBSIZE PING ECHO INFO FLUSHALL COMPACT QUIT`. Values may contain spaces. Multi-line replies
are prefixed with `*N`.

**Persistence** (`src/aof.*`) — every successful mutating command is appended as a protocol
line, so the log is itself a valid command stream and startup is just replaying it. TTLs are
logged as absolute deadlines (`PEXPIREAT key unix_ms`) so a restart doesn't reset them, and an
`INCR` on a key with a TTL re-logs the deadline. `COMPACT` rewrites the file from the live
store — 1,000 `INCR`s collapse to one `SET` — into a temp file, fsyncs it, and `rename()`s
it over the old log atomically.

**Server** (`src/server.*`) — `--mode thread`: one OS thread per connection, blocking reads.
`--mode poll`: N reactors, each running `poll()` over its own connections *and the shared
listening socket*, so every reactor accepts for itself. `--mode event` (the default): the same
reactors on **kqueue** (macOS/BSD) or **epoll** (Linux), O(ready) per wake-up instead of O(all). Both batch all responses from one read
into one write (which is what makes pipelining fast) and both shut down gracefully on
`SIGINT`/`SIGTERM` via `sigwait` (no async-signal-safety games).

**Bench** (`bench/kvbench.cpp`) — C client threads, N requests, pipeline depth P, GET/SET mix,
keyspace size; pre-populates the keyspace, then reports req/s and p50/p90/p99/p99.9 per round
trip, plain or CSV.

## Bugs this project found in itself (all pinned by tests now)

- **`nc` got no replies in poll mode.** macOS reports `POLLHUP` together with `POLLIN` when a
  peer has half-closed but bytes are still buffered; treating `POLLHUP` as "dead" dropped the
  connection before reading its commands. Read first; let `read()==0` say the input is finished
  — and then still *flush the responses* before closing.
- **Shutdown hung in thread mode.** `close()`-ing a listening socket from another thread does not
  wake a blocked `accept()` on macOS. The listener is now non-blocking and polled with a timeout.
- **`SIGINT` did nothing when launched from a script.** Shells start background jobs with `SIGINT`
  set to `SIG_IGN`, and an ignored signal is discarded before it can become pending, so `sigwait`
  never saw it. The server resets the disposition to default before blocking the signal.
- **Thread mode dropped every client on macOS.** BSD-derived kernels make an accepted socket
  inherit the listener's `O_NONBLOCK`; the blocking `read()` got `EAGAIN` and treated it as an
  error. The mode is now set explicitly per connection.
- **Ten reactors lost ~100 of 500 simultaneous connections.** macOS caps the listen backlog at
  `somaxconn = 128`; a single acceptor thread starved for CPU by ten busy reactors let the
  backlog overflow and the kernel reset the excess. Removing the acceptor thread — every reactor
  accepts directly from the shared listener — fixed it: 502 of 502, and a simpler design.

## Tests

`tests/` — store semantics and TTLs, framing edge cases, every command's response, AOF
write → replay → compact → replay (values, deletions and remaining TTLs survive), and an
**end-to-end suite over real sockets for both I/O modes**: pipelined packets, a command split
across packets, a value with spaces, TTL expiry observed over the wire, `QUIT`, a half-closing
client, an over-long line, a client that vanishes mid-command, 50 concurrent connections doing
`INCR` on one key (exactly 10,000), connection accounting, and graceful stop with a client
attached. CI runs everything on Linux and macOS, then again under ThreadSanitizer and
AddressSanitizer, then smoke-tests the binary with `nc` and `kvbench`.

## Run it in Docker

```bash
docker build -t kvserver . && docker run -p 5555:5555 -v kvdata:/data kvserver
```

## What I'd do next

A proper RESP encoding so real Redis clients can talk to it, and replication — ship the AOF
stream to a follower. (The `epoll`/`kqueue` backend that used to head this list is in.)
