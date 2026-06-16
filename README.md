# Concurrent Key-Value Store over TCP (C++)

A multithreaded TCP server that implements an in-memory key-value store. Multiple clients can connect at the same time and read/write shared data over the network using simple `SET` and `GET` commands.

## How It Works

The server uses the standard POSIX socket sequence:

- **`socket` → `bind` → `listen` → `accept`** sets up a TCP listener on port 5555.
- Every time a client connects, `accept` returns a new connection and the server hands it to **its own thread** (`std::thread(...).detach()`). This is the *thread-per-connection* model, so many clients can be served at once.
- All clients share one key-value map. A **mutex** guards every read and write to that map, so concurrent clients never corrupt the shared state or see a half-written value.

Because the data lives in the server, values persist across commands and across separate client connections for as long as the server is running.

## Build and Run

```bash
clang++ -std=c++17 kvserver.cpp -o kvserver
./kvserver
```

The server prints `Key-value server listening on port 5555` and waits for clients.

## Usage

From another terminal, connect with `nc` (netcat) and send commands:

```bash
nc localhost 5555
```

```
SET name abhee      ->  OK
GET name            ->  abhee
GET missing         ->  (nil)
```

- `SET <key> <value>` stores a value and replies `OK`.
- `GET <key>` returns the stored value, or `(nil)` if the key doesn't exist.

You can open several `nc` connections at once — they all talk to the same store concurrently.

## Design Notes

- **Concurrency model:** one thread per client connection. Simple and effective for a moderate number of clients.
- **Thread safety:** a single mutex serializes access to the shared map. Correctness first; under heavy contention this becomes the bottleneck (see below).

## Future Improvements
- Buffer and frame incoming data so multiple commands arriving in one packet are all processed (the current version handles one command per read)
- Add `DELETE` and `KEYS` commands
- Replace thread-per-connection with a fixed thread pool + event loop to scale to many connections
- Shard the map across multiple locks (or use a concurrent hash map) to reduce contention
- Optional persistence: write the store to disk so data survives a restart
