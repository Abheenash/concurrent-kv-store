// Append-only file persistence.
//
// Every successful mutating command is appended as one protocol line, so the log
// is itself a valid command stream: on startup the server replays it through the
// dispatcher. TTLs are logged as absolute wall-clock deadlines (PEXPIREAT) so a
// restart doesn't reset them.
//
// fsync policy, as in Redis:  always  — fsync after every append (durable, slow)
//                             everysec — a background thread fsyncs once a second
//                             no       — leave it to the OS
//
// compact() rewrites the file from the live store (one SET + optional PEXPIREAT
// per key) so a key that was written a million times costs one line, not a million.
#pragma once

#include "store.hpp"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>

namespace kv {

enum class Fsync { Always, EverySec, No };
bool parse_fsync(std::string_view s, Fsync& out);

class Aof {
public:
    Aof(std::string path, Fsync policy);
    ~Aof();

    // Replays existing contents through `apply`, then opens for appending.
    // Returns the number of lines replayed.
    std::size_t open(const std::function<void(std::string_view)>& apply);
    void append(std::string_view line);           // line without trailing newline
    void compact(const Store& store);             // rewrite from the live store
    std::uint64_t bytes() const noexcept { return bytes_.load(std::memory_order_relaxed); }
    const std::string& path() const noexcept { return path_; }

private:
    void sync_loop();
    void write_all(int fd, std::string_view s);

    std::string path_;
    Fsync policy_;
    int fd_ = -1;
    std::mutex m_;
    std::atomic<std::uint64_t> bytes_{0};
    std::atomic<bool> dirty_{false};
    std::atomic<bool> stop_{false};
    std::thread syncer_;
    std::condition_variable cv_;
};

}  // namespace kv
