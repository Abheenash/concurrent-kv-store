// Sharded in-memory key-value store with per-key TTLs.
//
// Keys are hashed to one of N shards; each shard has its own std::shared_mutex and
// map. Readers of different keys never contend, readers of the same shard run
// concurrently, and a writer only blocks its own shard. A single global mutex — the
// v1 design — serialises every operation from every connection through one lock;
// the benchmark measures the difference.
//
// Expiry is lazy (an expired key is removed when it's next touched) plus a periodic
// sweep, so a key that is never read again does not leak.
#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace kv {

using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;

struct Entry {
    std::string value;
    std::optional<TimePoint> expires_at;
};

class Store {
public:
    explicit Store(std::size_t shards = 64);

    // Returns the value, or nullopt if absent or expired.
    std::optional<std::string> get(std::string_view key) const;
    void set(std::string_view key, std::string value, std::optional<TimePoint> expires_at = std::nullopt);
    // SET only if absent (returns true if set).
    bool set_nx(std::string_view key, std::string value);
    bool del(std::string_view key);
    bool exists(std::string_view key) const;
    // Adds delta to an integer value (missing -> 0). Returns nullopt if the value isn't an integer.
    std::optional<std::int64_t> incr(std::string_view key, std::int64_t delta = 1);
    // Set a TTL on an existing key; false if the key is absent.
    bool expire(std::string_view key, std::chrono::milliseconds ttl);
    // Remaining TTL in ms: -2 absent, -1 no expiry.
    std::int64_t ttl_ms(std::string_view key) const;
    bool persist(std::string_view key);

    // Keys starting with `prefix`, up to `limit` (0 = all). Snapshot; order unspecified.
    std::vector<std::string> keys(std::string_view prefix = {}, std::size_t limit = 0) const;
    std::size_t size() const;
    std::size_t shard_count() const noexcept { return shards_.size(); }
    void clear();

    // Remove every expired key. Returns the number removed.
    std::size_t sweep_expired();

    // Visit every live entry (used by AOF compaction). Holds each shard's read lock in turn.
    void for_each(const std::function<void(const std::string& key, const Entry&)>& fn) const;

private:
    struct Shard {
        mutable std::shared_mutex m;
        std::unordered_map<std::string, Entry> map;
    };
    Shard& shard_for(std::string_view key) const;
    static bool expired(const Entry& e, TimePoint now) {
        return e.expires_at && *e.expires_at <= now;
    }

    std::vector<std::unique_ptr<Shard>> shards_;
};

}  // namespace kv
