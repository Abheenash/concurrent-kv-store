#include "store.hpp"

#include <charconv>
#include <mutex>

namespace kv {

Store::Store(std::size_t shards) {
    if (shards == 0) shards = 1;
    shards_.reserve(shards);
    for (std::size_t i = 0; i < shards; ++i) shards_.emplace_back(std::make_unique<Shard>());
}

Store::Shard& Store::shard_for(std::string_view key) const {
    return *shards_[std::hash<std::string_view>{}(key) % shards_.size()];
}

std::optional<std::string> Store::get(std::string_view key) const {
    Shard& s = shard_for(key);
    const TimePoint now = Clock::now();
    {
        std::shared_lock<std::shared_mutex> lk(s.m);
        auto it = s.map.find(std::string(key));
        if (it == s.map.end()) return std::nullopt;
        if (!expired(it->second, now)) return it->second.value;
    }
    // Expired: upgrade to a write lock and remove it (re-check under the lock).
    std::unique_lock<std::shared_mutex> lk(s.m);
    auto it = s.map.find(std::string(key));
    if (it != s.map.end() && expired(it->second, now)) s.map.erase(it);
    return std::nullopt;
}

void Store::set(std::string_view key, std::string value, std::optional<TimePoint> expires_at) {
    Shard& s = shard_for(key);
    std::unique_lock<std::shared_mutex> lk(s.m);
    s.map[std::string(key)] = Entry{std::move(value), expires_at};
}

bool Store::set_nx(std::string_view key, std::string value) {
    Shard& s = shard_for(key);
    std::unique_lock<std::shared_mutex> lk(s.m);
    auto it = s.map.find(std::string(key));
    if (it != s.map.end() && !expired(it->second, Clock::now())) return false;
    s.map[std::string(key)] = Entry{std::move(value), std::nullopt};
    return true;
}

bool Store::del(std::string_view key) {
    Shard& s = shard_for(key);
    std::unique_lock<std::shared_mutex> lk(s.m);
    auto it = s.map.find(std::string(key));
    if (it == s.map.end()) return false;
    const bool was_live = !expired(it->second, Clock::now());
    s.map.erase(it);
    return was_live;
}

bool Store::exists(std::string_view key) const { return get(key).has_value(); }

std::optional<std::int64_t> Store::incr(std::string_view key, std::int64_t delta) {
    Shard& s = shard_for(key);
    std::unique_lock<std::shared_mutex> lk(s.m);
    auto it = s.map.find(std::string(key));
    std::int64_t cur = 0;
    std::optional<TimePoint> exp;
    if (it != s.map.end() && !expired(it->second, Clock::now())) {
        const std::string& v = it->second.value;
        auto res = std::from_chars(v.data(), v.data() + v.size(), cur);
        if (res.ec != std::errc() || res.ptr != v.data() + v.size()) return std::nullopt;
        exp = it->second.expires_at;
    }
    cur += delta;
    s.map[std::string(key)] = Entry{std::to_string(cur), exp};
    return cur;
}

bool Store::expire(std::string_view key, std::chrono::milliseconds ttl) {
    Shard& s = shard_for(key);
    std::unique_lock<std::shared_mutex> lk(s.m);
    auto it = s.map.find(std::string(key));
    const TimePoint now = Clock::now();
    if (it == s.map.end() || expired(it->second, now)) return false;
    it->second.expires_at = now + ttl;
    return true;
}

std::int64_t Store::ttl_ms(std::string_view key) const {
    Shard& s = shard_for(key);
    std::shared_lock<std::shared_mutex> lk(s.m);
    auto it = s.map.find(std::string(key));
    const TimePoint now = Clock::now();
    if (it == s.map.end() || expired(it->second, now)) return -2;
    if (!it->second.expires_at) return -1;
    return std::chrono::duration_cast<std::chrono::milliseconds>(*it->second.expires_at - now).count();
}

bool Store::persist(std::string_view key) {
    Shard& s = shard_for(key);
    std::unique_lock<std::shared_mutex> lk(s.m);
    auto it = s.map.find(std::string(key));
    if (it == s.map.end() || expired(it->second, Clock::now()) || !it->second.expires_at) return false;
    it->second.expires_at.reset();
    return true;
}

std::vector<std::string> Store::keys(std::string_view prefix, std::size_t limit) const {
    std::vector<std::string> out;
    const TimePoint now = Clock::now();
    for (const auto& sp : shards_) {
        std::shared_lock<std::shared_mutex> lk(sp->m);
        for (const auto& [k, e] : sp->map) {
            if (expired(e, now)) continue;
            if (k.compare(0, prefix.size(), prefix) != 0) continue;
            out.push_back(k);
            if (limit && out.size() >= limit) return out;
        }
    }
    return out;
}

std::size_t Store::size() const {
    std::size_t n = 0;
    const TimePoint now = Clock::now();
    for (const auto& sp : shards_) {
        std::shared_lock<std::shared_mutex> lk(sp->m);
        for (const auto& [k, e] : sp->map) if (!expired(e, now)) ++n;
    }
    return n;
}

void Store::clear() {
    for (const auto& sp : shards_) {
        std::unique_lock<std::shared_mutex> lk(sp->m);
        sp->map.clear();
    }
}

std::size_t Store::sweep_expired() {
    std::size_t removed = 0;
    const TimePoint now = Clock::now();
    for (const auto& sp : shards_) {
        std::unique_lock<std::shared_mutex> lk(sp->m);
        for (auto it = sp->map.begin(); it != sp->map.end();) {
            if (expired(it->second, now)) { it = sp->map.erase(it); ++removed; }
            else ++it;
        }
    }
    return removed;
}

void Store::for_each(const std::function<void(const std::string&, const Entry&)>& fn) const {
    const TimePoint now = Clock::now();
    for (const auto& sp : shards_) {
        std::shared_lock<std::shared_mutex> lk(sp->m);
        for (const auto& [k, e] : sp->map) if (!expired(e, now)) fn(k, e);
    }
}

}  // namespace kv
