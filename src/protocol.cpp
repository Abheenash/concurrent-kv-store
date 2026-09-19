#include "protocol.hpp"

#include "aof.hpp"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <chrono>
#include <thread>

namespace kv {

// ------------------------------------------------------------------ framing
bool LineParser::next_line(std::string& out) {
    const std::size_t nl = buf_.find('\n', scan_from_);
    if (nl == std::string::npos) {
        // Nothing complete yet. Drop already-consumed bytes so the buffer doesn't grow.
        if (scan_from_ > 0) { buf_.erase(0, scan_from_); scan_from_ = 0; }
        return false;
    }
    std::size_t end = nl;
    if (end > scan_from_ && buf_[end - 1] == '\r') --end;
    out.assign(buf_, scan_from_, end - scan_from_);
    scan_from_ = nl + 1;
    if (scan_from_ >= buf_.size()) { buf_.clear(); scan_from_ = 0; }
    return true;
}

// ------------------------------------------------------------------ tokens
Command parse_command(std::string_view line) {
    Command c;
    c.line = line;
    std::size_t i = 0;
    while (i < line.size()) {
        while (i < line.size() && line[i] == ' ') ++i;
        if (i >= line.size()) break;
        std::size_t j = i;
        while (j < line.size() && line[j] != ' ') ++j;
        c.argv.push_back(line.substr(i, j - i));
        i = j;
    }
    return c;
}

std::string_view Command::rest_after_argv(std::size_t n) const {
    if (argv.size() <= n) return {};
    const char* start = argv[n].data() + argv[n].size();
    std::string_view rest(start, static_cast<std::size_t>(line.data() + line.size() - start));
    while (!rest.empty() && rest.front() == ' ') rest.remove_prefix(1);
    return rest;
}

// ------------------------------------------------------------------ helpers
namespace {

std::string upper(std::string_view s) {
    std::string u(s);
    for (char& ch : u) ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
    return u;
}

bool to_int(std::string_view s, std::int64_t& out) {
    auto r = std::from_chars(s.data(), s.data() + s.size(), out);
    return r.ec == std::errc() && r.ptr == s.data() + s.size();
}

std::string integer(std::int64_t v) { return "(integer) " + std::to_string(v) + "\n"; }
const std::string kOK = "OK\n";
const std::string kNil = "(nil)\n";
std::string err(std::string_view msg) { return "ERR " + std::string(msg) + "\n"; }

std::int64_t unix_ms_now() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch()).count();
}
// Absolute wall-clock deadline -> steady_clock time point used by the store.
std::optional<TimePoint> deadline_from_unix_ms(std::int64_t unix_ms) {
    const std::int64_t delta = unix_ms - unix_ms_now();
    return Clock::now() + std::chrono::milliseconds(delta);
}
std::int64_t unix_ms_from_ttl(std::chrono::milliseconds ttl) { return unix_ms_now() + ttl.count(); }

}  // namespace

// ------------------------------------------------------------------ dispatch
Dispatcher::Result Dispatcher::handle(std::string_view line) {
    stats_.commands.fetch_add(1, std::memory_order_relaxed);
    Result r = dispatch(parse_command(line), /*replaying=*/false);
    if (r.response.compare(0, 4, "ERR ") == 0) stats_.errors.fetch_add(1, std::memory_order_relaxed);
    return r;
}

void Dispatcher::replay(std::string_view line) { dispatch(parse_command(line), /*replaying=*/true); }

Dispatcher::Result Dispatcher::dispatch(const Command& c, bool replaying) {
    if (c.argv.empty()) return {err("empty command")};
    const std::string cmd = upper(c.argv[0]);
    const std::size_t argc = c.argv.size();
    auto log = [&](const std::string& line) { if (aof_ && !replaying) aof_->append(line); };

    if (cmd == "PING") return {"PONG\n"};
    if (cmd == "ECHO") return {std::string(c.rest_after_argv(0)) + "\n"};
    if (cmd == "QUIT") return {kOK, true};

    if (cmd == "GET") {
        if (argc != 2) return {err("usage: GET key")};
        stats_.gets.fetch_add(1, std::memory_order_relaxed);
        auto v = store_.get(c.argv[1]);
        return {v ? *v + "\n" : kNil};
    }
    if (cmd == "SET") {
        if (argc < 3) return {err("usage: SET key value")};
        stats_.sets.fetch_add(1, std::memory_order_relaxed);
        std::string_view value = c.rest_after_argv(1);
        store_.set(c.argv[1], std::string(value));
        log("SET " + std::string(c.argv[1]) + " " + std::string(value));
        return {kOK};
    }
    if (cmd == "SETNX") {
        if (argc < 3) return {err("usage: SETNX key value")};
        std::string_view value = c.rest_after_argv(1);
        const bool did = store_.set_nx(c.argv[1], std::string(value));
        if (did) log("SET " + std::string(c.argv[1]) + " " + std::string(value));
        return {integer(did ? 1 : 0)};
    }
    if (cmd == "DEL") {
        if (argc < 2) return {err("usage: DEL key [key ...]")};
        std::int64_t n = 0;
        for (std::size_t i = 1; i < argc; ++i) {
            if (store_.del(c.argv[i])) { ++n; log("DEL " + std::string(c.argv[i])); }
        }
        return {integer(n)};
    }
    if (cmd == "EXISTS") {
        if (argc != 2) return {err("usage: EXISTS key")};
        return {integer(store_.exists(c.argv[1]) ? 1 : 0)};
    }
    if (cmd == "INCR" || cmd == "DECR" || cmd == "INCRBY" || cmd == "DECRBY") {
        std::int64_t delta = 1;
        if (cmd == "INCRBY" || cmd == "DECRBY") {
            if (argc != 3 || !to_int(c.argv[2], delta)) return {err("usage: " + cmd + " key integer")};
        } else if (argc != 2) {
            return {err("usage: " + cmd + " key")};
        }
        if (cmd == "DECR" || cmd == "DECRBY") delta = -delta;
        auto v = store_.incr(c.argv[1], delta);
        if (!v) return {err("value is not an integer")};
        log("SET " + std::string(c.argv[1]) + " " + std::to_string(*v));
        // A SET line in the log drops the TTL; re-log the deadline so replay keeps it.
        if (const std::int64_t ms = store_.ttl_ms(c.argv[1]); ms >= 0) {
            log("PEXPIREAT " + std::string(c.argv[1]) + " " + std::to_string(unix_ms_from_ttl(std::chrono::milliseconds(ms))));
        }
        return {integer(*v)};
    }
    if (cmd == "EXPIRE" || cmd == "PEXPIRE") {
        std::int64_t n;
        if (argc != 3 || !to_int(c.argv[2], n) || n < 0) return {err("usage: " + cmd + " key integer")};
        const auto ttl = cmd == "EXPIRE" ? std::chrono::milliseconds(n * 1000) : std::chrono::milliseconds(n);
        const bool did = store_.expire(c.argv[1], ttl);
        if (did) log("PEXPIREAT " + std::string(c.argv[1]) + " " + std::to_string(unix_ms_from_ttl(ttl)));
        return {integer(did ? 1 : 0)};
    }
    if (cmd == "PEXPIREAT") {  // internal: absolute unix-ms deadline (what the AOF stores)
        std::int64_t at;
        if (argc != 3 || !to_int(c.argv[2], at)) return {err("usage: PEXPIREAT key unix_ms")};
        if (at <= unix_ms_now()) { store_.del(c.argv[1]); return {integer(1)}; }
        auto v = store_.get(c.argv[1]);
        if (!v) return {integer(0)};
        store_.set(c.argv[1], *v, deadline_from_unix_ms(at));
        log("PEXPIREAT " + std::string(c.argv[1]) + " " + std::to_string(at));
        return {integer(1)};
    }
    if (cmd == "TTL" || cmd == "PTTL") {
        if (argc != 2) return {err("usage: " + cmd + " key")};
        std::int64_t ms = store_.ttl_ms(c.argv[1]);
        if (cmd == "TTL" && ms >= 0) ms = (ms + 999) / 1000;
        return {integer(ms)};
    }
    if (cmd == "PERSIST") {
        if (argc != 2) return {err("usage: PERSIST key")};
        const bool did = store_.persist(c.argv[1]);
        if (did) { auto v = store_.get(c.argv[1]); if (v) log("SET " + std::string(c.argv[1]) + " " + *v); }
        return {integer(did ? 1 : 0)};
    }
    if (cmd == "MGET") {
        if (argc < 2) return {err("usage: MGET key [key ...]")};
        std::string out = "*" + std::to_string(argc - 1) + "\n";
        for (std::size_t i = 1; i < argc; ++i) {
            auto v = store_.get(c.argv[i]);
            out += v ? *v + "\n" : kNil;
        }
        return {out};
    }
    if (cmd == "KEYS") {
        std::int64_t limit = 0;
        if (argc > 3 || (argc == 3 && (!to_int(c.argv[2], limit) || limit < 0))) return {err("usage: KEYS [prefix] [limit]")};
        auto ks = store_.keys(argc >= 2 ? c.argv[1] : std::string_view{}, static_cast<std::size_t>(limit));
        std::sort(ks.begin(), ks.end());
        std::string out = "*" + std::to_string(ks.size()) + "\n";
        for (const auto& k : ks) { out += k; out += '\n'; }
        return {out};
    }
    if (cmd == "DBSIZE") return {integer(static_cast<std::int64_t>(store_.size()))};
    if (cmd == "FLUSHALL") {
        store_.clear();
        log("FLUSHALL");
        return {kOK};
    }
    if (cmd == "COMPACT") {
        if (!aof_) return {err("no append-only file configured")};
        aof_->compact(store_);
        return {kOK};
    }
    if (cmd == "INFO") return {info()};
    return {err("unknown command '" + std::string(c.argv[0]) + "'")};
}

std::string Dispatcher::info() const {
    const auto up = std::chrono::duration_cast<std::chrono::seconds>(Clock::now() - stats_.started).count();
    std::vector<std::string> lines = {
        "uptime_seconds: " + std::to_string(up),
        "mode: " + mode_info_,
        "shards: " + std::to_string(store_.shard_count()),
        "keys: " + std::to_string(store_.size()),
        "connections_open: " + std::to_string(stats_.connections_open.load()),
        "connections_total: " + std::to_string(stats_.connections_total.load()),
        "commands: " + std::to_string(stats_.commands.load()),
        "gets: " + std::to_string(stats_.gets.load()),
        "sets: " + std::to_string(stats_.sets.load()),
        "errors: " + std::to_string(stats_.errors.load()),
        "expired_swept: " + std::to_string(stats_.expired_swept.load()),
        "aof: " + (aof_ ? aof_->path() + " (" + std::to_string(aof_->bytes()) + " bytes)" : std::string("off")),
        "hardware_threads: " + std::to_string(std::thread::hardware_concurrency()),
    };
    std::string out = "*" + std::to_string(lines.size()) + "\n";
    for (const auto& l : lines) { out += l; out += '\n'; }
    return out;
}

}  // namespace kv
