// Wire protocol: newline-framed text, one command per line, values may contain spaces.
//
//   SET key value...        -> OK
//   GET key                 -> the value, or (nil)
//   SETNX key value...      -> (integer) 1|0
//   DEL key [key ...]       -> (integer) removed
//   EXISTS key              -> (integer) 1|0
//   INCR key | DECR key | INCRBY key n  -> (integer) new value
//   EXPIRE key seconds | PEXPIRE key ms -> (integer) 1|0
//   TTL key | PTTL key      -> (integer) remaining, -1 no expiry, -2 no key
//   PERSIST key             -> (integer) 1|0
//   MGET key [key ...]      -> *N then N lines (value or (nil))
//   KEYS [prefix] [limit]   -> *N then N lines
//   DBSIZE                  -> (integer) n
//   PING | ECHO text        -> PONG | text
//   INFO                    -> *N then N "field: value" lines
//   COMPACT                 -> OK (rewrite the append-only file from the live store)
//   FLUSHALL                -> OK
//   QUIT                    -> OK, then the server closes the connection
//
// LineParser turns an arbitrary byte stream into complete lines: several commands
// may arrive in one read(), and one command may be split across several — the v1
// server handled neither. Lines end in \n; a trailing \r is stripped (telnet/nc).
#pragma once

#include "store.hpp"

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

namespace kv {

class LineParser {
public:
    // Append bytes; call next_line() until it returns false.
    void feed(const char* data, std::size_t n) { buf_.append(data, n); }
    bool next_line(std::string& out);
    // Guard against a client that never sends a newline.
    bool overflow(std::size_t max_line) const noexcept { return buf_.size() > max_line; }
    std::size_t buffered() const noexcept { return buf_.size(); }

private:
    std::string buf_;
    std::size_t scan_from_ = 0;
};

// Tokenise a command line: first token is the command, then arguments.
// `rest_after(n)` gives everything after the nth token (for SET's value).
struct Command {
    std::vector<std::string_view> argv;
    std::string_view rest_after_argv(std::size_t n) const;
    std::string_view line;
};
Command parse_command(std::string_view line);

struct Stats {
    std::atomic<std::uint64_t> connections_total{0};
    std::atomic<std::uint64_t> connections_open{0};
    std::atomic<std::uint64_t> commands{0};
    std::atomic<std::uint64_t> errors{0};
    std::atomic<std::uint64_t> expired_swept{0};
    std::atomic<std::uint64_t> aof_bytes{0};
    std::atomic<std::uint64_t> gets{0}, sets{0};
    Clock::time_point started = Clock::now();
};

class Aof;  // append-only file, see aof.hpp

// Executes one command line against the store and returns the full response
// (including trailing newline). Thread-safe: shares nothing mutable except the
// store, stats, and AOF, which synchronise themselves.
class Dispatcher {
public:
    Dispatcher(Store& store, Stats& stats, Aof* aof, std::string mode_info = "")
        : store_(store), stats_(stats), aof_(aof), mode_info_(std::move(mode_info)) {}

    struct Result {
        std::string response;
        bool close = false;  // QUIT
    };
    Result handle(std::string_view line);

    // Replay a logged line without re-logging it (AOF startup).
    void replay(std::string_view line);

private:
    Result dispatch(const Command& c, bool replaying);
    std::string info() const;

    Store& store_;
    Stats& stats_;
    Aof* aof_;
    std::string mode_info_;
};

}  // namespace kv
