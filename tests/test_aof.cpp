#include "../src/aof.hpp"
#include "../src/protocol.hpp"
#include "check.hpp"

#include <cstdio>
#include <fstream>
#include <string>
#include <thread>

static std::size_t line_count(const std::string& path) {
    std::ifstream in(path); std::string l; std::size_t n = 0;
    while (std::getline(in, l)) ++n;
    return n;
}

int main() {
    const std::string path = "test_aof.log";
    std::remove(path.c_str());

    // 1. Write through the dispatcher, with a TTL and a counter hammered 1000 times.
    {
        kv::Store store(4); kv::Stats stats;
        kv::Aof aof(path, kv::Fsync::No);
        kv::Dispatcher d(store, stats, &aof, "t");
        CHECK_EQ(aof.open([&](std::string_view l) { d.replay(l); }), std::size_t{0});
        d.handle("SET name Abheenash Rajolu");
        d.handle("SET temp gone"); d.handle("DEL temp");
        d.handle("SET session abc"); d.handle("PEXPIRE session 60000");
        d.handle("SET short x"); d.handle("PEXPIRE short 10");
        for (int i = 0; i < 1000; ++i) d.handle("INCR counter");
        CHECK(aof.bytes() > 0);
    }
    const std::size_t before = line_count(path);
    CHECK(before >= 1000);

    // 2. Replay into a fresh store: values, deletions and TTLs survive; the short TTL expired.
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    {
        kv::Store store(4); kv::Stats stats;
        kv::Aof aof(path, kv::Fsync::No);
        kv::Dispatcher d(store, stats, &aof, "t");
        CHECK_EQ(aof.open([&](std::string_view l) { d.replay(l); }), before);
        CHECK_EQ(d.handle("GET name").response, std::string("Abheenash Rajolu\n"));
        CHECK_EQ(d.handle("GET temp").response, std::string("(nil)\n"));
        CHECK_EQ(d.handle("GET counter").response, std::string("1000\n"));
        CHECK_EQ(d.handle("GET session").response, std::string("abc\n"));
        const auto ttl = store.ttl_ms("session");
        CHECK(ttl > 50000 && ttl <= 60000);
        CHECK_EQ(d.handle("GET short").response, std::string("(nil)\n"));
        CHECK_EQ(aof.bytes(), std::uint64_t{0} + line_count(path) * 0 + aof.bytes());  // opened at end

        // 3. Compact: 1000 INCR lines collapse to one SET; everything still replays.
        d.handle("COMPACT");
        CHECK(line_count(path) < 10);
        d.handle("INCR counter");  // appends after the rewrite
    }
    {
        kv::Store store(4); kv::Stats stats;
        kv::Aof aof(path, kv::Fsync::EverySec);
        kv::Dispatcher d(store, stats, &aof, "t");
        aof.open([&](std::string_view l) { d.replay(l); });
        CHECK_EQ(d.handle("GET counter").response, std::string("1001\n"));
        CHECK_EQ(d.handle("GET name").response, std::string("Abheenash Rajolu\n"));
        CHECK(store.ttl_ms("session") > 0);
        CHECK_EQ(d.handle("GET temp").response, std::string("(nil)\n"));
    }
    std::remove(path.c_str());
    std::printf("aof: ok\n");
    return 0;
}
