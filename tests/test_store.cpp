#include "../src/store.hpp"
#include "check.hpp"

#include <atomic>
#include <thread>

int main() {
    kv::Store s(8);
    CHECK(!s.get("a"));
    s.set("a", "1");
    CHECK_EQ(*s.get("a"), std::string("1"));
    CHECK(s.exists("a"));
    CHECK_EQ(*s.incr("a"), 2);
    CHECK_EQ(*s.incr("a", 40), 42);
    CHECK_EQ(*s.incr("fresh", -3), -3);
    s.set("str", "hello");
    CHECK(!s.incr("str"));                       // not an integer
    CHECK(s.set_nx("b", "x"));
    CHECK(!s.set_nx("b", "y"));
    CHECK_EQ(*s.get("b"), std::string("x"));
    CHECK(s.del("b"));
    CHECK(!s.del("b"));

    // TTL: lazy expiry on read, sweep for the rest.
    s.set("t", "v");
    CHECK_EQ(s.ttl_ms("t"), -1);
    CHECK(s.expire("t", std::chrono::milliseconds(30)));
    CHECK(s.ttl_ms("t") > 0 && s.ttl_ms("t") <= 30);
    std::this_thread::sleep_for(std::chrono::milliseconds(40));
    CHECK(!s.get("t"));
    CHECK_EQ(s.ttl_ms("t"), -2);
    s.set("u", "v", kv::Clock::now() + std::chrono::milliseconds(10));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    CHECK_EQ(s.sweep_expired(), std::size_t{1});
    CHECK(!s.expire("nope", std::chrono::seconds(1)));
    s.set("p", "v"); s.expire("p", std::chrono::seconds(100));
    CHECK(s.persist("p")); CHECK_EQ(s.ttl_ms("p"), -1);

    // keys / size across shards
    for (int i = 0; i < 1000; ++i) s.set("user:" + std::to_string(i), "x");
    CHECK_EQ(s.keys("user:").size(), std::size_t{1000});
    CHECK_EQ(s.keys("user:9", 5).size(), std::size_t{5});
    CHECK(s.size() >= 1000);

    // Concurrency: 16 threads x 10,000 INCRs on one key must land exactly.
    kv::Store c(64);
    std::vector<std::thread> ts;
    for (int t = 0; t < 16; ++t) ts.emplace_back([&] { for (int i = 0; i < 10000; ++i) c.incr("hot"); });
    for (auto& t : ts) t.join();
    CHECK_EQ(*c.get("hot"), std::string("160000"));

    // Readers and writers on disjoint keys at the same time, no torn values.
    std::atomic<bool> stop{false};
    std::thread writer([&] { long n = 0; while (!stop) { c.set("w" + std::to_string(n % 50), std::string(64, 'a' + n % 26)); ++n; } });
    std::thread reader([&] { while (!stop) { auto v = c.get("w7"); if (v) { CHECK(v->size() == 64); for (char ch : *v) CHECK(ch == (*v)[0]); } } });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    stop = true; writer.join(); reader.join();

    std::printf("store: ok\n");
    return 0;
}
