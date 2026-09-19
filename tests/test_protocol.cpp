#include "../src/protocol.hpp"
#include "check.hpp"

#include <string>

int main() {
    // Framing: several commands in one chunk, one command split across chunks, CRLF.
    kv::LineParser p;
    std::string line;
    auto feed = [&](const std::string& chunk) { p.feed(chunk.data(), chunk.size()); };
    feed("SET a 1\nGET a\nGE");
    CHECK(p.next_line(line)); CHECK_EQ(line, std::string("SET a 1"));
    CHECK(p.next_line(line)); CHECK_EQ(line, std::string("GET a"));
    CHECK(!p.next_line(line));
    feed("T b\r\n");
    CHECK(p.next_line(line)); CHECK_EQ(line, std::string("GET b"));
    CHECK(!p.next_line(line));
    CHECK_EQ(p.buffered(), std::size_t{0});
    feed("x");
    CHECK(!p.overflow(4)); feed("yyyy"); CHECK(p.overflow(4));

    // Tokenising: SET's value is the rest of the line, spaces included.
    kv::Command c = kv::parse_command("SET  key   hello big   world ");
    CHECK_EQ(c.argv.size(), std::size_t{5});
    CHECK_EQ(std::string(c.rest_after_argv(1)), std::string("hello big   world "));

    // Dispatch.
    kv::Store store(4);
    kv::Stats stats;
    kv::Dispatcher d(store, stats, nullptr, "test");
    auto run = [&](const char* l) { return d.handle(l).response; };
    CHECK_EQ(run("PING"), std::string("PONG\n"));
    CHECK_EQ(run("set name  Abheenash Rajolu"), std::string("OK\n"));
    CHECK_EQ(run("GET name"), std::string("Abheenash Rajolu\n"));
    CHECK_EQ(run("GET missing"), std::string("(nil)\n"));
    CHECK_EQ(run("INCR n"), std::string("(integer) 1\n"));
    CHECK_EQ(run("INCRBY n 41"), std::string("(integer) 42\n"));
    CHECK_EQ(run("DECR n"), std::string("(integer) 41\n"));
    CHECK_EQ(run("INCR name"), std::string("ERR value is not an integer\n"));
    CHECK_EQ(run("EXISTS n"), std::string("(integer) 1\n"));
    CHECK_EQ(run("DEL n name nope"), std::string("(integer) 2\n"));
    CHECK_EQ(run("SETNX x 1"), std::string("(integer) 1\n"));
    CHECK_EQ(run("SETNX x 2"), std::string("(integer) 0\n"));
    CHECK_EQ(run("PEXPIRE x 5000"), std::string("(integer) 1\n"));
    CHECK(run("PTTL x").rfind("(integer) 4", 0) == 0);
    CHECK_EQ(run("TTL x"), std::string("(integer) 5\n"));
    CHECK_EQ(run("PERSIST x"), std::string("(integer) 1\n"));
    CHECK_EQ(run("TTL x"), std::string("(integer) -1\n"));
    CHECK_EQ(run("TTL zzz"), std::string("(integer) -2\n"));
    CHECK_EQ(run("MGET x zzz"), std::string("*2\n1\n(nil)\n"));
    run("SET k:1 a"); run("SET k:2 b");
    CHECK_EQ(run("KEYS k:"), std::string("*2\nk:1\nk:2\n"));
    CHECK_EQ(run("DBSIZE"), std::string("(integer) 3\n"));
    CHECK_EQ(run("BOGUS 1 2"), std::string("ERR unknown command 'BOGUS'\n"));
    CHECK_EQ(run("GET"), std::string("ERR usage: GET key\n"));
    CHECK_EQ(run("COMPACT"), std::string("ERR no append-only file configured\n"));
    CHECK(run("INFO").rfind("*", 0) == 0);
    auto q = d.handle("QUIT");
    CHECK(q.close && q.response == "OK\n");
    CHECK_EQ(run("FLUSHALL"), std::string("OK\n"));
    CHECK_EQ(run("DBSIZE"), std::string("(integer) 0\n"));
    CHECK(stats.errors.load() == 4);
    std::printf("protocol: ok\n");
    return 0;
}
