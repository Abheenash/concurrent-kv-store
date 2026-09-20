// End-to-end over real sockets, for both I/O models: an in-process server on an
// ephemeral port, clients that pipeline, split commands across packets, disconnect
// mid-line, and 50 connections doing INCR on one key at once.
#include "../src/protocol.hpp"
#include "../src/server.hpp"
#include "../src/store.hpp"
#include "check.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <csignal>
#include <string>
#include <thread>
#include <vector>

static int connect_local(std::uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(port); a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    CHECK(::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof a) == 0);
    return fd;
}
static void send_all(int fd, const std::string& s) { CHECK(::write(fd, s.data(), s.size()) == static_cast<ssize_t>(s.size())); }
static std::string read_lines(int fd, int want) {
    std::string out; char buf[4096]; int got = 0;
    while (got < want) {
        ssize_t n = ::read(fd, buf, sizeof buf);
        CHECK(n > 0);
        for (ssize_t i = 0; i < n; ++i) if (buf[i] == '\n') ++got;
        out.append(buf, static_cast<std::size_t>(n));
    }
    return out;
}

static void run_suite(kv::ServerOptions::Mode mode) {
    kv::Store store(16); kv::Stats stats;
    kv::Dispatcher d(store, stats, nullptr, "test");
    kv::ServerOptions o; o.port = 0; o.mode = mode; o.io_threads = 2; o.sweep_ms = 50; o.max_line = 256;
    kv::Server server(o, store, d, stats);
    server.start();
    const std::uint16_t port = server.port();
    CHECK(port != 0);

    // Pipelining: 3 commands in one packet -> 3 responses.
    int fd = connect_local(port);
    send_all(fd, "SET a 1\nINCR a\nGET a\n");
    CHECK_EQ(read_lines(fd, 3), std::string("OK\n(integer) 2\n2\n"));
    // One command split across two packets, with CRLF.
    send_all(fd, "GE"); std::this_thread::sleep_for(std::chrono::milliseconds(5)); send_all(fd, "T a\r\n");
    CHECK_EQ(read_lines(fd, 1), std::string("2\n"));
    // A value with spaces round-trips.
    send_all(fd, "SET msg hello big world\nGET msg\n");
    CHECK_EQ(read_lines(fd, 2), std::string("OK\nhello big world\n"));
    // TTL expiry is observed over the wire.
    send_all(fd, "SET t v\nPEXPIRE t 20\nSET u v\nPEXPIRE u 20\n"); read_lines(fd, 4);
    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    send_all(fd, "GET t\n"); CHECK_EQ(read_lines(fd, 1), std::string("(nil)\n"));  // lazy expiry on read
    // `u` is never read, so only the background sweep can remove it — wait for it, don't assume timing.
    for (int i = 0; i < 200 && stats.expired_swept.load() < 1; ++i) std::this_thread::sleep_for(std::chrono::milliseconds(10));
    CHECK(stats.expired_swept.load() >= 1);
    // QUIT closes the connection after replying.
    send_all(fd, "QUIT\n"); CHECK_EQ(read_lines(fd, 1), std::string("OK\n"));
    char c; CHECK(::read(fd, &c, 1) == 0);
    ::close(fd);

    // A line longer than max_line is rejected and the client dropped, server unaffected.
    fd = connect_local(port);
    send_all(fd, std::string(300, 'x'));
    CHECK(read_lines(fd, 1).find("ERR line too long") != std::string::npos);
    ::close(fd);

    // A client that sends and half-closes (nc at stdin EOF) still gets every response.
    fd = connect_local(port);
    send_all(fd, "SET hc 1\nINCR hc\nGET hc\n");
    ::shutdown(fd, SHUT_WR);
    CHECK_EQ(read_lines(fd, 3), std::string("OK\n(integer) 2\n2\n"));
    CHECK(::read(fd, &c, 1) == 0);  // and the server closes after flushing
    ::close(fd);

    // Disconnecting mid-command must not disturb anyone else.
    fd = connect_local(port); send_all(fd, "SET half"); ::close(fd);

    // 50 concurrent clients x 200 INCR on the same key == exactly 10,000.
    std::vector<std::thread> ts;
    for (int t = 0; t < 50; ++t) ts.emplace_back([&] {
        int c2 = connect_local(port);
        for (int i = 0; i < 200; ++i) { send_all(c2, "INCR hot\n"); read_lines(c2, 1); }
        ::close(c2);
    });
    for (auto& t : ts) t.join();
    CHECK_EQ(*store.get("hot"), std::string("10000"));

    // Wait for the server to notice the closes, then check the accounting.
    for (int i = 0; i < 100 && stats.connections_open.load() != 0; ++i) std::this_thread::sleep_for(std::chrono::milliseconds(10));
    CHECK_EQ(stats.connections_open.load(), std::uint64_t{0});
    CHECK_EQ(stats.connections_total.load(), std::uint64_t{54});

    // Graceful stop with a client still connected.
    fd = connect_local(port);
    send_all(fd, "PING\n"); CHECK_EQ(read_lines(fd, 1), std::string("PONG\n"));
    server.stop();
    CHECK(::read(fd, &c, 1) <= 0);
    ::close(fd);
    std::printf("server (%s): ok\n", kv::Server::mode_name(mode));
}

int main() {
    std::signal(SIGPIPE, SIG_IGN);
    run_suite(kv::ServerOptions::Mode::Thread);
    run_suite(kv::ServerOptions::Mode::Poll);
    run_suite(kv::ServerOptions::Mode::Event);
    return 0;
}
