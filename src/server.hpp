// TCP server with two I/O models, selectable at startup:
//
//   thread  — one OS thread per connection, blocking reads. Simple, and fine up to
//             a few hundred clients; every idle client still costs a thread.
//   poll    — N reactor threads, each running poll() over its own connections
//             AND the shared listening socket, so each reactor accepts for itself
//             (non-blocking accept; the losers of the race just get EAGAIN). Idle
//             clients cost a pollfd, not a thread — but every wake-up scans all of them.
//   event   — the same reactors on kqueue (macOS/BSD) or epoll (Linux): the kernel
//             hands back only the descriptors that are ready, so a wake-up costs
//             O(active connections) instead of O(all connections).
//
// Both models read into a per-connection LineParser, dispatch every complete
// line, and batch all the responses from one read into one write — which is what
// makes pipelined clients fast.
#pragma once

#include "protocol.hpp"
#include "store.hpp"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

namespace kv {

struct ServerOptions {
    std::string bind = "0.0.0.0";
    std::uint16_t port = 5555;       // 0 = ephemeral (tests)
    enum class Mode { Thread, Poll, Event } mode = Mode::Event;
    int io_threads = 0;              // poll/event modes; 0 = hardware_concurrency()
    std::size_t max_line = 1 << 20;  // a client that sends 1 MB without a newline is dropped
    int sweep_ms = 1000;             // expired-key sweep interval; 0 = off
};

class Server {
public:
    Server(ServerOptions opts, Store& store, Dispatcher& dispatcher, Stats& stats);
    ~Server();

    void start();                    // bind, listen, spawn threads; throws on failure
    void stop();                     // graceful: stop accepting, close clients, join
    std::uint16_t port() const noexcept { return port_; }
    static const char* mode_name(ServerOptions::Mode m);

private:
    struct Reactor;
    struct ConnState {
        LineParser parser;
        std::string out;  // unsent response bytes
        bool close_after_flush = false;  // QUIT, or the peer half-closed: flush, then close
    };
    void accept_loop();
    std::vector<int> drain_accept();
    void sweep_loop();
    void serve_blocking(int fd);                       // thread mode
    void reactor_loop(Reactor& r);                     // poll mode
    void event_loop(Reactor& r);                       // event mode (kqueue / epoll)
    // Shared per-connection step for both reactors. Returns true when the connection is finished.
    bool service_conn(int fd, struct ConnState& c, bool readable, bool writable, bool hangup, char* buf, std::size_t buflen);
    bool process_input(LineParser& parser, const char* data, std::size_t n, std::string& out, bool& close);

    ServerOptions opts_;
    Store& store_;
    Dispatcher& dispatcher_;
    Stats& stats_;
    int listen_fd_ = -1;
    std::uint16_t port_ = 0;
    std::atomic<bool> stopping_{false};

    std::thread acceptor_;
    std::thread sweeper_;
    std::vector<std::unique_ptr<Reactor>> reactors_;

    std::mutex conn_m_;
    std::condition_variable conn_cv_;
    std::unordered_set<int> conn_fds_;                 // thread mode: for shutdown() and drain
};

}  // namespace kv
