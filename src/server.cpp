#include "server.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#if defined(__APPLE__) || defined(__FreeBSD__)
#include <sys/event.h>
#else
#include <sys/epoll.h>
#endif

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <system_error>
#include <unordered_map>

namespace kv {

namespace {

void set_nonblocking(int fd) {
    int fl = ::fcntl(fd, F_GETFL, 0);
    ::fcntl(fd, F_SETFL, fl | O_NONBLOCK);
}
// BSD/macOS accepted sockets inherit O_NONBLOCK from the listener; Linux ones don't.
// Set it explicitly either way so both modes behave the same on both platforms.
void set_blocking(int fd) {
    int fl = ::fcntl(fd, F_GETFL, 0);
    ::fcntl(fd, F_SETFL, fl & ~O_NONBLOCK);
}
void set_nodelay(int fd) {
    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
}
// Write everything (blocking socket). Returns false if the peer went away.
bool write_all(int fd, const std::string& s) {
    std::size_t off = 0;
    while (off < s.size()) {
        ssize_t n = ::write(fd, s.data() + off, s.size() - off);
        if (n < 0) { if (errno == EINTR) continue; return false; }
        off += static_cast<std::size_t>(n);
    }
    return true;
}

}  // namespace

// A reactor owns a set of connections and a wake-up pipe. The acceptor hands it
// new fds through `pending` and writes one byte to the pipe so poll() returns.
struct Server::Reactor {
    int wake_r = -1, wake_w = -1;
    int kq = -1;  // kqueue / epoll descriptor (event mode)
    std::thread thread;
    std::unordered_map<int, ConnState> conns;
    std::vector<pollfd> fds;  // poll mode
};

const char* Server::mode_name(ServerOptions::Mode m) {
    switch (m) {
        case ServerOptions::Mode::Thread: return "thread-per-connection";
        case ServerOptions::Mode::Poll: return "poll";
        case ServerOptions::Mode::Event:
#if defined(__APPLE__) || defined(__FreeBSD__)
            return "kqueue";
#else
            return "epoll";
#endif
    }
    return "?";
}

Server::Server(ServerOptions opts, Store& store, Dispatcher& dispatcher, Stats& stats)
    : opts_(std::move(opts)), store_(store), dispatcher_(dispatcher), stats_(stats) {}

Server::~Server() { stop(); }

void Server::start() {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) throw std::system_error(errno, std::generic_category(), "socket");
    int one = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(opts_.port);
    if (::inet_pton(AF_INET, opts_.bind.c_str(), &addr.sin_addr) != 1) throw std::runtime_error("bad bind address " + opts_.bind);
    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof addr) < 0) {
        throw std::system_error(errno, std::generic_category(), "bind port " + std::to_string(opts_.port));
    }
    if (::listen(listen_fd_, 1024) < 0) throw std::system_error(errno, std::generic_category(), "listen");
    set_nonblocking(listen_fd_);
    socklen_t len = sizeof addr;
    ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
    port_ = ntohs(addr.sin_port);

    if (opts_.mode != ServerOptions::Mode::Thread) {
        const bool ev = opts_.mode == ServerOptions::Mode::Event;
        int n = opts_.io_threads > 0 ? opts_.io_threads : static_cast<int>(std::thread::hardware_concurrency());
        if (n < 1) n = 1;
        for (int i = 0; i < n; ++i) {
            auto r = std::make_unique<Reactor>();
            int p[2];
            if (::pipe(p) != 0) throw std::system_error(errno, std::generic_category(), "pipe");
            r->wake_r = p[0]; r->wake_w = p[1];
            set_nonblocking(r->wake_r);
            r->fds.push_back(pollfd{r->wake_r, POLLIN, 0});
            r->fds.push_back(pollfd{listen_fd_, POLLIN, 0});  // every reactor accepts
            reactors_.push_back(std::move(r));
        }
        for (auto& r : reactors_) {
            r->thread = ev ? std::thread([this, &r] { event_loop(*r); }) : std::thread([this, &r] { reactor_loop(*r); });
        }
    } else {
        acceptor_ = std::thread([this] { accept_loop(); });
    }
    if (opts_.sweep_ms > 0) sweeper_ = std::thread([this] { sweep_loop(); });
}

void Server::stop() {
    if (stopping_.exchange(true)) return;
    if (acceptor_.joinable()) acceptor_.join();
    if (listen_fd_ >= 0) ::close(listen_fd_);
    // Thread mode: shutdown() every client socket so its blocking read() returns 0.
    {
        std::lock_guard<std::mutex> lk(conn_m_);
        for (int fd : conn_fds_) ::shutdown(fd, SHUT_RDWR);
    }
    {   // wait for the (detached) connection threads to notice and exit
        std::unique_lock<std::mutex> lk(conn_m_);
        conn_cv_.wait_for(lk, std::chrono::seconds(5), [&] { return conn_fds_.empty(); });
    }
    // Poll mode: poke every reactor; it sees stopping_ and closes its connections.
    for (auto& r : reactors_) { char b = 1; (void)!::write(r->wake_w, &b, 1); }
    for (auto& r : reactors_) {
        if (r->thread.joinable()) r->thread.join();
        ::close(r->wake_r); ::close(r->wake_w);
    }
    if (sweeper_.joinable()) sweeper_.join();
    listen_fd_ = -1;
}

void Server::sweep_loop() {
    while (!stopping_.load()) {
        for (int i = 0; i < opts_.sweep_ms && !stopping_.load(); i += 50) std::this_thread::sleep_for(std::chrono::milliseconds(50));
        if (stopping_.load()) break;
        stats_.expired_swept.fetch_add(store_.sweep_expired(), std::memory_order_relaxed);
    }
}

// Accept everything queued on the (non-blocking) listener. Returns the new fds.
std::vector<int> Server::drain_accept() {
    std::vector<int> out;
    while (!stopping_.load()) {
        int fd = ::accept(listen_fd_, nullptr, nullptr);
        if (fd < 0) {
            if (errno == EINTR) continue;
            if (errno != EAGAIN && errno != EWOULDBLOCK) std::this_thread::sleep_for(std::chrono::milliseconds(10));  // EMFILE etc.
            break;
        }
        set_nodelay(fd);
        stats_.connections_total.fetch_add(1, std::memory_order_relaxed);
        stats_.connections_open.fetch_add(1, std::memory_order_relaxed);
        out.push_back(fd);
    }
    return out;
}

// Thread mode only. In poll mode every reactor accepts for itself — no handoff, no
// single acceptor thread to starve while 500 clients connect at once.
void Server::accept_loop() {
    // The listener is non-blocking and polled with a timeout: closing a listening
    // socket from another thread does not reliably wake a blocked accept() (it
    // doesn't on macOS), so a blocking accept would hang shutdown. When it is
    // readable we drain the whole backlog — the kernel's queue is small (macOS caps
    // it at somaxconn = 128) and a burst of connects would otherwise be reset.
    pollfd lp{listen_fd_, POLLIN, 0};
    while (!stopping_.load()) {
        int ready = ::poll(&lp, 1, 100);
        if (ready <= 0) continue;
        for (int fd : drain_accept()) {
            set_blocking(fd);
            {
                std::lock_guard<std::mutex> lk(conn_m_);
                conn_fds_.insert(fd);
            }
            std::thread([this, fd] { serve_blocking(fd); }).detach();
        }
    }
}

// Feed bytes to the parser, dispatch every complete line, append responses to `out`.
bool Server::process_input(LineParser& parser, const char* data, std::size_t n, std::string& out, bool& close) {
    parser.feed(data, n);
    if (parser.overflow(opts_.max_line)) {
        out += "ERR line too long\n";
        close = true;
        return false;
    }
    std::string line;
    while (parser.next_line(line)) {
        if (line.empty()) continue;
        Dispatcher::Result r = dispatcher_.handle(line);
        out += r.response;
        if (r.close) { close = true; break; }
    }
    return true;
}

// ---------------------------------------------------------------- thread mode
void Server::serve_blocking(int fd) {
    LineParser parser;
    char buf[16384];
    std::string out;
    bool close = false;
    while (!close) {
        ssize_t n = ::read(fd, buf, sizeof buf);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) break;
        out.clear();
        process_input(parser, buf, static_cast<std::size_t>(n), out, close);
        if (!out.empty() && !write_all(fd, out)) break;
    }
    ::close(fd);
    stats_.connections_open.fetch_sub(1, std::memory_order_relaxed);
    std::lock_guard<std::mutex> lk(conn_m_);
    conn_fds_.erase(fd);
    conn_cv_.notify_all();
}

// ---------------------------------------------------------------- poll mode
void Server::reactor_loop(Reactor& r) {
    char buf[16384];
    auto drop = [&](std::size_t i) {
        int fd = r.fds[i].fd;
        ::close(fd);
        r.conns.erase(fd);
        r.fds[i] = r.fds.back();
        r.fds.pop_back();
        stats_.connections_open.fetch_sub(1, std::memory_order_relaxed);
    };

    while (true) {
        int ready = ::poll(r.fds.data(), static_cast<nfds_t>(r.fds.size()), -1);
        if (ready < 0) { if (errno == EINTR) continue; break; }

        // Wake-up pipe: only used to exit.
        if (r.fds[0].revents & POLLIN) {
            while (::read(r.wake_r, buf, sizeof buf) > 0) {}
            if (stopping_.load()) break;
        }
        // Listener: accept directly into this reactor.
        if (r.fds[1].revents & POLLIN) {
            for (int fd : drain_accept()) {
                set_nonblocking(fd);
                r.conns.emplace(fd, ConnState{});
                r.fds.push_back(pollfd{fd, POLLIN, 0});
            }
        }

        for (std::size_t i = 2; i < r.fds.size();) {
            pollfd& p = r.fds[i];
            ConnState& c = r.conns[p.fd];
            bool dead = false;

            // POLLHUP can arrive together with POLLIN while unread bytes are still
            // buffered (a client that sent and half-closed, like nc). Read first; the
            // read() returning 0 is what says the input side is finished.
            if (p.revents & (POLLERR | POLLNVAL)) dead = true;

            if (!dead && (p.revents & (POLLIN | POLLHUP))) {
                // Drain the socket: read until EAGAIN so one poll() round can process
                // everything a pipelined client has in flight.
                while (true) {
                    ssize_t n = ::read(p.fd, buf, sizeof buf);
                    if (n > 0) {
                        bool close = false;
                        process_input(c.parser, buf, static_cast<std::size_t>(n), c.out, close);
                        if (close) { c.close_after_flush = true; break; }
                        continue;
                    }
                    if (n == 0) {
                        // Peer closed its write side (nc does this at stdin EOF). Responses
                        // to what it already sent are still owed — flush them, then close.
                        c.close_after_flush = true;
                        break;
                    }
                    if (errno == EINTR) continue;
                    if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                    dead = true; break;
                }
            }

            if (!dead && !c.out.empty()) {
                ssize_t n = ::write(p.fd, c.out.data(), c.out.size());
                if (n > 0) c.out.erase(0, static_cast<std::size_t>(n));
                else if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) dead = true;
            }
            if (!dead && c.close_after_flush && c.out.empty()) dead = true;

            if (dead) { drop(i); continue; }
            // Only ask for POLLOUT while there's something left to send; a socket that is
            // always writable would otherwise make poll() return instantly forever. Once
            // the peer has half-closed (or sent QUIT) we stop asking for POLLIN.
            p.events = static_cast<short>((c.close_after_flush ? 0 : POLLIN) | (c.out.empty() ? 0 : POLLOUT));
            p.revents = 0;
            ++i;
        }
    }
    for (std::size_t i = 2; i < r.fds.size(); ++i) {
        ::close(r.fds[i].fd);
        stats_.connections_open.fetch_sub(1, std::memory_order_relaxed);
    }
    r.fds.resize(2);
    r.conns.clear();
}


// ---------------------------------------------------------------- shared connection step
bool Server::service_conn(int fd, ConnState& c, bool readable, bool writable, bool hangup, char* buf, std::size_t buflen) {
    bool dead = false;
    // A hangup can arrive with unread bytes still buffered (a client that sent and
    // half-closed, like nc). Read first; read() returning 0 is what ends the input side.
    if (readable || hangup) {
        while (true) {
            ssize_t n = ::read(fd, buf, buflen);
            if (n > 0) {
                bool close = false;
                process_input(c.parser, buf, static_cast<std::size_t>(n), c.out, close);
                if (close) { c.close_after_flush = true; break; }
                continue;
            }
            if (n == 0) { c.close_after_flush = true; break; }
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            dead = true; break;
        }
    }
    if (!dead && !c.out.empty() && (writable || readable || hangup)) {
        ssize_t n = ::write(fd, c.out.data(), c.out.size());
        if (n > 0) c.out.erase(0, static_cast<std::size_t>(n));
        else if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) dead = true;
    }
    if (!dead && c.close_after_flush && c.out.empty()) dead = true;
    return dead;
}

// ---------------------------------------------------------------- event mode
#if defined(__APPLE__) || defined(__FreeBSD__)

void Server::event_loop(Reactor& r) {
    char buf[16384];
    r.kq = ::kqueue();
    auto want = [&](int fd, bool read, bool write) {
        struct kevent ch[2];
        EV_SET(&ch[0], static_cast<uintptr_t>(fd), EVFILT_READ, read ? EV_ADD : EV_DELETE, 0, 0, nullptr);
        EV_SET(&ch[1], static_cast<uintptr_t>(fd), EVFILT_WRITE, write ? EV_ADD : EV_DELETE, 0, 0, nullptr);
        ::kevent(r.kq, ch, 2, nullptr, 0, nullptr);  // EV_DELETE of an absent filter just returns ENOENT
    };
    want(r.wake_r, true, false);
    want(listen_fd_, true, false);
    auto drop = [&](int fd) {
        ::close(fd);  // closing removes its kevents
        r.conns.erase(fd);
        stats_.connections_open.fetch_sub(1, std::memory_order_relaxed);
    };

    struct kevent evs[128];
    while (true) {
        int n = ::kevent(r.kq, nullptr, 0, evs, 128, nullptr);
        if (n < 0) { if (errno == EINTR) continue; break; }
        bool stop = false;
        for (int i = 0; i < n; ++i) {
            const int fd = static_cast<int>(evs[i].ident);
            if (fd == r.wake_r) {
                while (::read(r.wake_r, buf, sizeof buf) > 0) {}
                if (stopping_.load()) stop = true;
                continue;
            }
            if (fd == listen_fd_) {
                for (int nfd : drain_accept()) {
                    set_nonblocking(nfd);
                    r.conns.emplace(nfd, ConnState{});
                    want(nfd, true, false);
                }
                continue;
            }
            auto it = r.conns.find(fd);
            if (it == r.conns.end()) continue;  // dropped earlier in this batch
            ConnState& c = it->second;
            const bool readable = evs[i].filter == EVFILT_READ;
            const bool writable = evs[i].filter == EVFILT_WRITE;
            const bool hangup = (evs[i].flags & EV_EOF) != 0;
            if (service_conn(fd, c, readable, writable, hangup, buf, sizeof buf)) { drop(fd); continue; }
            want(fd, !c.close_after_flush, !c.out.empty());
        }
        if (stop) break;
    }
    for (auto& [fd, c] : r.conns) { ::close(fd); stats_.connections_open.fetch_sub(1, std::memory_order_relaxed); }
    r.conns.clear();
    ::close(r.kq); r.kq = -1;
}

#else

void Server::event_loop(Reactor& r) {
    char buf[16384];
    r.kq = ::epoll_create1(EPOLL_CLOEXEC);
    auto ctl = [&](int op, int fd, uint32_t events) {
        epoll_event ev{}; ev.events = events; ev.data.fd = fd;
        ::epoll_ctl(r.kq, op, fd, &ev);
    };
    ctl(EPOLL_CTL_ADD, r.wake_r, EPOLLIN);
    ctl(EPOLL_CTL_ADD, listen_fd_, EPOLLIN);
    auto drop = [&](int fd) {
        ::epoll_ctl(r.kq, EPOLL_CTL_DEL, fd, nullptr);
        ::close(fd);
        r.conns.erase(fd);
        stats_.connections_open.fetch_sub(1, std::memory_order_relaxed);
    };

    epoll_event evs[128];
    while (true) {
        int n = ::epoll_wait(r.kq, evs, 128, -1);
        if (n < 0) { if (errno == EINTR) continue; break; }
        bool stop = false;
        for (int i = 0; i < n; ++i) {
            const int fd = evs[i].data.fd;
            const uint32_t e = evs[i].events;
            if (fd == r.wake_r) {
                while (::read(r.wake_r, buf, sizeof buf) > 0) {}
                if (stopping_.load()) stop = true;
                continue;
            }
            if (fd == listen_fd_) {
                for (int nfd : drain_accept()) {
                    set_nonblocking(nfd);
                    r.conns.emplace(nfd, ConnState{});
                    ctl(EPOLL_CTL_ADD, nfd, EPOLLIN | EPOLLRDHUP);
                }
                continue;
            }
            auto it = r.conns.find(fd);
            if (it == r.conns.end()) continue;
            ConnState& c = it->second;
            if (e & EPOLLERR) { drop(fd); continue; }
            const bool readable = (e & EPOLLIN) != 0;
            const bool writable = (e & EPOLLOUT) != 0;
            const bool hangup = (e & (EPOLLHUP | EPOLLRDHUP)) != 0;
            if (service_conn(fd, c, readable, writable, hangup, buf, sizeof buf)) { drop(fd); continue; }
            ctl(EPOLL_CTL_MOD, fd, (c.close_after_flush ? 0u : (EPOLLIN | EPOLLRDHUP)) | (c.out.empty() ? 0u : EPOLLOUT));
        }
        if (stop) break;
    }
    for (auto& [fd, c] : r.conns) { ::close(fd); stats_.connections_open.fetch_sub(1, std::memory_order_relaxed); }
    r.conns.clear();
    ::close(r.kq); r.kq = -1;
}
#endif

}  // namespace kv
