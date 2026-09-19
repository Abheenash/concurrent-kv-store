#include "aof.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <fstream>
#include <stdexcept>
#include <system_error>

namespace kv {

bool parse_fsync(std::string_view s, Fsync& out) {
    if (s == "always") { out = Fsync::Always; return true; }
    if (s == "everysec") { out = Fsync::EverySec; return true; }
    if (s == "no") { out = Fsync::No; return true; }
    return false;
}

Aof::Aof(std::string path, Fsync policy) : path_(std::move(path)), policy_(policy) {}

Aof::~Aof() {
    stop_.store(true);
    cv_.notify_all();
    if (syncer_.joinable()) syncer_.join();
    if (fd_ >= 0) { ::fsync(fd_); ::close(fd_); }
}

std::size_t Aof::open(const std::function<void(std::string_view)>& apply) {
    std::size_t replayed = 0;
    {
        std::ifstream in(path_);
        std::string line;
        while (std::getline(in, line)) {
            if (!line.empty() && line.back() == '\r') line.pop_back();
            if (line.empty()) continue;
            apply(line);
            ++replayed;
        }
    }
    fd_ = ::open(path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd_ < 0) throw std::system_error(errno, std::generic_category(), "open " + path_);
    struct stat_size { static std::uint64_t of(int fd) { off_t o = ::lseek(fd, 0, SEEK_END); return o < 0 ? 0 : static_cast<std::uint64_t>(o); } };
    bytes_.store(stat_size::of(fd_));
    if (policy_ == Fsync::EverySec) syncer_ = std::thread([this] { sync_loop(); });
    return replayed;
}

void Aof::write_all(int fd, std::string_view s) {
    while (!s.empty()) {
        ssize_t n = ::write(fd, s.data(), s.size());
        if (n < 0) {
            if (errno == EINTR) continue;
            throw std::system_error(errno, std::generic_category(), "write " + path_);
        }
        s.remove_prefix(static_cast<std::size_t>(n));
    }
}

void Aof::append(std::string_view line) {
    std::string rec(line);
    rec += '\n';
    std::lock_guard<std::mutex> lk(m_);
    if (fd_ < 0) return;
    write_all(fd_, rec);
    bytes_.fetch_add(rec.size(), std::memory_order_relaxed);
    if (policy_ == Fsync::Always) ::fsync(fd_);
    else dirty_.store(true, std::memory_order_relaxed);
}

void Aof::sync_loop() {
    std::unique_lock<std::mutex> lk(m_);
    while (!stop_.load()) {
        cv_.wait_for(lk, std::chrono::seconds(1), [&] { return stop_.load(); });
        if (dirty_.exchange(false) && fd_ >= 0) ::fsync(fd_);
    }
}

// Snapshot the live store into a temp file, fsync it, then atomically rename it over
// the log. The store keeps serving reads during the walk; writes that land during the
// rewrite are appended to the new file after the swap, so nothing is lost.
void Aof::compact(const Store& store) {
    std::lock_guard<std::mutex> lk(m_);  // blocks appends for the duration of the rewrite
    const std::string tmp = path_ + ".compact";
    int nfd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (nfd < 0) throw std::system_error(errno, std::generic_category(), "open " + tmp);
    const auto now_unix = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::system_clock::now().time_since_epoch()).count();
    const auto now_steady = Clock::now();
    std::string buf;
    store.for_each([&](const std::string& key, const Entry& e) {
        buf += "SET " + key + " " + e.value + "\n";
        if (e.expires_at) {
            const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(*e.expires_at - now_steady).count();
            buf += "PEXPIREAT " + key + " " + std::to_string(now_unix + ms) + "\n";
        }
        if (buf.size() > (1u << 16)) { write_all(nfd, buf); buf.clear(); }
    });
    if (!buf.empty()) write_all(nfd, buf);
    ::fsync(nfd);
    if (::rename(tmp.c_str(), path_.c_str()) != 0) {
        ::close(nfd);
        throw std::system_error(errno, std::generic_category(), "rename " + tmp);
    }
    if (fd_ >= 0) ::close(fd_);
    fd_ = nfd;
    off_t o = ::lseek(fd_, 0, SEEK_END);
    bytes_.store(o < 0 ? 0 : static_cast<std::uint64_t>(o));
}

}  // namespace kv
