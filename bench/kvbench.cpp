// kvbench — load generator for kvserver (or anything speaking the same protocol).
//
//   kvbench --port 5555 --clients 50 --requests 1000000 --pipeline 1 --get-ratio 0.9
//           --keyspace 100000 --value-size 32
//
// Each client runs on its own thread with its own connection and sends
// `pipeline` commands per round trip. Reports throughput and latency percentiles
// (per round trip, as redis-benchmark does).
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <random>
#include <string>
#include <thread>
#include <vector>

using clk = std::chrono::steady_clock;

struct Opts {
    std::string host = "127.0.0.1";
    int port = 5555;
    int clients = 50;
    long requests = 1'000'000;
    int pipeline = 1;
    double get_ratio = 0.9;
    int keyspace = 100'000;
    int value_size = 32;
    bool csv = false;
    std::string label;
};

static int connect_to(const Opts& o) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_port = htons(static_cast<std::uint16_t>(o.port));
    ::inet_pton(AF_INET, o.host.c_str(), &a.sin_addr);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof a) != 0) { std::perror("connect"); std::exit(1); }
    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    return fd;
}

// Read until `want` newline-terminated responses have arrived.
static bool read_responses(int fd, int want, std::string& buf) {
    int got = 0;
    char tmp[65536];
    buf.clear();
    while (got < want) {
        ssize_t n = ::read(fd, tmp, sizeof tmp);
        if (n <= 0) return false;
        for (ssize_t i = 0; i < n; ++i) if (tmp[i] == '\n') ++got;
        buf.append(tmp, static_cast<std::size_t>(n));
    }
    return true;
}

int main(int argc, char** argv) {
    Opts o;
    for (int i = 1; i < argc; ++i) {
        std::string k = argv[i];
        auto v = [&] { return argv[++i]; };
        if (k == "--host") o.host = v();
        else if (k == "--port") o.port = std::atoi(v());
        else if (k == "--clients") o.clients = std::atoi(v());
        else if (k == "--requests") o.requests = std::atol(v());
        else if (k == "--pipeline") o.pipeline = std::atoi(v());
        else if (k == "--get-ratio") o.get_ratio = std::atof(v());
        else if (k == "--keyspace") o.keyspace = std::atoi(v());
        else if (k == "--value-size") o.value_size = std::atoi(v());
        else if (k == "--csv") o.csv = true;
        else if (k == "--label") o.label = v();
        else { std::fprintf(stderr, "unknown option %s\n", k.c_str()); return 2; }
    }
    const std::string value(static_cast<std::size_t>(o.value_size), 'x');
    const long per_client = o.requests / o.clients;
    std::vector<std::vector<double>> lat(static_cast<std::size_t>(o.clients));
    std::atomic<long> done{0};
    std::atomic<long> errors{0};

    // Pre-populate so GETs hit real keys.
    {
        int fd = connect_to(o);
        std::string batch, resp;
        for (int k = 0; k < o.keyspace; ++k) {
            batch += "SET k" + std::to_string(k) + " " + value + "\n";
            if (batch.size() > 60000 || k + 1 == o.keyspace) {
                int lines = static_cast<int>(std::count(batch.begin(), batch.end(), '\n'));
                if (::write(fd, batch.data(), batch.size()) < 0 || !read_responses(fd, lines, resp)) { std::fprintf(stderr, "prepopulate failed\n"); return 1; }
                batch.clear();
            }
        }
        ::close(fd);
    }

    auto t0 = clk::now();
    std::vector<std::thread> threads;
    for (int c = 0; c < o.clients; ++c) {
        threads.emplace_back([&, c] {
            int fd = connect_to(o);
            std::mt19937 rng(static_cast<unsigned>(c * 7919 + 1));
            std::uniform_int_distribution<int> key(0, o.keyspace - 1);
            std::uniform_real_distribution<double> coin(0.0, 1.0);
            std::string batch, resp;
            auto& mine = lat[static_cast<std::size_t>(c)];
            mine.reserve(static_cast<std::size_t>(per_client / o.pipeline + 1));
            for (long sent = 0; sent < per_client; sent += o.pipeline) {
                batch.clear();
                const int n = static_cast<int>(std::min<long>(o.pipeline, per_client - sent));
                for (int i = 0; i < n; ++i) {
                    const std::string k = "k" + std::to_string(key(rng));
                    if (coin(rng) < o.get_ratio) batch += "GET " + k + "\n";
                    else batch += "SET " + k + " " + value + "\n";
                }
                auto a = clk::now();
                if (::write(fd, batch.data(), batch.size()) < 0 || !read_responses(fd, n, resp)) { errors.fetch_add(1); break; }
                mine.push_back(std::chrono::duration<double, std::micro>(clk::now() - a).count());
                if (resp.compare(0, 3, "ERR") == 0) errors.fetch_add(1);
                done.fetch_add(n, std::memory_order_relaxed);
            }
            ::close(fd);
        });
    }
    for (auto& t : threads) t.join();
    const double secs = std::chrono::duration<double>(clk::now() - t0).count();

    std::vector<double> all;
    for (auto& v : lat) all.insert(all.end(), v.begin(), v.end());
    std::sort(all.begin(), all.end());
    auto pct = [&](double p) { return all.empty() ? 0.0 : all[std::min(all.size() - 1, static_cast<std::size_t>(p * all.size()))]; };
    const double rps = done.load() / secs;

    if (o.csv) {
        std::printf("%s,%d,%d,%.2f,%ld,%.3f,%.0f,%.1f,%.1f,%.1f,%.1f,%ld\n", o.label.c_str(), o.clients, o.pipeline, o.get_ratio,
                    done.load(), secs, rps, pct(0.50), pct(0.90), pct(0.99), pct(0.999), errors.load());
    } else {
        std::printf("%s%ld requests  clients=%d  pipeline=%d  get-ratio=%.2f  keyspace=%d  value=%dB\n",
                    o.label.empty() ? "" : (o.label + ": ").c_str(), done.load(), o.clients, o.pipeline, o.get_ratio, o.keyspace, o.value_size);
        std::printf("  %.3f s   %.0f req/s   latency per round-trip: p50 %.1f µs  p90 %.1f µs  p99 %.1f µs  p99.9 %.1f µs   errors=%ld\n",
                    secs, rps, pct(0.50), pct(0.90), pct(0.99), pct(0.999), errors.load());
    }
    return 0;
}
