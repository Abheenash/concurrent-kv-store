// kvserver — a concurrent TCP key-value store.
//
//   kvserver [--port 5555] [--bind 0.0.0.0] [--mode poll|thread] [--io-threads N]
//            [--shards N] [--aof path] [--fsync always|everysec|no] [--sweep-ms N]
//
// Talk to it with nc:   printf 'SET a 1\nINCR a\nGET a\n' | nc localhost 5555
#include "aof.hpp"
#include "protocol.hpp"
#include "server.hpp"
#include "store.hpp"

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>

namespace {
void usage() {
    std::fprintf(stderr,
        "usage: kvserver [--port 5555] [--bind 0.0.0.0] [--mode poll|thread] [--io-threads N]\n"
        "                [--shards N] [--aof path] [--fsync always|everysec|no] [--sweep-ms N]\n");
}
}  // namespace

int main(int argc, char** argv) {
    kv::ServerOptions so;
    std::size_t shards = 64;
    std::string aof_path;
    kv::Fsync fsync = kv::Fsync::EverySec;

    for (int i = 1; i < argc; ++i) {
        std::string k = argv[i];
        auto next = [&]() -> const char* { if (i + 1 >= argc) { usage(); std::exit(2); } return argv[++i]; };
        if (k == "--port") so.port = static_cast<std::uint16_t>(std::atoi(next()));
        else if (k == "--bind") so.bind = next();
        else if (k == "--mode") {
            std::string m = next();
            if (m == "poll") so.mode = kv::ServerOptions::Mode::Poll;
            else if (m == "thread") so.mode = kv::ServerOptions::Mode::Thread;
            else { usage(); return 2; }
        }
        else if (k == "--io-threads") so.io_threads = std::atoi(next());
        else if (k == "--shards") shards = static_cast<std::size_t>(std::atoi(next()));
        else if (k == "--aof") aof_path = next();
        else if (k == "--fsync") { if (!kv::parse_fsync(next(), fsync)) { usage(); return 2; } }
        else if (k == "--sweep-ms") so.sweep_ms = std::atoi(next());
        else { usage(); return 2; }
    }

    // Block SIGINT/SIGTERM in every thread (threads inherit the mask), then wait for
    // one with sigwait() — no async-signal-safety worries, and a clean shutdown path.
    // A shell starts background jobs with SIGINT ignored, and an ignored signal is
    // discarded before it can become pending — so reset both to default first.
    std::signal(SIGINT, SIG_DFL);
    std::signal(SIGTERM, SIG_DFL);
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGINT);
    sigaddset(&set, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &set, nullptr);
    std::signal(SIGPIPE, SIG_IGN);  // a client that vanishes mid-write must not kill the server

    kv::Store store(shards);
    kv::Stats stats;
    std::unique_ptr<kv::Aof> aof;
    std::string mode_info = std::string(kv::Server::mode_name(so.mode)) +
        (so.mode == kv::ServerOptions::Mode::Poll ? " x" + std::to_string(so.io_threads > 0 ? so.io_threads : (int)std::thread::hardware_concurrency()) : "");
    if (!aof_path.empty()) aof = std::make_unique<kv::Aof>(aof_path, fsync);
    kv::Dispatcher dispatcher(store, stats, aof.get(), mode_info);
    if (aof) {
        std::size_t n = aof->open([&](std::string_view line) { dispatcher.replay(line); });
        std::printf("aof: replayed %zu lines from %s (%zu keys), fsync=%s\n", n, aof_path.c_str(), store.size(),
                    fsync == kv::Fsync::Always ? "always" : fsync == kv::Fsync::EverySec ? "everysec" : "no");
    }

    kv::Server server(so, store, dispatcher, stats);
    try {
        server.start();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "kvserver: %s\n", e.what());
        return 1;
    }
    std::printf("kvserver listening on %s:%u  mode=%s  shards=%zu  aof=%s\n", so.bind.c_str(), server.port(),
                mode_info.c_str(), shards, aof_path.empty() ? "off" : aof_path.c_str());
    std::fflush(stdout);

    int sig = 0;
    sigwait(&set, &sig);
    std::printf("\nkvserver: signal %d, shutting down (%llu commands served)\n", sig,
                static_cast<unsigned long long>(stats.commands.load()));
    server.stop();
    return 0;
}
