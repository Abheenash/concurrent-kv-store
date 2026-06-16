#include <iostream>
#include <string>
#include <sstream>
#include <unordered_map>
#include <mutex>
#include <thread>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

std::unordered_map<std::string, std::string> store;
std::mutex storeMutex;

// Handle one client connection: read commands, send back responses.
void handleClient(int clientFd) {
    char buf[1024];
    while (true) {
        ssize_t n = read(clientFd, buf, sizeof(buf) - 1);
        if (n <= 0) break;                 // client disconnected
        buf[n] = '\0';

        std::istringstream iss(buf);
        std::string cmd, key, value;
        iss >> cmd >> key;

        std::string response;
        if (cmd == "SET") {
            iss >> value;
            {
                std::lock_guard<std::mutex> lock(storeMutex);
                store[key] = value;
            }
            response = "OK\n";
        } else if (cmd == "GET") {
            std::lock_guard<std::mutex> lock(storeMutex);
            auto it = store.find(key);
            response = (it != store.end()) ? it->second + "\n" : "(nil)\n";
        } else {
            response = "ERR use: SET key value  /  GET key\n";
        }
        write(clientFd, response.c_str(), response.size());
    }
    close(clientFd);
}

int main() {
    int port = 5555;
    int serverFd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(serverFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(serverFd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "bind failed (is port " << port << " already in use?)\n";
        return 1;
    }
    listen(serverFd, 16);
    std::cout << "Key-value server listening on port " << port << "\n";

    while (true) {
        int clientFd = accept(serverFd, nullptr, nullptr);
        if (clientFd < 0) continue;
        std::thread(handleClient, clientFd).detach();   // one thread per client
    }
    return 0;
}
