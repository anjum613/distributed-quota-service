#include "counter.hpp"
#include <arpa/inet.h>
#include <chrono>
#include <csignal>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <map>
#include <netinet/tcp.h>
#include <poll.h>
#include <sstream>
#include <sys/socket.h>
#include <unistd.h>
#include <unordered_set>
#include <vector>

using namespace quota;
namespace {
volatile std::sig_atomic_t running = 1;
void stop(int) {
    running = 0;
}
U now_ns() {
    return static_cast<U>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                              std::chrono::steady_clock::now().time_since_epoch())
                              .count());
}
std::vector<std::string> words(const std::string &s) {
    std::istringstream in(s);
    std::vector<std::string> out;
    std::string w;
    while (in >> w)
        out.push_back(w);
    return out;
}
std::string json_string(const std::string &s) {
    std::string out = "\"";
    for (unsigned char ch : s) {
        if (ch == '"' || ch == '\\') {
            out += '\\';
            out += static_cast<char>(ch);
        } else if (ch >= 32 && ch < 127)
            out += static_cast<char>(ch);
        else
            out += '?';
    }
    return out + '"';
}
std::string state_json(const State &s) {
    std::string out = "[";
    for (std::size_t k = 0; k < s.size(); ++k)
        out += (k ? "," : "") + std::to_string(s[k]);
    return out + "]";
}
sockaddr_in address(int port) {
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_port = htons(static_cast<uint16_t>(port));
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    return a;
}
int bind_socket(int type, int port) {
    int fd = socket(AF_INET, type | SOCK_NONBLOCK, 0);
    if (fd < 0)
        throw std::runtime_error("socket failed");
    int yes = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    auto a = address(port);
    if (bind(fd, reinterpret_cast<sockaddr *>(&a), sizeof(a)) < 0) {
        close(fd);
        throw std::runtime_error("bind failed");
    }
    return fd;
}
struct Config {
    int id = -1, base = 19000, relay = 0;
    std::string mode = "threshold", epoch, log;
    std::array<U, 3> allocation{34, 33, 33};
    U threshold = 8, batch = 16, max_success = 2000000;
    bool audit = true;
};
Config config(int argc, char **argv) {
    Config c;
    for (int k = 1; k < argc; ++k) {
        std::string key = argv[k];
        if (k + 1 == argc)
            throw std::invalid_argument("missing option value");
        std::string val = argv[++k];
        if (key == "--id")
            c.id = replica(number(val));
        else if (key == "--base" || key == "--relay") {
            U p = number(val);
            if (p < 1024 || p > 65000)
                throw std::invalid_argument("port out of range");
            (key == "--base" ? c.base : c.relay) = static_cast<int>(p);
        } else if (key == "--mode")
            c.mode = val;
        else if (key == "--epoch")
            c.epoch = val;
        else if (key == "--log")
            c.log = val;
        else if (key == "--threshold")
            c.threshold = number(val);
        else if (key == "--batch")
            c.batch = number(val);
        else if (key == "--max-success")
            c.max_success = number(val);
        else if (key == "--audit") {
            if (val != "0" && val != "1")
                throw std::invalid_argument("audit is 0 or 1");
            c.audit = val == "1";
        } else if (key == "--allocation") {
            for (char &ch : val)
                if (ch == ',')
                    ch = ' ';
            auto v = words(val);
            if (v.size() != 3)
                throw std::invalid_argument("three allocations required");
            for (int i = 0; i < 3; ++i)
                c.allocation[i] = number(v[static_cast<std::size_t>(i)]);
        } else
            throw std::invalid_argument("unknown option: " + key);
    }
    if (c.id < 0 || c.epoch.empty() || c.epoch.size() > 64 || c.log.empty() ||
        c.epoch.find_first_not_of(
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-") !=
            std::string::npos)
        throw std::invalid_argument("require --id, --epoch (alphanumeric), --log");
    if (c.mode != "central" && c.mode != "fixed" && c.mode != "threshold")
        throw std::invalid_argument("invalid mode");
    if (c.mode == "central" && (c.allocation[1] || c.allocation[2]))
        throw std::invalid_argument("central requires Q,0,0");
    if (!c.batch || !c.max_success || c.max_success > 2000000)
        throw std::invalid_argument("invalid resource limit");
    return c;
}
struct Connection {
    std::string input, output;
    bool busy = false;
    U rid = 0, deadline = 0;
};
class Server {
    Config c;
    Counter counter;
    int tcp, udp;
    std::ofstream log;
    U event_seq = 0, message_seq = 0, tx = 0, bytes = 0, rx = 0, accepted = 0, deferred = 0,
      duplicates = 0;
    U next_gossip = 0, next_borrow = 0, last_demand = 0;
    bool quiet = false, gossip = true;
    std::map<int, Connection> clients;
    std::unordered_set<U> home_success, owner_success;

    void event(const std::string &kind, const std::string &fields = "") {
        if (!c.audit && kind != "init" && kind != "snapshot" && kind != "stop")
            return;
        log << "{\"node\":" << c.id << ",\"seq\":" << ++event_seq << ",\"ns\":" << now_ns()
            << ",\"kind\":" << json_string(kind) << fields << "}\n";
        if (!log)
            throw std::runtime_error("audit write failed");
    }
    void send_peer(int dest, const std::string &payload) {
        U mid = plus(message_seq, 1);
        message_seq = mid;
        std::string wire = c.epoch + " " + std::to_string(c.id) + " " + std::to_string(dest) + " " +
                           std::to_string(mid) + " " + payload;
        event("send", ",\"mid\":" + std::to_string(mid) + ",\"to\":" + std::to_string(dest) +
                          ",\"payload\":" + json_string(payload));
        auto a = address(c.relay ? c.relay : c.base + 10 + dest);
        auto n =
            sendto(udp, wire.data(), wire.size(), 0, reinterpret_cast<sockaddr *>(&a), sizeof(a));
        if (n >= 0) {
            ++tx;
            bytes += static_cast<U>(n);
        }
    }
    void send_state(int dest) {
        std::string s = "STATE";
        for (auto n : counter.state())
            s += ' ' + std::to_string(n);
        send_peer(dest, s);
    }
    std::string status() {
        return "{\"node\":" + std::to_string(c.id) + ",\"mode\":" + json_string(c.mode) +
               ",\"pid\":" + std::to_string(getpid()) +
               ",\"available\":" + std::to_string(counter.available()) +
               ",\"state\":" + state_json(counter.state()) +
               ",\"accepted_home\":" + std::to_string(accepted) +
               ",\"deferred\":" + std::to_string(deferred) +
               ",\"duplicates\":" + std::to_string(duplicates) +
               ",\"peer_tx\":" + std::to_string(tx) + ",\"peer_bytes\":" + std::to_string(bytes) +
               ",\"peer_rx\":" + std::to_string(rx) + "}";
    }
    void reply(int fd, U rid, const std::string &result) {
        event("reply", ",\"rid\":" + std::to_string(rid) + ",\"result\":" + json_string(result));
        auto it = clients.find(fd);
        if (it != clients.end()) {
            it->second.output += result + " " + request_text(rid) + "\n";
            it->second.busy = false;
        }
    }
    void succeed(int fd, U rid) {
        bool fresh = home_success.insert(rid).second;
        if (fresh)
            ++accepted;
        else
            ++duplicates;
        reply(fd, rid, fresh ? "OK NEW" : "OK DUP");
    }
    void defer(int fd, U rid, const std::string &why) {
        ++deferred;
        reply(fd, rid, "DEFER " + why);
    }
    bool spend(U rid) {
        if (!counter.spend())
            return false;
        event("spend", ",\"rid\":" + std::to_string(rid));
        return true;
    }
    void transfer(int dest, U amount) {
        if (counter.transfer(dest, amount)) {
            event("transfer",
                  ",\"to\":" + std::to_string(dest) + ",\"amount\":" + std::to_string(amount));
            send_state(dest);
        }
    }
    void request(int fd, U rid, U time) {
        if (static_cast<int>(rid % 3) != c.id)
            throw std::invalid_argument("wrong home server");
        last_demand = time;
        if (home_success.contains(rid)) {
            succeed(fd, rid);
            return;
        }
        if (home_success.size() >= c.max_success) {
            defer(fd, rid, "CAPACITY");
            return;
        }
        if (c.mode == "central" && c.id != 0) {
            auto &cl = clients.at(fd);
            cl.busy = true;
            cl.rid = rid;
            cl.deadline = time + 100000000;
            send_peer(0, "ASK " + request_text(rid));
            return;
        }
        if (c.mode == "central" && owner_success.contains(rid)) {
            succeed(fd, rid);
            return;
        }
        if (c.mode == "central" && owner_success.size() >= c.max_success) {
            defer(fd, rid, "CAPACITY");
            return;
        }
        if (spend(rid)) {
            if (c.mode == "central")
                owner_success.insert(rid);
            succeed(fd, rid);
        } else
            defer(fd, rid, "NO_RIGHTS");
    }
    void command(int fd, const std::string &line, U time) {
        auto v = words(line);
        if (v.size() == 2 && v[0] == "REQ")
            request(fd, request_id(v[1]), time);
        else if (v.size() == 1 && v[0] == "STATUS") {
            event("snapshot", ",\"state\":" + state_json(counter.state()));
            log.flush();
            clients.at(fd).output += status() + "\n";
        } else if (v.size() == 1 && v[0] == "QUIET") {
            quiet = true;
            clients.at(fd).output += "OK QUIET\n";
        } else if (v.size() == 1 && v[0] == "PAUSE") {
            gossip = false;
            clients.at(fd).output += "OK PAUSE\n";
        } else if (v.size() == 1 && v[0] == "SYNC") {
            for (int j = 0; j < 3; ++j)
                if (j != c.id)
                    send_state(j);
            clients.at(fd).output += "OK SYNC\n";
        } else if (v.size() == 3 && v[0] == "TRANSFER" && c.mode == "threshold") {
            int dest = replica(number(v[1]));
            U n = number(v[2]);
            if (dest == c.id || !n)
                throw std::invalid_argument("invalid transfer");
            if (n > counter.available())
                clients.at(fd).output += "DEFER NO_RIGHTS\n";
            else {
                transfer(dest, n);
                clients.at(fd).output += "OK TRANSFER\n";
            }
        } else
            throw std::invalid_argument("invalid command");
    }
    void receive() {
        for (int budget = 0; budget < 128; ++budget) {
            char buf[4096];
            sockaddr_in a{};
            socklen_t len = sizeof(a);
            auto n = recvfrom(udp, buf, sizeof(buf), 0, reinterpret_cast<sockaddr *>(&a), &len);
            if (n < 0)
                break;
            try {
                if (a.sin_addr.s_addr != htonl(INADDR_LOOPBACK) || n == sizeof(buf))
                    continue;
                auto v = words(std::string(buf, static_cast<std::size_t>(n)));
                if (v.size() < 5 || v[0] != c.epoch)
                    continue;
                int from = replica(number(v[1])), dest = replica(number(v[2]));
                U mid = number(v[3]);
                if (from == c.id || dest != c.id ||
                    ntohs(a.sin_port) != (c.relay ? c.relay : c.base + 10 + from))
                    continue;
                std::string payload = v[4];
                for (std::size_t k = 5; k < v.size(); ++k)
                    payload += ' ' + v[k];
                ++rx;
                event("recv", ",\"from\":" + std::to_string(from) + ",\"mid\":" +
                                  std::to_string(mid) + ",\"payload\":" + json_string(payload));
                if (v[4] == "STATE" && v.size() == 17) {
                    State s{};
                    for (std::size_t k = 0; k < s.size(); ++k)
                        s[k] = number(v[k + 5]);
                    counter.merge(s);
                    event("merge",
                          ",\"from\":" + std::to_string(from) + ",\"mid\":" + std::to_string(mid));
                } else if (v[4] == "NEED" && v.size() == 6 && c.mode == "threshold" && !quiet) {
                    U target = number(v[5]), sent = counter.sent_to(from);
                    if (target > sent && counter.available())
                        transfer(from, std::min(target - sent, counter.available()));
                    else
                        send_state(from); // retransmit cumulative grant, never repeat an increment
                } else if (v[4] == "ASK" && v.size() == 6 && c.mode == "central" && c.id == 0) {
                    U rid = request_id(v[5]);
                    if (static_cast<int>(rid % 3) != from)
                        throw std::invalid_argument("wrong RPC home");
                    std::string result;
                    if (owner_success.contains(rid))
                        result = "OK";
                    else if (owner_success.size() >= c.max_success)
                        result = "CAPACITY";
                    else if (spend(rid)) {
                        owner_success.insert(rid);
                        result = "OK";
                    } else
                        result = "NO_RIGHTS";
                    send_peer(from, "ANSWER " + v[5] + " " + result);
                } else if (v[4] == "ANSWER" && v.size() == 7 && c.mode == "central" && from == 0) {
                    U rid = request_id(v[5]);
                    if (static_cast<int>(rid % 3) != c.id)
                        throw std::invalid_argument("wrong answer home");
                    if (v[6] != "OK" && v[6] != "CAPACITY" && v[6] != "NO_RIGHTS")
                        throw std::invalid_argument("bad answer");
                    // Late OK remains useful, but only cache it when a caller awaits this ID.
                    for (auto &[fd, cl] : clients)
                        if (cl.busy && cl.rid == rid) {
                            if (v[6] == "OK")
                                succeed(fd, rid);
                            else if (home_success.contains(rid))
                                succeed(fd, rid);
                            else
                                defer(fd, rid, v[6]);
                        }
                }
            } catch (const std::invalid_argument &) {
                event("invalid_peer");
            } catch (const std::overflow_error &) {
                event("invalid_peer");
            }
        }
    }
    void timers(U time) {
        if (gossip && time >= next_gossip) {
            for (int j = 0; j < 3; ++j)
                if (j != c.id)
                    send_state(j);
            next_gossip = time + 50000000;
        }
        // Demand merely enables a low-watermark rule. No predictor or demand model.
        if (c.mode == "threshold" && !quiet && time - last_demand < 100000000 &&
            time >= next_borrow && counter.available() <= c.threshold) {
            for (int j = 0; j < 3; ++j)
                if (j != c.id) {
                    U old = counter.received_from(j);
                    U delta = std::min(c.batch, std::numeric_limits<U>::max() - old);
                    if (delta)
                        send_peer(j, "NEED " + std::to_string(old + delta));
                }
            next_borrow = time + 20000000;
        }
        for (auto &[fd, cl] : clients)
            if (cl.busy && time >= cl.deadline)
                defer(fd, cl.rid, "OWNER_TIMEOUT");
    }

  public:
    explicit Server(Config cfg)
        : c(std::move(cfg)), counter(c.id, c.allocation),
          tcp(bind_socket(SOCK_STREAM, c.base + c.id)),
          udp(bind_socket(SOCK_DGRAM, c.base + 10 + c.id)), log(c.log) {
        if (!log || listen(tcp, 128) < 0)
            throw std::runtime_error("log/listen failed");
        event("init", ",\"mode\":" + json_string(c.mode) + ",\"epoch\":" + json_string(c.epoch) +
                          ",\"audit\":" + (c.audit ? "true" : "false") +
                          ",\"state\":" + state_json(counter.state()));
        log.flush();
        std::cout << "READY server=" << c.id << " pid=" << getpid() << " mode=" << c.mode
                  << " client=" << c.base + c.id << " peer=" << c.base + 10 + c.id << std::endl;
    }
    ~Server() {
        for (auto &[fd, cl] : clients) {
            (void)cl;
            close(fd);
        }
        close(tcp);
        close(udp);
    }
    void run() {
        while (running) {
            std::vector<pollfd> p{{tcp, POLLIN, 0}, {udp, POLLIN, 0}};
            for (auto &[fd, cl] : clients)
                p.push_back(
                    {fd,
                     static_cast<short>((cl.busy ? 0 : POLLIN) | (cl.output.empty() ? 0 : POLLOUT)),
                     0});
            if (poll(p.data(), p.size(), 2) < 0 && errno != EINTR)
                throw std::runtime_error("poll failed");
            if (p[0].revents & POLLIN) {
                for (int b = 0; b < 32; ++b) {
                    int fd = accept4(tcp, nullptr, nullptr, SOCK_NONBLOCK);
                    if (fd < 0)
                        break;
                    if (clients.size() >= 128) {
                        close(fd);
                        continue;
                    }
                    int yes = 1;
                    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
                    clients.emplace(fd, Connection{});
                }
            }
            if (p[1].revents & POLLIN)
                receive();
            std::vector<int> dead;
            for (std::size_t k = 2; k < p.size(); ++k) {
                int fd = p[k].fd;
                auto &cl = clients.at(fd);
                bool closed = (p[k].revents & (POLLERR | POLLNVAL)) != 0;
                if (p[k].revents & POLLIN) {
                    char buf[4096];
                    auto n = recv(fd, buf, sizeof(buf), 0);
                    if (n > 0)
                        cl.input.append(buf, static_cast<std::size_t>(n));
                    else if (n == 0 || (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR))
                        closed = true;
                }
                for (int b = 0; b < 32 && !cl.busy; ++b) {
                    auto e = cl.input.find('\n');
                    if (e == std::string::npos)
                        break;
                    std::string line = cl.input.substr(0, e);
                    cl.input.erase(0, e + 1);
                    try {
                        command(fd, line, now_ns());
                    } catch (const std::invalid_argument &) {
                        cl.output += "ERR INPUT\n";
                    } catch (const std::overflow_error &) {
                        cl.output += "ERR OVERFLOW\n";
                    }
                }
                if (cl.input.size() > 4096 || cl.output.size() > 65536)
                    closed = true;
                if (!cl.output.empty() && !closed) {
                    auto n = send(fd, cl.output.data(), cl.output.size(), MSG_NOSIGNAL);
                    if (n > 0)
                        cl.output.erase(0, static_cast<std::size_t>(n));
                    else if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)
                        closed = true;
                }
                if (closed)
                    dead.push_back(fd);
            }
            for (int fd : dead) {
                close(fd);
                clients.erase(fd);
            }
            timers(now_ns());
        }
        event("stop", ",\"state\":" + state_json(counter.state()));
        log.flush();
    }
};
} // namespace
int main(int argc, char **argv) {
    try {
        std::signal(SIGTERM, stop);
        std::signal(SIGINT, stop);
        std::signal(SIGPIPE, SIG_IGN);
        Server s(config(argc, argv));
        s.run();
        return 0;
    } catch (const std::exception &e) {
        std::cerr << "quota_server: " << e.what() << '\n';
        return 1;
    }
}
