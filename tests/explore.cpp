// Bounded exhaustive exploration of the actual C++ counter, NOT a general proof.
// 3 replicas, initial [1,1,0], depth <= 8. Unit spends and transfers;
// publish any replica snapshot to a persistent unordered message pool; deliver
// any stored snapshot to either other replica, any number of times. Keeping
// every old snapshot models arbitrary duplication, delay and reordering.
#include "counter.hpp"
#include <iostream>
#include <set>
#include <vector>
using namespace quota;
struct Message {
    int from;
    State state;
    auto operator<=>(const Message &) const = default;
};
struct World {
    std::array<Counter, 3> replicas{Counter(0, {1, 1, 0}), Counter(1, {1, 1, 0}),
                                    Counter(2, {1, 1, 0})};
    std::set<Message> messages;
};
std::vector<U> key(const World &w) {
    std::vector<U> k;
    for (auto &c : w.replicas)
        k.insert(k.end(), c.state().begin(), c.state().end());
    for (auto &m : w.messages) {
        k.push_back(static_cast<U>(m.from));
        k.insert(k.end(), m.state.begin(), m.state.end());
    }
    return k;
}
void verify(const World &w) {
    U spent = 0;
    for (int i = 0; i < 3; ++i)
        spent += w.replicas[i].state()[9 + i];
    if (spent > 2)
        throw std::runtime_error("quota violation");
    auto r = w.replicas;
    // Quiescent full dissemination must reach the exact component maxima.
    State expected{};
    for (auto &c : r)
        for (int k = 0; k < 12; ++k)
            expected[k] = std::max(expected[k], c.state()[k]);
    for (int round = 0; round < 2; ++round)
        for (int i = 0; i < 3; ++i)
            for (int j = 0; j < 3; ++j)
                if (i != j)
                    r[j].merge(r[i].state());
    for (auto &c : r)
        if (c.state() != expected)
            throw std::runtime_error("convergence violation");
}
int main() {
    try {
        std::vector<World> layer(1);
        std::set<std::vector<U>> seen;
        seen.insert(key(layer[0]));
        U transitions = 0;
        verify(layer[0]);
        for (int depth = 1; depth <= 8; ++depth) {
            std::vector<World> next;
            auto visit = [&](World w) {
                ++transitions;
                verify(w);
                if (seen.insert(key(w)).second)
                    next.push_back(std::move(w));
            };
            for (auto &w : layer)
                for (int i = 0; i < 3; ++i) {
                    auto spend = w;
                    if (spend.replicas[i].spend())
                        visit(std::move(spend));
                    for (int j = 0; j < 3; ++j)
                        if (i != j) {
                            auto move = w;
                            if (move.replicas[i].transfer(j, 1))
                                visit(std::move(move));
                        }
                    auto send = w;
                    send.messages.insert({i, w.replicas[i].state()});
                    visit(std::move(send));
                    for (auto &msg : w.messages)
                        if (msg.from != i) {
                            auto deliver = w;
                            deliver.replicas[i].merge(msg.state);
                            visit(std::move(deliver));
                        }
                }
            std::cout << "depth=" << depth << " new_states=" << next.size()
                      << " total_states=" << seen.size() << '\n';
            layer = std::move(next);
        }
        std::cout << "PASS: " << seen.size() << " states, " << transitions
                  << " transitions; bounds: n=3 quota=2 depth=8; quiescent convergence checked at "
                     "every state\n";
        return 0;
    } catch (const std::exception &e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}
