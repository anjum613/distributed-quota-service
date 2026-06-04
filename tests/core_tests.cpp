#include "counter.hpp"
#include <functional>
#include <iostream>
using namespace quota;
void check(bool ok, const char *why) {
    if (!ok)
        throw std::runtime_error(why);
}
void rejects(const std::function<void()> &f) {
    bool threw = false;
    try {
        f();
    } catch (const std::exception &) {
        threw = true;
    }
    check(threw, "expected rejection");
}
int main() {
    try {
        Counter a(0, {34, 33, 33}), b(1, {34, 33, 33}), c(2, {34, 33, 33});
        for (int i = 0; i < 34; ++i)
            check(a.spend(), "initial spend");
        check(!a.spend(), "exhaustion");
        check(!a.transfer(1, 1), "no overdraft");
        auto old = b.state();
        check(b.transfer(0, 6), "grant");
        check(b.available() == 27, "sender relinquishes first");
        check(!a.spend(), "receiver cannot spend in flight");
        auto grant = b.state();
        a.merge(grant);
        check(a.available() == 6, "receive grant");
        for (int k = 0; k < 5; ++k) {
            a.merge(old);
            a.merge(grant);
        }
        check(a.available() == 6, "duplicate/stale grants");
        for (int i = 0; i < 6; ++i)
            check(a.spend(), "borrowed spend");
        check(!a.spend(), "no duplicate rights");
        b.merge(a.state());
        c.merge(a.state());
        a.merge(c.state());
        check(a.state() == b.state() && b.state() == c.state(), "convergence");
        Counter x(0, {4, 4, 4}), y(1, {4, 4, 4}), z(2, {4, 4, 4});
        check(x.transfer(1, 3) && y.spend() && z.transfer(0, 2), "concurrent writes");
        Counter m1 = x, m2 = x;
        m1.merge(y.state());
        m1.merge(z.state());
        m2.merge(z.state());
        m2.merge(y.state());
        check(m1.state() == m2.state(), "merge commutes");
        auto snap = m1.state();
        m1.merge(snap);
        check(m1.state() == snap, "merge idempotent");
        Counter yz = y;
        yz.merge(z.state());
        Counter grouped = x;
        grouped.merge(yz.state());
        check(grouped.state() == m1.state(), "merge associative");
        auto corrupt = x.state();
        corrupt[0]++;
        rejects([&] { x.merge(corrupt); });
        corrupt = x.state();
        corrupt[9]++;
        rejects([&] { x.merge(corrupt); });
        corrupt = x.state();
        corrupt[4] = 0;
        rejects([&] { x.merge(corrupt); });
        rejects([&] { x.transfer(0, 1); });
        rejects([&] { x.transfer(1, 0); });
        rejects([&] { x.transfer(3, 1); });
        U max = std::numeric_limits<U>::max();
        rejects([&] { Counter bad(0, {max, 1, 0}); });
        rejects([&] { plus(max, 1); });
        Counter big(0, {max, 0, 0});
        check(big.transfer(1, max), "wide arithmetic transfer");
        Counter receiver(1, {max, 0, 0});
        receiver.merge(big.state());
        check(receiver.available() == max, "wide balance");
        check(receiver.transfer(0, max), "return rights");
        big.merge(receiver.state());
        auto before = big.state();
        rejects([&] { big.transfer(1, 1); });
        check(before == big.state(), "overflow leaves state unchanged");
        for (auto bad : {"-1", "18446744073709551616", "12x", ""})
            rejects([&] { number(bad); });
        for (auto bad : {"3:0", "0:-1", "01:2", "1:02", "1:1000000000001", "1"})
            rejects([&] { request_id(bad); });
        check(request_text(request_id("2:100")) == "2:100", "stable request IDs");
        std::cout << "PASS: spending, relinquishment, duplicate/stale merge, convergence, merge "
                     "laws, invalid inputs, overflow\n";
        return 0;
    } catch (const std::exception &e) {
        std::cerr << "FAIL: " << e.what() << '\n';
        return 1;
    }
}
