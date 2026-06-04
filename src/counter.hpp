#pragma once
// Fixed-quota specialisation of Balegas et al., arXiv:1503.09052, Fig. 2.
// Caller MUST serialise all operations at a replica. No persistence or recovery.
#include <algorithm>
#include <array>
#include <charconv>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>

namespace quota {
using U = std::uint64_t;
using Wide = __int128_t;
constexpr int N = 3;
using State = std::array<U, 12>; // R row-major (9), then U/spending (3).

inline U number(std::string_view text) {
    U n{};
    auto [p, ec] = std::from_chars(text.data(), text.data() + text.size(), n);
    if (text.empty() || ec != std::errc{} || p != text.data() + text.size())
        throw std::invalid_argument("invalid unsigned integer");
    return n;
}
inline int replica(U i) {
    if (i >= N)
        throw std::invalid_argument("replica must be 0, 1 or 2");
    return static_cast<int>(i);
}
inline U plus(U a, U b) {
    if (b > std::numeric_limits<U>::max() - a)
        throw std::overflow_error("counter overflow");
    return a + b;
}
inline U request_id(const std::string &text) {
    auto colon = text.find(':');
    if (colon == std::string::npos)
        throw std::invalid_argument("expected home:sequence");
    U home = number(text.substr(0, colon)), seq = number(text.substr(colon + 1));
    replica(home);
    if (seq > 1000000000000ULL || text != std::to_string(home) + ":" + std::to_string(seq))
        throw std::invalid_argument("noncanonical or oversized request ID");
    return seq * 3 + home;
}
inline std::string request_text(U id) {
    return std::to_string(id % 3) + ":" + std::to_string(id / 3);
}

class Counter {
    int me_;
    std::array<U, 3> allocation_;
    U total_{};
    State s_{};
    Wide balance(const State &s, int i) const {
        Wide n = static_cast<Wide>(s[i * 3 + i]) - s[9 + i];
        for (int j = 0; j < N; ++j)
            if (i != j)
                n += static_cast<Wide>(s[j * 3 + i]) - s[i * 3 + j];
        return n;
    }
    void validate(const State &s) const {
        Wide spent = 0;
        for (int i = 0; i < N; ++i) {
            if (s[i * 3 + i] != allocation_[i] || balance(s, i) < 0 || balance(s, i) > total_)
                throw std::invalid_argument("invalid counter state");
            spent += s[9 + i];
        }
        if (spent > total_)
            throw std::invalid_argument("quota exceeded in message");
    }

  public:
    Counter(int me, std::array<U, 3> allocation)
        : me_(replica(static_cast<U>(me))), allocation_(allocation) {
        for (int i = 0; i < N; ++i) {
            total_ = plus(total_, allocation[i]);
            s_[i * 3 + i] = allocation[i];
        }
    }
    const State &state() const {
        return s_;
    }
    U total() const {
        return total_;
    }
    U available() const {
        return static_cast<U>(balance(s_, me_));
    }
    U sent_to(int to) const {
        return s_[me_ * 3 + replica(static_cast<U>(to))];
    }
    U received_from(int from) const {
        return s_[replica(static_cast<U>(from)) * 3 + me_];
    }
    bool spend() {
        if (!available())
            return false;
        s_[9 + me_] = plus(s_[9 + me_], 1);
        return true;
    }
    bool transfer(int to, U n) {
        replica(static_cast<U>(to));
        if (to == me_ || !n)
            throw std::invalid_argument("invalid transfer");
        if (n > available())
            return false;
        s_[me_ * 3 + to] = plus(s_[me_ * 3 + to], n); // relinquish BEFORE publishing state
        return true;
    }
    bool merge(const State &other) {
        validate(other);
        // A peer cannot legitimately know a newer version of our own row.
        for (int j = 0; j < N; ++j)
            if (other[me_ * 3 + j] > s_[me_ * 3 + j])
                throw std::invalid_argument("own row advanced remotely");
        if (other[9 + me_] > s_[9 + me_])
            throw std::invalid_argument("own spending advanced remotely");
        State next = s_;
        for (std::size_t k = 0; k < next.size(); ++k)
            next[k] = std::max(next[k], other[k]);
        validate(next);
        bool changed = next != s_;
        s_ = next;
        return changed;
    }
};
} // namespace quota
