#include "creek/tight/fec.hpp"

#include <algorithm>

namespace creek {

Bytes ReedSolomon::parity(const std::vector<Bytes>& fragments, std::size_t width) {
    Bytes p(width, 0);
    for (const auto& f : fragments) {
        std::size_t n = std::min(f.size(), width);
        for (std::size_t i = 0; i < n; ++i) p[i] ^= f[i];
    }
    return p;
}

bool ReedSolomon::recover_one(std::vector<Bytes>& fragments, const Bytes& parity,
                              std::size_t missing_index, std::size_t width) {
    if (missing_index >= fragments.size()) return false;
    if (parity.size() < width) return false;
    Bytes rec(width, 0);
    for (std::size_t i = 0; i < fragments.size(); ++i) {
        if (i == missing_index) continue;
        const auto& f = fragments[i];
        std::size_t n = std::min(f.size(), width);
        for (std::size_t j = 0; j < n; ++j) rec[j] ^= f[j];
    }
    for (std::size_t j = 0; j < width; ++j) rec[j] ^= parity[j];
    fragments[missing_index] = std::move(rec);
    return true;
}

}
