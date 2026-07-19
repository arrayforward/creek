#pragma once

#include "creek/types.hpp"

#include <cstddef>
#include <vector>

namespace creek {

class ReedSolomon {
public:
    static Bytes parity(const std::vector<Bytes>& fragments, std::size_t width);
    static bool recover_one(std::vector<Bytes>& fragments, const Bytes& parity,
                            std::size_t missing_index, std::size_t width);
};

}
