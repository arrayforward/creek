#include "creek/logger.hpp"

#include <gtest/gtest.h>

#include <cstdio>
#include <string>

namespace creek::e2e {

struct TestResult {
    bool ok = false;
    std::string phase = "init";
    std::string detail;
};

TestResult run_e2e_hardening_entry();

TEST(CreekE2EHardening, MountRetryServerStreamingAdminDirectoryDeadRemoval) {
    auto result = run_e2e_hardening_entry();
    if (!result.ok && result.phase == "env") {
        GTEST_SKIP() << result.detail;
    }
    if (!result.ok) {
        ADD_FAILURE() << "phase=" << result.phase << " detail=" << result.detail;
    }
    SUCCEED();
}

}  // namespace creek::e2e

int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IONBF, 0);
    setvbuf(stderr, nullptr, _IONBF, 0);
    creek::Logger::init("./e2e-logs");
    ::testing::InitGoogleTest(&argc, argv);
    int r = RUN_ALL_TESTS();
    creek::Logger::shutdown();
    return r;
}
