#include "creek/logger.hpp"

#include <gtest/gtest.h>

#include <cstdio>
#include <string>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#endif

namespace creek::e2e {

struct TestResult {
    bool ok = false;
    std::string phase = "init";
    std::string detail;
};

TestResult run_e2e_generic_entry();

TEST(CreekE2EGeneric, UnaryBidiAndSticky) {
    auto result = run_e2e_generic_entry();
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
#ifdef _WIN32
    WSADATA wsa{};
    WSAStartup(MAKEWORD(2, 2), &wsa);
#endif
    setvbuf(stdout, nullptr, _IONBF, 0);
    setvbuf(stderr, nullptr, _IONBF, 0);
    creek::Logger::init("./e2e-logs");
    ::testing::InitGoogleTest(&argc, argv);
    int r = RUN_ALL_TESTS();
    creek::Logger::shutdown();
#ifdef _WIN32
    WSACleanup();
#endif
    return r;
}
