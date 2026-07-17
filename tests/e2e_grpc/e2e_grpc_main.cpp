#include "e2e_grpc_test.hpp"
#include "creek/logger.hpp"

#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>
#include <exception>
#include <string>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#endif

namespace creek::e2e {

namespace {

std::string env_or(const char* key, const std::string& fallback) {
    const char* v = std::getenv(key);
    return v ? std::string(v) : fallback;
}

}

TEST(CreekE2EGrpc, DiscoveryStickyAndFailover) {
    std::string sidecar = env_or("CREEK_SIDECAR", "");
    std::string hello_server = env_or("CREEK_HELLO_SERVER", "");
    std::string log_dir = env_or("CREEK_E2E_LOG_DIR",
                                 "D:/vit/creek/tests/e2e-logs");
    std::string token = env_or("CREEK_E2E_TOKEN", "test-e2e-token");

    if (sidecar.empty() || hello_server.empty()) {
        GTEST_SKIP() << "CREEK_SIDECAR/CREEK_HELLO_SERVER not set";
    }

    TestEnv env(sidecar, hello_server, log_dir, token);
    auto result = run_e2e(env);
    if (!result.ok) {
        ADD_FAILURE() << "phase=" << result.phase << " detail=" << result.detail;
    }
    SUCCEED();
}

}

int main(int argc, char** argv) {
#ifdef _WIN32
    WSADATA wsa{};
    WSAStartup(MAKEWORD(2, 2), &wsa);
#endif
    setvbuf(stdout, nullptr, _IONBF, 0);
    setvbuf(stderr, nullptr, _IONBF, 0);
    std::fprintf(stdout, "[e2e_grpc] initializing logger\n");
    std::fflush(stdout);
    creek::Logger::init("D:/vit/creek/tests/e2e-logs");
    std::fprintf(stdout, "[e2e_grpc] starting gtest\n");
    std::fflush(stdout);
    ::testing::InitGoogleTest(&argc, argv);
    int r = RUN_ALL_TESTS();
    std::fprintf(stdout, "[e2e_grpc] RUN_ALL_TESTS returned %d\n", r);
    std::fflush(stdout);
    creek::Logger::shutdown();
#ifdef _WIN32
    WSACleanup();
#endif
    return r;
}