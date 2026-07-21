#include "e2e_2node_test.hpp"
#include "creek/logger.hpp"

#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>
#include <exception>
#include <string>

#ifdef _WIN32
#include <windows.h>
#endif

namespace creek::e2e {

namespace {

std::string env_or(const char* key, const std::string& fallback) {
    const char* v = std::getenv(key);
    return v ? std::string(v) : fallback;
}

#ifdef _WIN32
const char* kDefaultLogDir = "D:/vit/creek/tests/e2e-logs";
#else
const char* kDefaultLogDir = "/tmp/creek-e2e-logs";
#endif

#ifdef _WIN32
LONG WINAPI seh_handler(EXCEPTION_POINTERS* info) {
    char buf[512];
    std::snprintf(buf, sizeof(buf), "FATAL: SEH code=0x%08lx addr=%p\n",
                  info ? (unsigned long)info->ExceptionRecord->ExceptionCode : 0,
                  info ? info->ExceptionRecord->ExceptionAddress : nullptr);
    fputs(buf, stdout);
    fflush(stdout);
    auto* p = std::fopen("D:\\vit\\creek\\tests\\e2e-fatal.log", "w");
    if (p) { fputs(buf, p); fclose(p); }
    return EXCEPTION_EXECUTE_HANDLER;
}
#endif

}

int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IONBF, 0);
    setvbuf(stderr, nullptr, _IONBF, 0);
#ifdef _WIN32
    AddVectoredExceptionHandler(1, seh_handler);
#endif

    std::string log_dir = env_or("CREEK_E2E_LOG_DIR", kDefaultLogDir);
    creek::Logger::init(log_dir.c_str());
    auto* p = std::fopen((log_dir + "/e2e-probe.log").c_str(), "w");
    if (p) { std::fprintf(p, "start\n"); std::fclose(p); }
    std::fprintf(stdout, "PRE-GTEST\n"); std::fflush(stdout);
    ::testing::InitGoogleTest(&argc, argv);
    int r = RUN_ALL_TESTS();
    std::fprintf(stdout, "DONE rc=%d\n", r); std::fflush(stdout);
    creek::Logger::shutdown();
    return r;
}

#ifdef _WIN32
int probe() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    setvbuf(stderr, nullptr, _IONBF, 0);
    fprintf(stdout, "probe start\n"); fflush(stdout);
    auto dir = "D:\\vit\\creek\\tests\\e2e-logs";
    CreateDirectoryA(dir, nullptr);
    auto* fp = fopen("D:\\vit\\creek\\tests\\e2e-logs\\probe.log", "w");
    fprintf(stdout, "fp=%p errno=%d\n", (void*)fp, errno); fflush(stdout);
    if (fp) { fputs("hello\n", fp); fclose(fp); }
    return 0;
}
#endif

TEST(CreekE2E2Node, StickyAndFailover) {
    std::string sidecar = env_or("CREEK_SIDECAR", "");
    std::string hello_server = env_or("CREEK_HELLO_SERVER", "");
    std::string hello_client = env_or("CREEK_HELLO_CLIENT", "");
    std::string redis_host = env_or("CREEK_REDIS_HOST", "127.0.0.1");
    int redis_port = std::atoi(env_or("CREEK_REDIS_PORT", "6379").c_str());
    std::string redis_pass = env_or("CREEK_REDIS_PASS", "creekredis");
    std::string redis_key = env_or("CREEK_REDIS_KEY", "creek.nodes");
    std::string log_dir = env_or("CREEK_E2E_LOG_DIR", kDefaultLogDir);
    std::string token = env_or("CREEK_E2E_TOKEN", "test-e2e-token");

    if (sidecar.empty() || hello_server.empty() || hello_client.empty()) {
        GTEST_SKIP() << "CREEK_SIDECAR/CREEK_HELLO_SERVER/CREEK_HELLO_CLIENT not set";
    }

    TestEnv env(sidecar, hello_server, hello_client, redis_host, redis_port, redis_pass,
                redis_key, token, log_dir);
    auto result = run_e2e(env);
    if (!result.ok) {
        ADD_FAILURE() << "phase=" << result.phase << " detail=" << result.detail;
    }
    SUCCEED();
}

}
