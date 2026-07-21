# Builds the C++ e2e tests:
#   creek_e2e_2node    - 2-node / 1-leaf / redis-based discovery
#   creek_e2e_grpc     - 5-component gRPC test (2 nodes + 3 leaves + 2 backends)
#   creek_e2e_jsonrpc  - 5-component JSON-RPC test (HTTP/JSON client)
#
# NOTE: ENVIRONMENT must be set as a SINGLE property per test. Repeating
# ENVIRONMENT in set_tests_properties keeps only the LAST value, which used to
# drop PATH/CREEK_SIDECAR/... and made the tests silently GTEST_SKIP. The
# property is a ';'-separated list, so ';' inside PATH is escaped as '\;'.
if(WIN32)
    string(REPLACE ";" "\\;" CREEK_E2E_ESCAPED_PATH "$ENV{PATH}")
    set(CREEK_E2E_ENV_PREFIX "PATH=${CMAKE_BINARY_DIR}/bin\;D:/msys64/mingw64/bin\;${CREEK_E2E_ESCAPED_PATH};")
else()
    set(CREEK_E2E_ENV_PREFIX "")
endif()
set(CREEK_E2E_LOG_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../e2e-logs")

add_executable(creek_e2e_2node
    e2e_2node_test.cpp
    e2e_2node_main.cpp
)
target_include_directories(creek_e2e_2node PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(creek_e2e_2node PRIVATE creek_core GTest::gtest_main)
if(WIN32)
    set_target_properties(creek_e2e_2node PROPERTIES WIN32_EXECUTABLE OFF)
endif()
add_test(
    NAME creek_e2e_2node
    COMMAND "$<TARGET_FILE:creek_e2e_2node>"
)
set_tests_properties(creek_e2e_2node PROPERTIES
    TIMEOUT 180
    RUN_SERIAL TRUE
    # Set working directory to the binary dir so DLLs (gRPC, absl, etc.) are
    # found next to the executable on Windows.
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    ENVIRONMENT "${CREEK_E2E_ENV_PREFIX}CREEK_SIDECAR=$<TARGET_FILE:creek_sidecar>;CREEK_HELLO_SERVER=$<TARGET_FILE:creek_hello_server>;CREEK_HELLO_CLIENT=$<TARGET_FILE:creek_hello_client>;CREEK_REDIS_HOST=127.0.0.1;CREEK_REDIS_PORT=6379;CREEK_REDIS_PASS=creekredis;CREEK_REDIS_KEY=creek.nodes;CREEK_E2E_LOG_DIR=${CREEK_E2E_LOG_DIR};CREEK_E2E_TOKEN=test-e2e-token"
)

# 5-component end-to-end gRPC test (2 nodes + 3 leaves + 2 backends).
# Implemented in pure C++ so it doesn't depend on Python, generates fewer
# cross-process artifacts, and can use direct gRPC stubs.
add_executable(creek_e2e_grpc
    e2e_grpc_test.cpp
    e2e_grpc_main.cpp
)
target_include_directories(creek_e2e_grpc PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(creek_e2e_grpc PRIVATE creek_core creek_proto GTest::gtest_main)
if(WIN32)
    set_target_properties(creek_e2e_grpc PROPERTIES WIN32_EXECUTABLE OFF)
endif()
add_test(
    NAME creek_e2e_grpc
    COMMAND "$<TARGET_FILE:creek_e2e_grpc>"
)
set_tests_properties(creek_e2e_grpc PROPERTIES
    TIMEOUT 300
    RUN_SERIAL TRUE
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    ENVIRONMENT "${CREEK_E2E_ENV_PREFIX}CREEK_SIDECAR=$<TARGET_FILE:creek_sidecar>;CREEK_HELLO_SERVER=$<TARGET_FILE:creek_hello_server>;CREEK_E2E_LOG_DIR=${CREEK_E2E_LOG_DIR};CREEK_E2E_TOKEN=test-e2e-token"
)

# 5-component end-to-end JSON-RPC test (HTTP/JSON client).
# Pure C++ so it doesn't depend on Python. Uses nlohmann::json which is
# already linked in via creek_core.
add_executable(creek_e2e_jsonrpc
    e2e_jsonrpc_test.cpp
    e2e_jsonrpc_main.cpp
)
target_include_directories(creek_e2e_jsonrpc PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(creek_e2e_jsonrpc PRIVATE creek_core nlohmann_json::nlohmann_json GTest::gtest_main)
if(WIN32)
    set_target_properties(creek_e2e_jsonrpc PROPERTIES WIN32_EXECUTABLE OFF)
endif()
add_test(
    NAME creek_e2e_jsonrpc
    COMMAND "$<TARGET_FILE:creek_e2e_jsonrpc>"
)
set_tests_properties(creek_e2e_jsonrpc PROPERTIES
    TIMEOUT 300
    RUN_SERIAL TRUE
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    ENVIRONMENT "${CREEK_E2E_ENV_PREFIX}CREEK_SIDECAR=$<TARGET_FILE:creek_sidecar>;CREEK_HELLO_SERVER=$<TARGET_FILE:creek_hello_server>;CREEK_E2E_LOG_DIR=${CREEK_E2E_LOG_DIR};CREEK_E2E_TOKEN=test-e2e-token"
)
