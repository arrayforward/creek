cmake_minimum_required(VERSION 3.24)

project(creek VERSION 0.1.0 LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")

find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(GTest CONFIG REQUIRED)
find_package(Threads REQUIRED)
find_package(Python3 COMPONENTS Interpreter)

find_path(CREEK_OPENSSL_INCLUDE_DIR
    NAMES openssl/evp.h
    PATHS
        "D:/msys64/mingw64/include"
        "C:/msys64/mingw64/include"
)
find_library(CREEK_OPENSSL_SSL_LIBRARY
    NAMES ssl
    PATHS
        "D:/msys64/mingw64/lib"
        "C:/msys64/mingw64/lib"
)
find_library(CREEK_OPENSSL_CRYPTO_LIBRARY
    NAMES crypto
    PATHS
        "D:/msys64/mingw64/lib"
        "C:/msys64/mingw64/lib"
)
if(NOT CREEK_OPENSSL_INCLUDE_DIR OR NOT CREEK_OPENSSL_SSL_LIBRARY OR NOT CREEK_OPENSSL_CRYPTO_LIBRARY)
    message(FATAL_ERROR "OpenSSL not found at D:/msys64/mingw64 or C:/msys64/mingw64")
endif()

find_path(CREEK_HIREDIS_INCLUDE_DIR
    NAMES hiredis/hiredis.h
    PATHS
        "${CMAKE_PREFIX_PATH}/include"
        "D:/msys64/mingw64/include"
        "C:/msys64/mingw64/include"
        "/usr/include"
        "/usr/local/include"
)
find_library(CREEK_HIREDIS_LIBRARY
    NAMES hiredis
    PATHS
        "${CMAKE_PREFIX_PATH}/lib"
        "D:/msys64/mingw64/lib"
        "C:/msys64/mingw64/lib"
        "/usr/lib"
        "/usr/local/lib"
)
if(NOT CREEK_HIREDIS_INCLUDE_DIR OR NOT CREEK_HIREDIS_LIBRARY)
    message(FATAL_ERROR "hiredis library not found; install hiredis development headers and library")
endif()

set(CREEK_PROTO "${CMAKE_CURRENT_SOURCE_DIR}/proto/creek.proto")
set(CREEK_GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
set(CREEK_PROTO_SOURCE "${CREEK_GENERATED_DIR}/creek.pb.cc")
set(CREEK_PROTO_HEADER "${CREEK_GENERATED_DIR}/creek.pb.h")
set(CREEK_GRPC_SOURCE "${CREEK_GENERATED_DIR}/creek.grpc.pb.cc")
set(CREEK_GRPC_HEADER "${CREEK_GENERATED_DIR}/creek.grpc.pb.h")

add_custom_command(
    OUTPUT
        "${CREEK_PROTO_SOURCE}"
        "${CREEK_PROTO_HEADER}"
        "${CREEK_GRPC_SOURCE}"
        "${CREEK_GRPC_HEADER}"
    COMMAND "${CMAKE_COMMAND}" -E make_directory "${CREEK_GENERATED_DIR}"
    COMMAND "$<TARGET_FILE:protobuf::protoc>"
        "--proto_path=${CMAKE_CURRENT_SOURCE_DIR}/proto"
        "--cpp_out=${CREEK_GENERATED_DIR}"
        "--grpc_out=${CREEK_GENERATED_DIR}"
        "--plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
        "${CREEK_PROTO}"
    DEPENDS
        "${CREEK_PROTO}"
        protobuf::protoc
        gRPC::grpc_cpp_plugin
    VERBATIM
)

add_library(creek_proto STATIC
    "${CREEK_PROTO_SOURCE}"
    "${CREEK_GRPC_SOURCE}"
)
target_include_directories(creek_proto PUBLIC "${CREEK_GENERATED_DIR}")
target_link_libraries(creek_proto PUBLIC protobuf::libprotobuf gRPC::grpc++ gRPC::grpc++_reflection)

file(GLOB CREEK_CORE_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")
add_library(creek_core STATIC ${CREEK_CORE_SOURCES})
target_include_directories(creek_core PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
target_link_libraries(creek_core
    PUBLIC
        creek_proto
        nlohmann_json::nlohmann_json
        Threads::Threads
)
target_include_directories(creek_core PUBLIC "${CREEK_HIREDIS_INCLUDE_DIR}" "${CREEK_OPENSSL_INCLUDE_DIR}")
target_link_libraries(creek_core PUBLIC "${CREEK_HIREDIS_LIBRARY}" "${CREEK_OPENSSL_SSL_LIBRARY}" "${CREEK_OPENSSL_CRYPTO_LIBRARY}")
if(WIN32)
    target_link_libraries(creek_core PRIVATE ws2_32)
endif()

file(GLOB CREEK_WASM3_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/third_party/wasm3/source/m3_*.c")
list(FILTER CREEK_WASM3_SOURCES EXCLUDE REGEX ".*m3_api_uvwasi\\.c$")
add_library(creek_wasm3 STATIC ${CREEK_WASM3_SOURCES})
target_include_directories(creek_wasm3 PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/third_party/wasm3/source")
target_compile_features(creek_wasm3 PRIVATE c_std_99)
target_link_libraries(creek_core PUBLIC creek_wasm3)

add_executable(creek_sidecar "${CMAKE_CURRENT_SOURCE_DIR}/apps/sidecar_main.cpp")
target_link_libraries(creek_sidecar PRIVATE creek_core)

add_executable(creek_hello_server "${CMAKE_CURRENT_SOURCE_DIR}/apps/hello_server.cpp")
target_link_libraries(creek_hello_server PRIVATE creek_core)

add_executable(creek_hello_client "${CMAKE_CURRENT_SOURCE_DIR}/apps/hello_client.cpp")
target_link_libraries(creek_hello_client PRIVATE creek_proto)

add_executable(creek_admin_client "${CMAKE_CURRENT_SOURCE_DIR}/apps/admin_client.cpp")
target_link_libraries(creek_admin_client PRIVATE creek_proto)

enable_testing()
file(GLOB CREEK_UNIT_TEST_SOURCES CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/tests/unit/*.cpp"
)
list(FILTER CREEK_UNIT_TEST_SOURCES EXCLUDE REGEX ".*/e2e_.*\\.cpp$")
list(LENGTH CREEK_UNIT_TEST_SOURCES CREEK_UNIT_TEST_COUNT)
if(NOT CREEK_UNIT_TEST_COUNT EQUAL 5)
    message(FATAL_ERROR "Expected exactly five unit test source files, found ${CREEK_UNIT_TEST_COUNT}")
endif()
foreach(CREEK_UNIT_TEST_SOURCE IN LISTS CREEK_UNIT_TEST_SOURCES)
    get_filename_component(CREEK_UNIT_TEST_NAME "${CREEK_UNIT_TEST_SOURCE}" NAME_WE)
    string(REGEX REPLACE "[^A-Za-z0-9_]+" "_" CREEK_UNIT_TEST_NAME "${CREEK_UNIT_TEST_NAME}")
    set(CREEK_UNIT_TEST_TARGET "creek_${CREEK_UNIT_TEST_NAME}")
    add_executable("${CREEK_UNIT_TEST_TARGET}" "${CREEK_UNIT_TEST_SOURCE}")
    target_link_libraries("${CREEK_UNIT_TEST_TARGET}" PRIVATE creek_core GTest::gtest)
    add_test(NAME "${CREEK_UNIT_TEST_TARGET}" COMMAND "${CREEK_UNIT_TEST_TARGET}")
endforeach()

add_subdirectory(tests)

# C++ e2e tests (2-node redis discovery + 5-component gRPC routing).
# All end-to-end tests are now C++; the Python wrappers were removed.
add_subdirectory(tests/e2e_grpc)
