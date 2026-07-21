# ============================================================
# tight 可靠 UDP 传输协议库（独立构建 / 父项目 add_subdirectory 两用）
#
# 单独构建：
#   cmake -B build -S tight
#   cmake --build build
# 被引用：
#   add_subdirectory(tight)
#   target_link_libraries(your_target PRIVATE tight)
#   #include "tight/tight.hpp"
# ============================================================

# 独立构建时的引导（作为子目录构建时跳过，继承父项目设置）
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    cmake_minimum_required(VERSION 3.16)
    project(tight VERSION 2.1.0 LANGUAGES CXX)
    set(CMAKE_CXX_STANDARD 17)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
    set(CMAKE_CXX_EXTENSIONS OFF)
    if(MSVC)
        add_compile_options(/W4 /utf-8)
    else()
        add_compile_options(-Wall -Wextra -Wpedantic -Wno-unused-parameter)
    endif()
    find_package(Threads REQUIRED)
endif()

if(WIN32)
    set(TIGHT_PLATFORM_LIBS ws2_32)
else()
    set(TIGHT_PLATFORM_LIBS)
endif()

# 库本体：公共接口头在 include/tight/，私有实现细节在 src/（不对外暴露）
add_library(tight STATIC
    src/transport.cpp
    src/reassembler.cpp
    src/fragmenter.cpp
    src/report.cpp
    src/command.cpp
    src/crypto.cpp
    src/packet_codec.cpp
    src/fec.cpp
    src/bandwidth.cpp
    src/address.cpp
    src/crc32.cpp
    src/wsa.cpp
)

target_include_directories(tight
    PUBLIC  ${CMAKE_CURRENT_SOURCE_DIR}/include   # 对外：#include "tight/tight.hpp"
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src       # 对内：实现细节，不导出
)

target_link_libraries(tight PUBLIC
    Threads::Threads
    ${TIGHT_PLATFORM_LIBS}
)

# 安装导出：供其他项目直接引用
install(TARGETS tight ARCHIVE DESTINATION lib)
install(DIRECTORY include/ DESTINATION include)
