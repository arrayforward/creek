add_test(
    NAME creek_redis_discovery_e2e
    COMMAND
        "D:/msys64/usr/bin/bash.exe" "${CMAKE_CURRENT_SOURCE_DIR}/../tools/redis_e2e.sh"
)
set_tests_properties(creek_redis_discovery_e2e PROPERTIES TIMEOUT 180 RUN_SERIAL TRUE
    ENVIRONMENT "PATH=${CMAKE_BINARY_DIR}/bin;D:/msys64/mingw64/bin;$ENV{PATH}")
