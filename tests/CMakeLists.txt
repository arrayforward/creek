add_test(
    NAME creek_redis_discovery_e2e
    COMMAND
        "powershell.exe" -NoProfile -ExecutionPolicy Bypass -File
        "${CMAKE_CURRENT_SOURCE_DIR}/../tools/redis_e2e.ps1"
        -SidecarExe "$<TARGET_FILE:creek_sidecar>"
        -HelloServerExe "$<TARGET_FILE:creek_hello_server>"
        -HelloClientExe "$<TARGET_FILE:creek_hello_client>"
)
set_tests_properties(creek_redis_discovery_e2e PROPERTIES TIMEOUT 180 RUN_SERIAL TRUE)
