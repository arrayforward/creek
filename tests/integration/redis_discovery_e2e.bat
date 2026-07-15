@echo off
setlocal EnableExtensions EnableDelayedExpansion

set SIDECAR=%~1
set HELLO_SERVER=%~2
set HELLO_CLIENT=%~3
set REDIS_HOST=%~4
set REDIS_PORT=%~5
set REDIS_PASS=%~6
set REDIS_KEY=%~7
set LOG_DIR=%~8

if "%SIDECAR%"=="" (
    echo [redis_e2e] missing sidecar path
    exit /b 2
)
if "%HELLO_SERVER%"=="" (
    echo [redis_e2e] missing hello_server path
    exit /b 2
)
if "%HELLO_CLIENT%"=="" (
    echo [redis_e2e] missing hello_client path
    exit /b 2
)
if "%REDIS_HOST%"=="" set REDIS_HOST=127.0.0.1
if "%REDIS_PORT%"=="" set REDIS_PORT=6379
if "%REDIS_PASS%"=="" set REDIS_PASS=creekredis
if "%REDIS_KEY%"=="" set REDIS_KEY=creek.nodes
if "%LOG_DIR%"=="" set LOG_DIR=%TEMP%\creek-redis-e2e

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
del /q "%LOG_DIR%\*" 2>nul

set REDIS_CLI=D:\vit\creek\tools\redis-cli.exe
set REDIS_OPTS=-h %REDIS_HOST% -p %REDIS_PORT% -a %REDIS_PASS% --no-auth-warning
set TOKEN=test-redis-token
set BIN_DIR=D:\vit\creek\build\msys2-mingw64\bin
set HELPER=D:\vit\creek\tools\start_proc.ps1
set PATH=%BIN_DIR%;%PATH%
set BASE_UDP=30000
set STEP=2
set /a N1_UDP=%BASE_UDP%
set /a N1_METRICS=%BASE_UDP%+1
set /a N2_UDP=%BASE_UDP%+%STEP%
set /a N2_METRICS=%BASE_UDP%+%STEP%+1
set /a ENTRY_UDP=%BASE_UDP%+%STEP%*2
set /a ENTRY_GRPC=%BASE_UDP%+%STEP%*2+1
set /a ENTRY_METRICS=%BASE_UDP%+%STEP%*3
set /a SVC1_UDP=%BASE_UDP%+%STEP%*3+1
set /a SVC1_GRPC=%BASE_UDP%+%STEP%*4
set /a SVC1_METRICS=%BASE_UDP%+%STEP%*4+1
set /a SVC2_UDP=%BASE_UDP%+%STEP%*5
set /a SVC2_GRPC=%BASE_UDP%+%STEP%*5+1
set /a SVC2_METRICS=%BASE_UDP%+%STEP%*6
set /a BACKEND1=%BASE_UDP%+%STEP%*6+1
set /a BACKEND2=%BASE_UDP%+%STEP%*7

call :sub_redis_flush
if errorlevel 1 exit /b 3

call :sub_start creek-n1 "%SIDECAR%" n1 --id n1 --udp 127.0.0.1:%N1_UDP% --metrics 127.0.0.1:%N1_METRICS% --token %TOKEN% --redis-host %REDIS_HOST% --redis-port %REDIS_PORT% --redis-password %REDIS_PASS% --redis-key %REDIS_KEY% --sync-ms 100 --heartbeat-ms 50 --dead-timeout-ms 2000
call :sub_sleep 1

call :sub_start creek-n2 "%SIDECAR%" n2 --id n2 --udp 127.0.0.1:%N2_UDP% --metrics 127.0.0.1:%N2_METRICS% --token %TOKEN% --redis-host %REDIS_HOST% --redis-port %REDIS_PORT% --redis-password %REDIS_PASS% --redis-key %REDIS_KEY% --sync-ms 100 --heartbeat-ms 50 --dead-timeout-ms 2000

echo [redis_e2e] waiting for redis to discover both nodes
set N1_FOUND=0
set N2_FOUND=0
for /l %%I in (1,1,30) do (
    for /f "usebackq tokens=*" %%A in (`%REDIS_CLI% %REDIS_OPTS% HGET %REDIS_KEY% n1`) do (
        if not "%%A"=="" set N1_FOUND=1
    )
    for /f "usebackq tokens=*" %%A in (`%REDIS_CLI% %REDIS_OPTS% HGET %REDIS_KEY% n2`) do (
        if not "%%A"=="" set N2_FOUND=1
    )
    if "!N1_FOUND!"=="1" if "!N2_FOUND!"=="1" goto :nodes_ready
    call :sub_sleep 1
)
echo [redis_e2e] nodes did not register in redis n1=!N1_FOUND! n2=!N2_FOUND!
call :sub_dump_logs
call :sub_cleanup
exit /b 4

:nodes_ready
echo [redis_e2e] node1 registered node2 registered

call :sub_start creek-entry "%SIDECAR%" entry --id entry-leaf --udp 127.0.0.1:%ENTRY_UDP% --parent n2@127.0.0.1:%N2_UDP% --grpc 127.0.0.1:%ENTRY_GRPC% --metrics 127.0.0.1:%ENTRY_METRICS% --token %TOKEN% --redis-host %REDIS_HOST% --redis-port %REDIS_PORT% --redis-password %REDIS_PASS% --redis-key %REDIS_KEY% --sync-ms 100 --heartbeat-ms 50 --dead-timeout-ms 2000 --rpc-timeout-ms 3000

call :sub_start creek-svc1 "%SIDECAR%" svc1 --id service-leaf-1 --udp 127.0.0.1:%SVC1_UDP% --parent n1@127.0.0.1:%N1_UDP% --grpc 127.0.0.1:%SVC1_GRPC% --metrics 127.0.0.1:%SVC1_METRICS% --token %TOKEN% --redis-host %REDIS_HOST% --redis-port %REDIS_PORT% --redis-password %REDIS_PASS% --redis-key %REDIS_KEY% --sync-ms 100 --heartbeat-ms 50 --dead-timeout-ms 2000 --rpc-timeout-ms 3000

call :sub_start creek-svc2 "%SIDECAR%" svc2 --id service-leaf-2 --udp 127.0.0.1:%SVC2_UDP% --parent n2@127.0.0.1:%N2_UDP% --grpc 127.0.0.1:%SVC2_GRPC% --metrics 127.0.0.1:%SVC2_METRICS% --token %TOKEN% --redis-host %REDIS_HOST% --redis-port %REDIS_PORT% --redis-password %REDIS_PASS% --redis-key %REDIS_KEY% --sync-ms 100 --heartbeat-ms 50 --dead-timeout-ms 2000 --rpc-timeout-ms 3000

call :sub_wait_tcp %ENTRY_GRPC% 30
if errorlevel 1 (
    echo [redis_e2e] entry grpc never came up
    call :sub_dump_logs
    call :sub_cleanup
    exit /b 5
)
call :sub_wait_tcp %SVC1_GRPC% 30
if errorlevel 1 (
    echo [redis_e2e] service1 grpc never came up
    call :sub_dump_logs
    call :sub_cleanup
    exit /b 6
)
call :sub_wait_tcp %SVC2_GRPC% 30
if errorlevel 1 (
    echo [redis_e2e] service2 grpc never came up
    call :sub_dump_logs
    call :sub_cleanup
    exit /b 7
)

call :sub_start creek-b1 "%HELLO_SERVER%" b1 --id backend-1 --listen 127.0.0.1:%BACKEND1% --leaf 127.0.0.1:%SVC1_GRPC%
call :sub_start creek-b2 "%HELLO_SERVER%" b2 --id backend-2 --listen 127.0.0.1:%BACKEND2% --leaf 127.0.0.1:%SVC2_GRPC%

call :sub_wait_tcp %BACKEND1% 30
call :sub_wait_tcp %BACKEND2% 30

call :sub_wait_for_discovery 30 || (
    echo [redis_e2e] service discovery via redis never converged
    call :sub_dump_logs
    call :sub_cleanup
    exit /b 8
)

call :sub_run_sticky 10
if errorlevel 1 (
    echo [redis_e2e] sticky routing assertion failed
    call :sub_dump_logs
    call :sub_cleanup
    exit /b 9
)

call :sub_kill_backend %SELECTED%
call :sub_wait_for_switch 30 || (
    echo [redis_e2e] backend switch did not occur
    call :sub_dump_logs
    call :sub_cleanup
    exit /b 10
)

echo [redis_e2e] all checks passed
call :sub_cleanup
exit /b 0

:sub_redis_flush
%REDIS_CLI% %REDIS_OPTS% DEL %REDIS_KEY% >nul 2>&1
%REDIS_CLI% %REDIS_OPTS% DEL %REDIS_KEY%:leaves >nul 2>&1
exit /b 0

:sub_sleep
powershell -NoProfile -Command "Start-Sleep -Seconds %~1" >nul 2>&1
exit /b 0

:sub_start
set "TITLE=%~1"
set "EXE=%~2"
set "LOGNAME=%~3"
set "ARGSTR="^""
shift /3
:collect_loop
if "%~1"=="" goto :build_ps
if defined ARGSTR set "ARGSTR=!ARGSTR!,"
set "ARGSTR=!ARGSTR!\"%~1\""
shift
goto :collect_loop
:build_ps
endlocal & set "PATH=%PATH%;%BIN_DIR%;D:\msys64\mingw64\bin"
set "CMDLINE=set PATH=!PATH!&&\"%EXE%\" !ARGSTR! 1>\"%LOG_DIR%\%LOGNAME%.log\" 2>\"%LOG_DIR%\%LOGNAME%.err\""
start "creek-%LOGNAME%" /B cmd /C "!CMDLINE!"
exit /b 0

:sub_wait_tcp
powershell -NoProfile -Command "$ok=$false; for($i=0;$i -lt 30;$i++){ try { $c=New-Object Net.Sockets.TcpClient; $c.BeginConnect('127.0.0.1',%~1,$null,$null)|Out-Null; Start-Sleep -Milliseconds 50; if($c.Connected){$ok=$true; break} } catch {}; $c.Close() }; if($ok){exit 0} else {exit 1}" >nul 2>&1
if not errorlevel 1 exit /b 0
set /a TRIES=%~2
if %TRIES% LEQ 0 exit /b 1
set /a TRIES=%TRIES%-1
call :sub_sleep 1
call :sub_wait_tcp %~1 %TRIES%
exit /b %errorlevel%

:sub_run_sticky
set TARGET=127.0.0.1:%ENTRY_GRPC%
set FIRST=
set SELECTED=
for /l %%I in (1,1,%~1) do (
    set LINE=
    for /f "usebackq tokens=*" %%L in (`"%HELLO_CLIENT%" --target %TARGET% --name redis --sid 1 --sticky true --count 1 --timeout-ms 1500`) do (
        set LINE=%%L
    )
    if "!LINE!"=="" (
        echo [redis_e2e] hello_client returned no output on attempt %%I
        exit /b 1
    )
    for /f "tokens=1,2 delims=	" %%A in ("!LINE!") do (
        set CUR=%%A
    )
    if "!FIRST!"=="" set FIRST=!CUR!
    if not "!SELECTED!"=="" if not "!CUR!"=="!SELECTED!" (
        echo [redis_e2e] sticky sid switched mid-loop from !SELECTED! to !CUR!
        exit /b 1
    )
    set SELECTED=!CUR!
)
if "!SELECTED!"=="" (
    echo [redis_e2e] no backend selected
    exit /b 1
)
if not "!SELECTED!"=="backend-1" if not "!SELECTED!"=="backend-2" (
    echo [redis_e2e] unknown backend id !SELECTED!
    exit /b 1
)
echo [redis_e2e] sticky=!SELECTED!
exit /b 0

:sub_kill_backend
if "%~1"=="backend-1" (
    taskkill /F /FI "WINDOWTITLE eq creek-b1*" >nul 2>&1
) else (
    taskkill /F /FI "WINDOWTITLE eq creek-b2*" >nul 2>&1
)
exit /b 0

:sub_wait_for_switch
for /l %%I in (1,1,%~1) do (
    set LINE=
    for /f "usebackq tokens=*" %%L in (`"%HELLO_CLIENT%" --target 127.0.0.1:%ENTRY_GRPC% --name redis --sid 1 --sticky true --count 1 --timeout-ms 1500`) do (
        set LINE=%%L
    )
    if not "!LINE!"=="" (
        for /f "tokens=1,2 delims=	" %%A in ("!LINE!") do (
            set CUR=%%A
        )
        if not "!CUR!"=="%SELECTED%" if "!CUR!"=="backend-1" goto :switched
        if not "!CUR!"=="%SELECTED%" if "!CUR!"=="backend-2" goto :switched
    )
    call :sub_sleep 1
)
exit /b 1
:switched
echo [redis_e2e] switched to !CUR!
exit /b 0

:sub_wait_for_discovery
for /l %%I in (1,1,%~1) do (
    set LINE=
    for /f "usebackq tokens=*" %%L in (`"%HELLO_CLIENT%" --target 127.0.0.1:%ENTRY_GRPC% --name redis --sid 99 --sticky false --count 1 --timeout-ms 1500`) do (
        set LINE=%%L
    )
    if not "!LINE!"=="" exit /b 0
    call :sub_sleep 1
)
exit /b 1

:sub_dump_logs
echo [redis_e2e] dumping logs from %LOG_DIR%
for %%F in ("%LOG_DIR%\*.log") do (
    echo ----- %%F -----
    type "%%F" 2>nul
)
for %%F in ("%LOG_DIR%\*.err") do (
    echo ----- %%F -----
    type "%%F" 2>nul
)
exit /b 0

:sub_cleanup
echo [redis_e2e] killing sidecars and backends
taskkill /F /IM creek_sidecar.exe >nul 2>&1
taskkill /F /IM creek_hello_server.exe >nul 2>&1
call :sub_sleep 1
exit /b 0
