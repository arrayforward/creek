@echo off
setlocal EnableExtensions EnableDelayedExpansion
echo [redis_e2e] start
set REDIS_CLI=D:\vit\creek\tools\redis-cli.exe
set REDIS_OPTS=-h 127.0.0.1 -p 6379 -a creekredis --no-auth-warning
call :redis_flush
echo [redis_e2e] flush rc=%errorlevel%
exit /b 0

:redis_flush
%REDIS_CLI% %REDIS_OPTS% DEL creek.nodes >nul 2>&1
echo [redis_e2e] flush done
exit /b 0
