param(
    [Parameter(Mandatory=$true)][string]$SidecarExe,
    [Parameter(Mandatory=$true)][string]$HelloServerExe,
    [Parameter(Mandatory=$true)][string]$HelloClientExe,
    [string]$RedisHost = "127.0.0.1",
    [int]$RedisPort = 6379,
    [string]$RedisPassword = "creekredis",
    [string]$RedisKey = "creek.nodes",
    [string]$LogDir = "$env:TEMP\creek-redis-e2e",
    [string]$Token = "test-redis-token"
)

$ErrorActionPreference = "Stop"
$BinDir = Split-Path -Parent $SidecarExe
$RedisCli = "D:\vit\creek\tools\redis-cli.exe"

if (Test-Path $LogDir) {
    foreach ($f in (Get-ChildItem -Path $LogDir -Force -ErrorAction SilentlyContinue)) {
        try { Remove-Item -Force -Path $f.FullName -ErrorAction SilentlyContinue } catch {}
    }
} else {
    New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
}

$env:PATH = "$BinDir;D:\msys64\mingw64\bin;$env:PATH"

$redisArgs = @("-h", $RedisHost, "-p", "$RedisPort", "-a", $RedisPassword, "--no-auth-warning")

function Invoke-RedisCli {
    param([string[]]$Cmd)
    & $RedisCli @redisArgs @Cmd 2>$null | Where-Object { $_ -is [string] }
}

function Wait-RedisContains {
    param([string]$Field, [string]$ExpectedValue, [int]$TimeoutSeconds = 30)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $value = (Invoke-RedisCli @("HGET", $RedisKey, $Field)) -join ""
        if ($value -eq $ExpectedValue) { return $true }
        Start-Sleep -Milliseconds 500
    }
    return $false
}

function Wait-Tcp {
    param([int]$Port, [int]$TimeoutSeconds = 30)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $client = New-Object Net.Sockets.TcpClient
            $iar = $client.BeginConnect("127.0.0.1", $Port, $null, $null)
            $ok = $iar.AsyncWaitHandle.WaitOne(200)
            if ($ok -and $client.Connected) { $client.Close(); return $true }
            $client.Close()
        } catch {}
        Start-Sleep -Milliseconds 200
    }
    return $false
}

function Start-Child {
    param(
        [string]$Tag,
        [string]$Exe,
        [Parameter(ValueFromRemainingArguments=$true)]$Args,
        [string]$StdoutLog,
        [string]$StderrLog
    )
    $cleanArgs = @()
    foreach ($a in $Args) {
        if ($null -ne $a -and "$a".Length -gt 0) {
            $cleanArgs += [string]$a
        }
    }
    Write-Host "[redis_e2e] $Tag argv: $($cleanArgs -join ' ')"
    $p = Start-Process -FilePath $Exe -ArgumentList $cleanArgs -RedirectStandardOutput $StdoutLog -RedirectStandardError $StderrLog -WindowStyle Hidden -PassThru
    return $p
}

function Stop-Child {
    param([System.Diagnostics.Process]$Proc)
    if ($null -ne $Proc -and -not $Proc.HasExited) {
        try { Stop-Process -Id $Proc.Id -Force } catch {}
    }
}

$basePort = 30000
$step = 4
$ports = @{
    Node1Udp = $basePort
    Node1Metrics = $basePort + 1
    Node2Udp = $basePort + $step
    Node2Metrics = $basePort + $step + 1
    EntryUdp = $basePort + 2 * $step
    EntryGrpc = $basePort + 2 * $step + 1
    EntryMetrics = $basePort + 3 * $step
    Svc1Udp = $basePort + 3 * $step + 1
    Svc1Grpc = $basePort + 4 * $step
    Svc1Metrics = $basePort + 4 * $step + 1
    Svc2Udp = $basePort + 5 * $step
    Svc2Grpc = $basePort + 5 * $step + 1
    Svc2Metrics = $basePort + 6 * $step
    Backend1 = $basePort + 6 * $step + 1
    Backend2 = $basePort + 7 * $step
}

Write-Host "[redis_e2e] cleaning redis state"
$null = Invoke-RedisCli @("DEL", $RedisKey)
$null = Invoke-RedisCli @("DEL", "$($RedisKey):leaves")

$procs = @()

Get-Process creek_sidecar, creek_hello_server -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 1

try {
    Write-Host "[redis_e2e] starting n1 on udp $($ports.Node1Udp)"
    $procs += Start-Child -Tag "n1" -Exe $SidecarExe -Args @(
        "node", "--id", "n1",
        "--udp", "127.0.0.1:$($ports.Node1Udp)",
        "--metrics", "127.0.0.1:$($ports.Node1Metrics)",
        "--token", $Token,
        "--redis-host", $RedisHost,
        "--redis-port", "$RedisPort",
        "--redis-password", $RedisPassword,
        "--redis-key", $RedisKey,
        "--sync-ms", "100",
        "--heartbeat-ms", "50",
        "--dead-timeout-ms", "2000"
    ) -StdoutLog (Join-Path $LogDir "n1.log") -StderrLog (Join-Path $LogDir "n1.err")

    Start-Sleep -Seconds 1

    Write-Host "[redis_e2e] starting n2 on udp $($ports.Node2Udp)"
    $procs += Start-Child -Tag "n2" -Exe $SidecarExe -Args @(
        "node", "--id", "n2",
        "--udp", "127.0.0.1:$($ports.Node2Udp)",
        "--metrics", "127.0.0.1:$($ports.Node2Metrics)",
        "--token", $Token,
        "--redis-host", $RedisHost,
        "--redis-port", "$RedisPort",
        "--redis-password", $RedisPassword,
        "--redis-key", $RedisKey,
        "--sync-ms", "100",
        "--heartbeat-ms", "50",
        "--dead-timeout-ms", "2000"
    ) -StdoutLog (Join-Path $LogDir "n2.log") -StderrLog (Join-Path $LogDir "n2.err")

    Write-Host "[redis_e2e] waiting for redis to see n1 and n2"
    if (-not (Wait-RedisContains -Field "n1" -ExpectedValue "127.0.0.1:$($ports.Node1Udp)" -TimeoutSeconds 20)) {
        throw "[redis_e2e] n1 not registered in redis"
    }
    if (-not (Wait-RedisContains -Field "n2" -ExpectedValue "127.0.0.1:$($ports.Node2Udp)" -TimeoutSeconds 20)) {
        throw "[redis_e2e] n2 not registered in redis"
    }
    Write-Host "[redis_e2e] n1 and n2 visible in redis"

    Write-Host "[redis_e2e] starting entry leaf"
    $procs += Start-Child -Tag "entry" -Exe $SidecarExe -Args @(
        "leaf", "--id", "entry-leaf",
        "--udp", "127.0.0.1:$($ports.EntryUdp)",
        "--parent", "n2@127.0.0.1:$($ports.Node2Udp)",
        "--grpc", "127.0.0.1:$($ports.EntryGrpc)",
        "--metrics", "127.0.0.1:$($ports.EntryMetrics)",
        "--token", $Token,
        "--redis-host", $RedisHost,
        "--redis-port", "$RedisPort",
        "--redis-password", $RedisPassword,
        "--redis-key", $RedisKey,
        "--sync-ms", "100",
        "--heartbeat-ms", "50",
        "--dead-timeout-ms", "2000",
        "--rpc-timeout-ms", "3000"
    ) -StdoutLog (Join-Path $LogDir "entry.log") -StderrLog (Join-Path $LogDir "entry.err")

    Write-Host "[redis_e2e] starting service-leaf-1"
    $procs += Start-Child -Tag "svc1" -Exe $SidecarExe -Args @(
        "leaf", "--id", "service-leaf-1",
        "--udp", "127.0.0.1:$($ports.Svc1Udp)",
        "--parent", "n1@127.0.0.1:$($ports.Node1Udp)",
        "--grpc", "127.0.0.1:$($ports.Svc1Grpc)",
        "--metrics", "127.0.0.1:$($ports.Svc1Metrics)",
        "--token", $Token,
        "--redis-host", $RedisHost,
        "--redis-port", "$RedisPort",
        "--redis-password", $RedisPassword,
        "--redis-key", $RedisKey,
        "--sync-ms", "100",
        "--heartbeat-ms", "50",
        "--dead-timeout-ms", "2000",
        "--rpc-timeout-ms", "3000"
    ) -StdoutLog (Join-Path $LogDir "svc1.log") -StderrLog (Join-Path $LogDir "svc1.err")

    Write-Host "[redis_e2e] starting service-leaf-2"
    $procs += Start-Child -Tag "svc2" -Exe $SidecarExe -Args @(
        "leaf", "--id", "service-leaf-2",
        "--udp", "127.0.0.1:$($ports.Svc2Udp)",
        "--parent", "n2@127.0.0.1:$($ports.Node2Udp)",
        "--grpc", "127.0.0.1:$($ports.Svc2Grpc)",
        "--metrics", "127.0.0.1:$($ports.Svc2Metrics)",
        "--token", $Token,
        "--redis-host", $RedisHost,
        "--redis-port", "$RedisPort",
        "--redis-password", $RedisPassword,
        "--redis-key", $RedisKey,
        "--sync-ms", "100",
        "--heartbeat-ms", "50",
        "--dead-timeout-ms", "2000",
        "--rpc-timeout-ms", "3000"
    ) -StdoutLog (Join-Path $LogDir "svc2.log") -StderrLog (Join-Path $LogDir "svc2.err")

    if (-not (Wait-Tcp -Port $ports.EntryGrpc -TimeoutSeconds 20)) { throw "[redis_e2e] entry grpc never came up" }
    if (-not (Wait-Tcp -Port $ports.Svc1Grpc -TimeoutSeconds 20)) { throw "[redis_e2e] svc1 grpc never came up" }
    if (-not (Wait-Tcp -Port $ports.Svc2Grpc -TimeoutSeconds 20)) { throw "[redis_e2e] svc2 grpc never came up" }
    Write-Host "[redis_e2e] leaves gRPC servers are up"

    Write-Host "[redis_e2e] starting backend-1"
    $b1 = Start-Child -Tag "b1" -Exe $HelloServerExe -Args @(
        "--id", "backend-1",
        "--listen", "127.0.0.1:$($ports.Backend1)",
        "--leaf", "127.0.0.1:$($ports.Svc1Grpc)"
    ) -StdoutLog (Join-Path $LogDir "b1.log") -StderrLog (Join-Path $LogDir "b1.err")

    Write-Host "[redis_e2e] starting backend-2"
    $b2 = Start-Child -Tag "b2" -Exe $HelloServerExe -Args @(
        "--id", "backend-2",
        "--listen", "127.0.0.1:$($ports.Backend2)",
        "--leaf", "127.0.0.1:$($ports.Svc2Grpc)"
    ) -StdoutLog (Join-Path $LogDir "b2.log") -StderrLog (Join-Path $LogDir "b2.err")

    if (-not (Wait-Tcp -Port $ports.Backend1 -TimeoutSeconds 20)) { throw "[redis_e2e] backend-1 never came up" }
    if (-not (Wait-Tcp -Port $ports.Backend2 -TimeoutSeconds 20)) { throw "[redis_e2e] backend-2 never came up" }

    function Invoke-Hello {
        param([int]$Sid, [bool]$Sticky = $true)
        $tmpOut = Join-Path $LogDir "client.tmp"
        $tmpErr = Join-Path $LogDir "client.err"
        if (Test-Path $tmpOut) { Remove-Item -Force $tmpOut }
        if (Test-Path $tmpErr) { Remove-Item -Force $tmpErr }
        $proc = Start-Process -FilePath $HelloClientExe -ArgumentList @(
            "--target", "127.0.0.1:$($ports.EntryGrpc)",
            "--name", "redis",
            "--sid", "$Sid",
            "--sticky", ($(if ($Sticky) { "true" } else { "false" })),
            "--count", "1",
            "--timeout-ms", "1500"
        ) -RedirectStandardOutput $tmpOut -RedirectStandardError $tmpErr -WindowStyle Hidden -Wait -PassThru
        if ($proc.ExitCode -ne 0) { return $null }
        $output = Get-Content $tmpOut -Raw
        if ([string]::IsNullOrWhiteSpace($output)) { return $null }
        $line = ($output -split "`r?`n")[0]
        if ($line -match '^(backend-\d+)\s') {
            return $Matches[1]
        }
        return $null
    }

    $discovered = $false
    for ($i = 0; $i -lt 30; $i++) {
        $id = Invoke-Hello -Sid 99 -Sticky $false
        if ($id) { $discovered = $true; break }
        Start-Sleep -Seconds 1
    }
    if (-not $discovered) { throw "[redis_e2e] service discovery via redis never converged" }
    Write-Host "[redis_e2e] service discovery converged"

    $selected = $null
    for ($i = 0; $i -lt 10; $i++) {
        $id = Invoke-Hello -Sid 1 -Sticky $true
        if (-not $id) { throw "[redis_e2e] sticky call returned no backend (attempt $($i+1))" }
        if ($null -eq $selected) { $selected = $id }
        elseif ($id -ne $selected) { throw "[redis_e2e] sticky sid flipped from $selected to $id mid-loop" }
    }
    if ($selected -notin @("backend-1", "backend-2")) { throw "[redis_e2e] unknown sticky backend $selected" }
    Write-Host "[redis_e2e] sticky=$selected"

    $target = $null
    if ($selected -eq "backend-1") { $target = $b1 } else { $target = $b2 }
    Write-Host "[redis_e2e] killing $selected to trigger failover"
    Stop-Child -Proc $target
    Start-Sleep -Seconds 1

    $switched = $false
    $expected = if ($selected -eq "backend-1") { "backend-2" } else { "backend-1" }
    for ($i = 0; $i -lt 30; $i++) {
        $id = Invoke-Hello -Sid 1 -Sticky $true
        if ($id -eq $expected) { $switched = $true; break }
        Start-Sleep -Seconds 1
    }
    if (-not $switched) { throw "[redis_e2e] failover did not reach $expected within 30s" }
    Write-Host "[redis_e2e] switched to $expected"

    Write-Host "[redis_e2e] all checks passed"
    exit 0
}
catch {
    Write-Host $_
    Write-Host "[redis_e2e] dumping logs from $LogDir"
    Get-ChildItem "$LogDir\*.log", "$LogDir\*.err" -ErrorAction SilentlyContinue | Sort-Object Name | ForEach-Object {
        Write-Host ("---- " + $_.Name + " ----")
        Get-Content $_.FullName -Raw
    }
    exit 1
}
finally {
    foreach ($p in $procs) {
        Stop-Child -Proc $p
    }
    Get-Process creek_hello_server -ErrorAction SilentlyContinue | Stop-Process -Force
    Get-Process creek_sidecar -ErrorAction SilentlyContinue | Stop-Process -Force
    Start-Sleep -Seconds 1
}
