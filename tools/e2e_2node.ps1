param(
    [Parameter(Mandatory=$true)][string]$SidecarExe,
    [Parameter(Mandatory=$true)][string]$HelloServerExe,
    [Parameter(Mandatory=$true)][string]$HelloClientExe,
    [string]$RedisHost = "127.0.0.1",
    [int]$RedisPort = 6379,
    [string]$RedisPassword = "creekredis",
    [string]$RedisKey = "creek.nodes",
    [string]$LogDir = "$env:TEMP\creek-e2e-2node",
    [string]$Token = "test-e2e-token"
)

$ErrorActionPreference = "Stop"
$Script = $MyInvocation.MyCommand.Path
$Dir = Split-Path -Parent $Script
if (-not $Dir) { $Dir = (Get-Location).Path }
$StartTime = Get-Date

function Log-Phase {
    param([string]$Phase, [string]$Detail = "")
    $elapsed = ((Get-Date) - $StartTime).TotalSeconds
    $stamp = (Get-Date).ToString("HH:mm:ss.fff")
    $line = "[$stamp +{0:N2}s] === {1} === {2}" -f $elapsed, $Phase, $Detail
    Write-Host $line
    Add-Content -Path (Join-Path $LogDir "test.log") -Value $line
}

function Log-Step {
    param([string]$Tag, [string]$Detail = "")
    $elapsed = ((Get-Date) - $StartTime).TotalSeconds
    $stamp = (Get-Date).ToString("HH:mm:ss.fff")
    $line = "[$stamp +{0:N2}s] {1}: {2}" -f $elapsed, $Tag, $Detail
    Write-Host $line
    Add-Content -Path (Join-Path $LogDir "test.log") -Value $line
}

if (Test-Path $LogDir) {
    foreach ($f in (Get-ChildItem -Path $LogDir -Force -ErrorAction SilentlyContinue)) {
        try { Remove-Item -Force -Path $f.FullName -ErrorAction SilentlyContinue } catch {}
    }
} else {
    New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
}
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$BinDir = Split-Path -Parent $SidecarExe
$env:PATH = "$BinDir;D:\msys64\mingw64\bin;$env:PATH"
$RedisCli = "D:\vit\creek\tools\redis-cli.exe"
$redisArgs = @("-h", $RedisHost, "-p", "$RedisPort", "-a", $RedisPassword, "--no-auth-warning")

function Invoke-RedisCli {
    param([string[]]$Cmd)
    $out = & $RedisCli @redisArgs @Cmd 2>$null
    if ($null -eq $out) { return @() }
    return @($out | Where-Object { $_ -is [string] })
}

function Wait-RedisContains {
    param([string]$Field, [string]$ExpectedValue, [int]$TimeoutSeconds = 30)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $value = (Invoke-RedisCli @("HGET", $RedisKey, $Field)) -join ""
        if ($value -eq $ExpectedValue) { return $true }
        Start-Sleep -Milliseconds 300
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
    Log-Step "spawn" "$Tag $($cleanArgs -join ' ')"
    $p = Start-Process -FilePath $Exe -ArgumentList $cleanArgs -RedirectStandardOutput $StdoutLog -RedirectStandardError $StderrLog -WindowStyle Hidden -PassThru
    Log-Step "pid" "$Tag=$($p.Id)"
    return $p
}

function Stop-Child {
    param([System.Diagnostics.Process]$Proc)
    if ($null -ne $Proc -and -not $Proc.HasExited) {
        try { Stop-Process -Id $Proc.Id -Force } catch {}
    }
}

function Stop-All {
    Get-Process creek_sidecar, creek_hello_server -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 1
}

function Invoke-Hello {
    param([int]$Sid, [bool]$Sticky = $true, [int]$TimeoutMs = 1500)
    $tmpOut = Join-Path $LogDir "client.tmp"
    $tmpErr = Join-Path $LogDir "client.err"
    if (Test-Path $tmpOut) { Remove-Item -Force $tmpOut }
    if (Test-Path $tmpErr) { Remove-Item -Force $tmpErr }
    $proc = Start-Process -FilePath $HelloClientExe -ArgumentList @(
        "--target", "127.0.0.1:$($ports.EntryGrpc)",
        "--name", "e2e",
        "--sid", "$Sid",
        "--sticky", ($(if ($Sticky) { "true" } else { "false" })),
        "--count", "1",
        "--timeout-ms", "$TimeoutMs"
    ) -RedirectStandardOutput $tmpOut -RedirectStandardError $tmpErr -WindowStyle Hidden -Wait -PassThru
    $output = ""
    if (Test-Path $tmpOut) { $output = Get-Content $tmpOut -Raw }
    $errOutput = ""
    if (Test-Path $tmpErr) { $errOutput = Get-Content $tmpErr -Raw }
    if ($proc.ExitCode -ne 0 -or [string]::IsNullOrWhiteSpace($output)) {
        return [pscustomobject]@{ Ok = $false; Backend = $null; Raw = $output; Err = $errOutput; Code = $proc.ExitCode }
    }
    $line = ($output -split "`r?`n")[0]
    if ($line -match '^(backend-\d+)\s+(.+)$') {
        return [pscustomobject]@{ Ok = $true; Backend = $Matches[1]; Message = $Matches[2]; Raw = $line; Err = $errOutput; Code = 0 }
    }
    return [pscustomobject]@{ Ok = $false; Backend = $null; Raw = $line; Err = $errOutput; Code = $proc.ExitCode }
}

function Tail-SidecarLog {
    param([string]$Tag, [int]$Lines = 8)
    $err = Join-Path $LogDir "$Tag.err"
    if (Test-Path $err) {
        Get-Content $err -Tail $Lines -ErrorAction SilentlyContinue | ForEach-Object {
            if ($_ -match '\[creek-') {
                Log-Step "  $Tag" $_
            }
        }
    }
}

Stop-All

$basePort = 30000
$step = 4
$ports = @{
    Node1Udp = $basePort
    Node1Metrics = $basePort + 1
    Node2Udp = $basePort + $step
    Node2Metrics = $basePort + $step + 1
    ClientLeafUdp = $basePort + 2 * $step
    ClientLeafGrpc = $basePort + 2 * $step + 1
    ClientLeafMetrics = $basePort + 3 * $step
    Service1LeafUdp = $basePort + 3 * $step + 1
    Service1LeafGrpc = $basePort + 4 * $step
    Service1LeafMetrics = $basePort + 4 * $step + 1
    Service2LeafUdp = $basePort + 5 * $step
    Service2LeafGrpc = $basePort + 5 * $step + 1
    Service2LeafMetrics = $basePort + 6 * $step
    Backend1 = $basePort + 6 * $step + 1
    Backend2 = $basePort + 7 * $step
}

Log-Phase "PORTS" ($ports | ConvertTo-Json -Compress)
Log-Phase "REDIS" "clearing $RedisKey and $RedisKey`:leaves"
$null = Invoke-RedisCli @("DEL", $RedisKey)
$null = Invoke-RedisCli @("DEL", "$($RedisKey):leaves")

$procs = @()

$exitCode = 1
$exitMessage = ""
$failed = $false
$failures = New-Object System.Collections.Generic.List[string]

try {
    Log-Phase "PHASE 1" "boot node1 + node2 (the full mesh)"
    $procs += Start-Child -Tag "node1" -Exe $SidecarExe -Args @(
        "node", "--id", "node-1",
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
    ) -StdoutLog (Join-Path $LogDir "node1.log") -StderrLog (Join-Path $LogDir "node1.err")

    Start-Sleep -Milliseconds 800

    $procs += Start-Child -Tag "node2" -Exe $SidecarExe -Args @(
        "node", "--id", "node-2",
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
    ) -StdoutLog (Join-Path $LogDir "node2.log") -StderrLog (Join-Path $LogDir "node2.err")

    if (-not (Wait-RedisContains -Field "node-1" -ExpectedValue "127.0.0.1:$($ports.Node1Udp)" -TimeoutSeconds 15)) {
        throw "node-1 not registered in redis"
    }
    if (-not (Wait-RedisContains -Field "node-2" -ExpectedValue "127.0.0.1:$($ports.Node2Udp)" -TimeoutSeconds 15)) {
        throw "node-2 not registered in redis"
    }
    Log-Phase "PHASE 1 DONE" "node-1 / node-2 in redis"

    Log-Phase "PHASE 2" "boot client_leaf (client) + service1_leaf + service2_leaf"
    $procs += Start-Child -Tag "client_leaf" -Exe $SidecarExe -Args @(
        "leaf", "--id", "client-leaf",
        "--udp", "127.0.0.1:$($ports.ClientLeafUdp)",
        "--parent", "node-1@127.0.0.1:$($ports.Node1Udp)",
        "--grpc", "127.0.0.1:$($ports.ClientLeafGrpc)",
        "--metrics", "127.0.0.1:$($ports.ClientLeafMetrics)",
        "--token", $Token,
        "--redis-host", $RedisHost,
        "--redis-port", "$RedisPort",
        "--redis-password", $RedisPassword,
        "--redis-key", $RedisKey,
        "--sync-ms", "100",
        "--heartbeat-ms", "50",
        "--dead-timeout-ms", "2000",
        "--rpc-timeout-ms", "3000"
    ) -StdoutLog (Join-Path $LogDir "client_leaf.log") -StderrLog (Join-Path $LogDir "client_leaf.err")

    $procs += Start-Child -Tag "service1_leaf" -Exe $SidecarExe -Args @(
        "leaf", "--id", "service1-leaf",
        "--udp", "127.0.0.1:$($ports.Service1LeafUdp)",
        "--parent", "node-2@127.0.0.1:$($ports.Node2Udp)",
        "--grpc", "127.0.0.1:$($ports.Service1LeafGrpc)",
        "--metrics", "127.0.0.1:$($ports.Service1LeafMetrics)",
        "--token", $Token,
        "--redis-host", $RedisHost,
        "--redis-port", "$RedisPort",
        "--redis-password", $RedisPassword,
        "--redis-key", $RedisKey,
        "--sync-ms", "100",
        "--heartbeat-ms", "50",
        "--dead-timeout-ms", "2000",
        "--rpc-timeout-ms", "3000"
    ) -StdoutLog (Join-Path $LogDir "service1_leaf.log") -StderrLog (Join-Path $LogDir "service1_leaf.err")

    $procs += Start-Child -Tag "service2_leaf" -Exe $SidecarExe -Args @(
        "leaf", "--id", "service2-leaf",
        "--udp", "127.0.0.1:$($ports.Service2LeafUdp)",
        "--parent", "node-2@127.0.0.1:$($ports.Node2Udp)",
        "--grpc", "127.0.0.1:$($ports.Service2LeafGrpc)",
        "--metrics", "127.0.0.1:$($ports.Service2LeafMetrics)",
        "--token", $Token,
        "--redis-host", $RedisHost,
        "--redis-port", "$RedisPort",
        "--redis-password", $RedisPassword,
        "--redis-key", $RedisKey,
        "--sync-ms", "100",
        "--heartbeat-ms", "50",
        "--dead-timeout-ms", "2000",
        "--rpc-timeout-ms", "3000"
    ) -StdoutLog (Join-Path $LogDir "service2_leaf.log") -StderrLog (Join-Path $LogDir "service2_leaf.err")

    if (-not (Wait-Tcp -Port $ports.ClientLeafGrpc -TimeoutSeconds 15)) { throw "client_leaf grpc not up" }
    if (-not (Wait-Tcp -Port $ports.Service1LeafGrpc -TimeoutSeconds 15)) { throw "service1_leaf grpc not up" }
    if (-not (Wait-Tcp -Port $ports.Service2LeafGrpc -TimeoutSeconds 15)) { throw "service2_leaf grpc not up" }
    Log-Phase "PHASE 2 DONE" "all leaves gRPC servers are listening"

    Log-Phase "PHASE 3" "boot hello_server backends and register endpoints"
    $backend1 = Start-Child -Tag "backend1" -Exe $HelloServerExe -Args @(
        "--id", "backend-1",
        "--listen", "127.0.0.1:$($ports.Backend1)",
        "--leaf", "127.0.0.1:$($ports.Service1LeafGrpc)"
    ) -StdoutLog (Join-Path $LogDir "backend1.log") -StderrLog (Join-Path $LogDir "backend1.err")

    $backend2 = Start-Child -Tag "backend2" -Exe $HelloServerExe -Args @(
        "--id", "backend-2",
        "--listen", "127.0.0.1:$($ports.Backend2)",
        "--leaf", "127.0.0.1:$($ports.Service2LeafGrpc)"
    ) -StdoutLog (Join-Path $LogDir "backend2.log") -StderrLog (Join-Path $LogDir "backend2.err")

    if (-not (Wait-Tcp -Port $ports.Backend1 -TimeoutSeconds 15)) { throw "backend-1 hello_server not up" }
    if (-not (Wait-Tcp -Port $ports.Backend2 -TimeoutSeconds 15)) { throw "backend-2 hello_server not up" }
    Log-Phase "PHASE 3 DONE" "backend hello_servers are up"

    Log-Phase "PHASE 4" "wait for service discovery to converge"
    $discovered = $false
    for ($i = 0; $i -lt 30; $i++) {
        $r = Invoke-Hello -Sid 99 -Sticky $false
        if ($r.Ok) {
            $discovered = $true
            break
        }
        Log-Step "discovery" "attempt $($i+1): $($r.Raw) err=$($r.Err.Trim())"
        Start-Sleep -Seconds 1
    }
    if (-not $discovered) { throw "service discovery never converged" }
    Log-Phase "PHASE 4 DONE" "discovery converged"

    Log-Phase "PHASE 5" "sticky sid=1 -> 10 calls must hit same backend"
    $selected = $null
    for ($i = 1; $i -le 10; $i++) {
        $r = Invoke-Hello -Sid 1 -Sticky $true
        if (-not $r.Ok) {
            Tail-SidecarLog "client_leaf"
            Tail-SidecarLog "node1"
            Tail-SidecarLog "node2"
            Tail-SidecarLog "service1_leaf"
            Tail-SidecarLog "service2_leaf"
            throw "sticky call $i failed: $($r.Raw) err=$($r.Err.Trim())"
        }
        Log-Step "sticky" "[$i/10] -> $($r.Backend) :: $($r.Message)"
        if ($null -eq $selected) { $selected = $r.Backend }
        elseif ($r.Backend -ne $selected) {
            Tail-SidecarLog "client_leaf"
            throw "sticky flipped from $selected to $($r.Backend) on attempt $i"
        }
    }
    if ($selected -notin @("backend-1", "backend-2")) {
        throw "unknown selected backend $selected"
    }
    Log-Phase "PHASE 5 DONE" "sticky=$selected"

    $killTarget = if ($selected -eq "backend-1") { $backend1 } else { $backend2 }
    $expected = if ($selected -eq "backend-1") { "backend-2" } else { "backend-1"
    }
    Log-Phase "PHASE 6" "kill $selected (pid=$($killTarget.Id)) and watch failover to $expected"
    Stop-Child -Proc $killTarget
    Start-Sleep -Seconds 2

    $switched = $false
    $failDetail = ""
    for ($i = 1; $i -le 30; $i++) {
        $r = Invoke-Hello -Sid 1 -Sticky $true
        if ($r.Ok -and $r.Backend -eq $expected) {
            $switched = $true
            Log-Step "switched" "[$i/30] -> $($r.Backend) :: $($r.Message)"
            break
        }
        $summary = if ($r.Ok) { "got $($r.Backend)" } else { "failed err=$($r.Err.Trim())" }
        Log-Step "switch-wait" "[$i/30] $summary"
        if ($i -in 3, 6, 12, 20) {
            Tail-SidecarLog "client_leaf" 6
            Tail-SidecarLog "node1" 6
            Tail-SidecarLog "node2" 6
            if ($expected -eq "backend-1") {
                Tail-SidecarLog "service1_leaf" 6
            } else {
                Tail-SidecarLog "service2_leaf" 6
            }
        }
        Start-Sleep -Seconds 1
    }
    if (-not $switched) {
        Tail-SidecarLog "client_leaf" 30
        Tail-SidecarLog "node1" 30
        Tail-SidecarLog "node2" 30
        Tail-SidecarLog "service1_leaf" 30
        Tail-SidecarLog "service2_leaf" 30
        throw "failover did not reach $expected within 30s"
    }
    Log-Phase "PHASE 6 DONE" "switched to $expected"

    Log-Phase "ALL CHECKS PASSED"
    $exitCode = 0
}
catch {
    $exitMessage = $_.Exception.Message
    Log-Phase "FAILED" $exitMessage
    Log-Phase "dumping sidecar creek- stderr (last 40 lines each)"
    foreach ($tag in @("client_leaf","node1","node2","service1_leaf","service2_leaf","backend1","backend2")) {
        $err = Join-Path $LogDir "$tag.err"
        if (Test-Path $err) {
            Write-Host "---- $tag.err (last 40) ----"
            Get-Content $err -Tail 40 -ErrorAction SilentlyContinue
        }
    }
}
finally {
    foreach ($p in $procs) { Stop-Child -Proc $p }
    Stop-All
}

if ($exitCode -eq 0) {
    Write-Host "E2E_OK"
    exit 0
} else {
    Write-Host "E2E_FAIL: $exitMessage"
    exit 1
}
