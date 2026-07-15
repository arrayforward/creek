param(
    [string]$Exe,
    [string[]]$Args,
    [string]$Stdout,
    [string]$Stderr,
    [string]$PidFile
)
$env:PATH = $env:PATH + ';' + (Split-Path -Parent $Exe)
$env:PATH = $env:PATH + ';D:\msys64\mingw64\bin'
$p = Start-Process -FilePath $Exe -ArgumentList $Args -RedirectStandardOutput $Stdout -RedirectStandardError $Stderr -WindowStyle Hidden -PassThru
Set-Content -Path $PidFile -Value $p.Id
exit 0
