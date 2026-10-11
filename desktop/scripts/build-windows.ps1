$ErrorActionPreference = "Stop"
Push-Location (Join-Path $PSScriptRoot "..")
try {
  $env:GOOS = "windows"
  $env:GOARCH = "amd64"
  $env:CGO_ENABLED = "0"
  New-Item -ItemType Directory -Force -Path bin | Out-Null
  go build -trimpath -tags production -ldflags "-s -w -H windowsgui" -o bin/p2pfs.exe .
  if ($LASTEXITCODE -ne 0) { throw "Windows build failed" }
  Write-Host "Built bin/p2pfs.exe. Windows/WebView2 runtime acceptance is a separate check."
} finally { Pop-Location }
