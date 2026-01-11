param(
  [string]$EnvFile = ".env"
)

if (!(Test-Path $EnvFile)) {
  throw "Missing $EnvFile. Create it from .env.example."
}

Get-Content $EnvFile | ForEach-Object {
  $line = $_.Trim()
  if ($line -eq "" -or $line.StartsWith("#")) { return }

  $kv = $line.Split("=", 2)
  if ($kv.Length -ne 2) { return }

  $name = $kv[0].Trim()
  $value = $kv[1].Trim().Trim('"')

  [System.Environment]::SetEnvironmentVariable($name, $value, "Process")
}

if ($env:VCPKG_ROOT) {
  $env:CMAKE_TOOLCHAIN_FILE = Join-Path $env:VCPKG_ROOT "scripts\buildsystems\vcpkg.cmake"
}

if ($env:VCPKG_TARGET_TRIPLET) {
  $env:VCPKG_TARGET_TRIPLET = $env:VCPKG_TARGET_TRIPLET
}

Write-Host "VCPKG_ROOT=$env:VCPKG_ROOT"
Write-Host "CMAKE_TOOLCHAIN_FILE=$env:CMAKE_TOOLCHAIN_FILE"
Write-Host "VCPKG_TARGET_TRIPLET=$env:VCPKG_TARGET_TRIPLET"
