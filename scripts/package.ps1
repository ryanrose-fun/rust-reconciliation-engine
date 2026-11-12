$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.IO.Compression.FileSystem
$root = Split-Path $PSScriptRoot -Parent
Push-Location $root
try {
    cargo build --release --locked --bin api --bin hello_cargo --target-dir target/pilot
    if ($LASTEXITCODE -ne 0) { throw 'Release build failed' }
    $meta = cargo metadata --no-deps --format-version 1 | ConvertFrom-Json
    $version = $meta.packages[0].version
    $folder = Join-Path $root ('dist/reconciliation-' + $version + '-windows-x64')
    New-Item -ItemType Directory -Path $folder -Force | Out-Null
    Copy-Item target/pilot/release/api.exe,target/pilot/release/hello_cargo.exe,LICENSE,README.md,THIRD_PARTY_NOTICES.txt -Destination $folder -Force
    foreach ($dir in @('docs','examples','data')) { Copy-Item -LiteralPath $dir -Destination $folder -Recurse -Force }
    $hashes = Get-ChildItem -LiteralPath $folder -Recurse -File | Where-Object Name -ne 'SHA256SUMS' | ForEach-Object {
        $relative = $_.FullName.Substring($folder.Length + 1)
        (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLower() + '  ' + $relative
    }
    [IO.File]::WriteAllLines((Join-Path $folder 'SHA256SUMS'), $hashes)
    $zip = $folder + '.zip'
    Compress-Archive -Path (Join-Path $folder '*') -DestinationPath $zip -Force
    [IO.File]::WriteAllText(($zip + '.sha256'), (Get-FileHash -LiteralPath $zip -Algorithm SHA256).Hash.ToLower())
    Write-Output $zip
    $sourceFolder = Join-Path $root ('dist/reconciliation-' + $version + '-source')
    New-Item -ItemType Directory -Path $sourceFolder -Force | Out-Null
    foreach ($file in @('Cargo.toml','Cargo.lock','README.md','LICENSE','THIRD_PARTY_NOTICES.txt','SECURITY.md','CONTRIBUTING.md','Dockerfile','.dockerignore','.gitignore')) {
        Copy-Item -LiteralPath $file -Destination $sourceFolder -Force
    }
    foreach ($dir in @('src','tests','data','examples','docs','scripts','deploy','.github')) {
        Copy-Item -LiteralPath $dir -Destination $sourceFolder -Recurse -Force
    }
    # Benchmark tooling is source-only; never package its build directory.
    $perfFolder = Join-Path $sourceFolder 'perf'
    New-Item -ItemType Directory -Path $perfFolder -Force | Out-Null
    foreach ($file in @('Cargo.toml','Cargo.lock','build.rs','README.md')) {
        Copy-Item -LiteralPath (Join-Path 'perf' $file) -Destination $perfFolder -Force
    }
    Copy-Item -LiteralPath 'perf/src' -Destination $perfFolder -Recurse -Force
    $sourceZip = $sourceFolder + '.zip'
    # .NET includes dotfiles, unlike Compress-Archive's hidden-file filtering.
    $zipStream = [IO.File]::Open($sourceZip, [IO.FileMode]::Create)
    $archive = New-Object IO.Compression.ZipArchive($zipStream, [IO.Compression.ZipArchiveMode]::Create)
    try {
        Get-ChildItem -LiteralPath $sourceFolder -Recurse -Force -File | ForEach-Object {
            $relative = $_.FullName.Substring($sourceFolder.Length + 1).Replace('\','/')
            [IO.Compression.ZipFileExtensions]::CreateEntryFromFile($archive,$_.FullName,$relative) | Out-Null
        }
    } finally { $archive.Dispose(); $zipStream.Dispose() }
    [IO.File]::WriteAllText(($sourceZip + '.sha256'), (Get-FileHash -LiteralPath $sourceZip -Algorithm SHA256).Hash.ToLower())
    Write-Output $sourceZip
} finally { Pop-Location }