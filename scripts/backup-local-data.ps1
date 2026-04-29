param(
    [string]$ConfigDir = "configs",
    [string]$BackupDir,
    [switch]$SkipRaw,
    [switch]$SkipSilver,
    [switch]$SkipGold,
    [switch]$SkipLogs,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$argsList = @(
    "backup-local-data",
    "--config-dir", $ConfigDir
)

if ($BackupDir) {
    $argsList += @("--backup-dir", $BackupDir)
}

if ($SkipRaw) { $argsList += "--skip-raw" } else { $argsList += "--include-raw" }
if ($SkipSilver) { $argsList += "--skip-silver" } else { $argsList += "--include-silver" }
if ($SkipGold) { $argsList += "--skip-gold" } else { $argsList += "--include-gold" }
if ($SkipLogs) { $argsList += "--skip-logs" } else { $argsList += "--include-logs" }
if ($DryRun) { $argsList += "--dry-run" } else { $argsList += "--write-archive" }

iee @argsList
