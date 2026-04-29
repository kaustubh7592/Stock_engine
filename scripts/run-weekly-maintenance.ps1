param(
    [string]$ConfigDir = "configs",
    [string]$BackupDir,
    [switch]$SkipBackup,
    [switch]$DryRunBackup
)

$ErrorActionPreference = "Stop"

$argsList = @(
    "run-weekly-maintenance",
    "--config-dir", $ConfigDir
)

if ($BackupDir) {
    $argsList += @("--backup-dir", $BackupDir)
}

if ($SkipBackup) {
    $argsList += "--skip-backup"
} else {
    $argsList += "--include-backup"
}

if ($DryRunBackup) {
    $argsList += "--dry-run-backup"
} else {
    $argsList += "--write-backup"
}

iee @argsList
