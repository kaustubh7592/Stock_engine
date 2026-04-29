param(
    [string]$ConfigDir = "configs",
    [switch]$SkipGdelt,
    [switch]$SkipOfficialPages,
    [switch]$RefreshSnapshots,
    [int]$GdeltMaxRecords = 50,
    [int]$EventSignalLimit = 10000,
    [int]$SnapshotLimit = 5000,
    [int]$ExplanationLimit = 100
)

$ErrorActionPreference = "Stop"

$argsList = @(
    "run-hourly-events",
    "--config-dir", $ConfigDir,
    "--gdelt-max-records", $GdeltMaxRecords,
    "--event-signal-limit", $EventSignalLimit,
    "--snapshot-limit", $SnapshotLimit,
    "--explanation-limit", $ExplanationLimit
)

if ($SkipGdelt) {
    $argsList += "--skip-gdelt"
} else {
    $argsList += "--include-gdelt"
}

if ($SkipOfficialPages) {
    $argsList += "--skip-official-pages"
} else {
    $argsList += "--include-official-pages"
}

if ($RefreshSnapshots) {
    $argsList += "--refresh-snapshots"
} else {
    $argsList += "--skip-snapshots"
}

iee @argsList
