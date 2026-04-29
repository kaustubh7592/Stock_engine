param(
    [string]$ConfigDir = "configs",
    [switch]$IncludeLiveIngest,
    [switch]$IncludeFilingProcessing,
    [string]$AsOfDate,
    [int]$SnapshotLimit = 5000,
    [int]$ExplanationLimit = 100
)

$ErrorActionPreference = "Stop"

$argsList = @(
    "run-daily",
    "--config-dir", $ConfigDir,
    "--snapshot-limit", $SnapshotLimit,
    "--explanation-limit", $ExplanationLimit
)

if ($IncludeLiveIngest) {
    $argsList += "--include-live-ingest"
} else {
    $argsList += "--skip-live-ingest"
}

if ($IncludeFilingProcessing) {
    $argsList += "--include-filing-processing"
} else {
    $argsList += "--skip-filing-processing"
}

if ($AsOfDate) {
    $argsList += @("--as-of-date", $AsOfDate)
}

iee @argsList
