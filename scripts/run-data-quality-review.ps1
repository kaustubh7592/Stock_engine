param(
    [string]$ConfigDir = "configs",
    [int]$MaxAgeDays = 0
)

$ErrorActionPreference = "Stop"

$argsList = @(
    "run-data-quality-review",
    "--config-dir", $ConfigDir
)

if ($MaxAgeDays -gt 0) {
    $argsList += @("--max-age-days", $MaxAgeDays)
}

iee @argsList
