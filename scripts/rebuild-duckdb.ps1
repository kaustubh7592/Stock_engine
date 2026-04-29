param(
    [string]$ConfigDir = "configs"
)

$ErrorActionPreference = "Stop"

iee rebuild-duckdb --config-dir $ConfigDir
