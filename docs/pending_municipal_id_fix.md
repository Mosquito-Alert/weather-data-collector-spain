# Municipal Forecast ID Normalization (Paused Task)

_Last updated: 2025-11-19_

## Status
- Nationwide municipal forecast run produced 65,737 rows for `2025-11-19`, but `municipio_id` values are stored without left padding and some contain stray whitespace/newlines.
- Direct comparisons against `data/input/municipalities.csv.gz` therefore flag entire provinces (e.g., Barcelona, Badajoz, Burgos) as missing even though data exists.

## Outstanding Work
1. Patch the municipal forecast collector(s) so `municipio_id` values are `str_trim`\+`str_pad(width = 5, pad = "0")` prior to persistence.
2. Regenerate today’s municipal forecasts after deploying the fix to validate that all ~8k municipalities collect successfully.
3. Re-run the audit script to confirm the differential drops to the expected handful of communal territories (53xxx codes, North African islets, etc.).

## Notes
- Example bad value: `municipio_id = "8001"` (should be `08001`).
- CSV file to reprocess: `data/output/daily_municipal_forecast.csv.gz` (plain-text CSV despite `.gz` extension).
- Reference mapping: `data/input/municipalities.csv.gz`.
- Prior Python helper lives in shell history: `python - <<'PY' ...` extracting outstanding IDs.

Resume from **Step 1** once Barcelona pipeline issues are resolved.
