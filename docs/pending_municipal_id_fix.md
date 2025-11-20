# Municipal Forecast ID Normalization (Paused Task)

_Last updated: 2025-11-20_

## Status
- `scripts/r/get_forecast_data_hybrid.R` pads and trims `municipio_id` values everywhere via `normalize_municipio_id()` (live since 2025-11-20).
- `update_municipal_forecasts_only.sh` (job 27127, shards 1–5) re-ran with the fix at 11:32 CET; cumulative file now holds 664,489 rows with correctly padded IDs.
- Coverage audit installed: shard 1 now runs `python3 scripts/python/audit_municipal_forecast_coverage.py` after each array completion. The audit exits non-zero if any non-excluded IDs are missing.
- Latest audit (2025-11-20): 8,129 reference municipios, 8,037 collected; shortfall limited to the known excluded sets below.

## Expected Gaps (excluded from coverage metrics)

### New municipios without AEMET forecasts (monitor if they appear)
- `11903` — San Martín del Tesorillo
- `14901` — Fuente Carreteros
- `14902` — La Guijarrosa
- `18077` — Fornes
- `21902` — La Zarza-Perrunal
- `41904` — El Palmar de Troya

### Communal / parzonería / ledanía territories
- `53000`–`53083`
- `54001`–`54005`

The audit script ignores the IDs above for coverage calculations but will print a warning if any of them begin to appear in the AEMET output so we can revisit downstream handling.

## Coverage Snapshot — 2025-11-20
- Reference municipalities: 8,129
- Output municipalities (after de-duplication): 8,037
- Ignored IDs: 92
- Unexpected extras: none

Full ignored list:

```
11903, 14901, 14902, 18077, 21902, 41904, 53000, 53001, 53002, 53003, 53004,
53005, 53006, 53007, 53008, 53009, 53010, 53011, 53012, 53013, 53014, 53015,
53016, 53017, 53018, 53019, 53020, 53021, 53022, 53023, 53024, 53025, 53026,
53027, 53028, 53029, 53031, 53032, 53033, 53034, 53035, 53036, 53037, 53038,
53039, 53040, 53041, 53042, 53043, 53044, 53045, 53046, 53047, 53048, 53049,
53050, 53051, 53052, 53053, 53054, 53055, 53056, 53057, 53058, 53059, 53060,
53061, 53062, 53063, 53064, 53065, 53066, 53067, 53068, 53069, 53070, 53071,
53072, 53073, 53074, 53075, 53076, 53077, 53078, 53080, 53081, 53083, 54001,
54002, 54003, 54004, 54005
```

## Notes
- Forecast cumulative file is plain CSV at `data/output/daily_municipal_forecast.csv.gz` despite the `.gz` suffix.
- Municipal coverage audit runs automatically for SLURM array task 1; rerun manually with `python3 scripts/python/audit_municipal_forecast_coverage.py` if needed.
- Manual rewrite script (2025-11-20) remains in shell history should another one-off normalization ever be required.
