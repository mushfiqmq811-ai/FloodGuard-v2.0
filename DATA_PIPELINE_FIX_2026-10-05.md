# FloodGuard BD — Data Pipeline Fix

Fixed a critical failure-path bug in `real_data.py`.

## Root cause
When an EWDS/GloFAS request failed and no previous authentic cache existed, `fetch_all()` called `live_snapshot()`, which calls `fetch_all()` again. This created recursive failure handling instead of exposing the actual API error.

## Fix
The failure path now raises the actual EWDS/GloFAS error. FloodGuard continues to refuse synthetic hydrological fallback.

## Expected diagnostics
`/api/glofas/diagnostics` now reports:
- `connected: true` when a real snapshot is loaded
- `station_count: 6`
- `forecast_lengths` for the six zones
- `last_error: null` on success
- the actual EWDS error when fetching fails

## Validation
Python syntax compilation passed for `app.py` and `real_data.py`.
