# FloodGuard BD — Dataset Contract

## Runtime file

`data/glofas_processed.csv`

## Required columns

| Column | Type | Meaning |
|---|---|---|
| date | YYYY-MM-DD | GloFAS historical date |
| station_id | string | FloodGuard zone ID |
| discharge_m3s | float | GloFAS average river discharge in m³/s |

## Station IDs

- sylhet — 24.895, 91.869
- kurigram — 25.805, 89.636
- sirajganj — 24.453, 89.700
- rajshahi — 24.374, 88.604
- faridpur — 23.607, 89.842
- feni — 23.015, 91.396

These are GloFAS target coordinates / nearest model river-grid cells, not Bangladesh gauge stations.

## Processing rules

1. Preserve the original raw GloFAS files outside the web runtime.
2. Extract the nearest valid GloFAS river-grid cell to each FloodGuard target coordinate.
3. Keep one row per station per calendar day.
4. Never interpolate or invent discharge values to fill missing observations without an explicit missing-data flag.
5. Keep source/version/product/model metadata in a separate manifest.
6. Remove exact duplicate station/date rows.
7. Do not mix FFWC observed water level into `discharge_m3s`; FFWC will be integrated later as a separate validation/observation table.
