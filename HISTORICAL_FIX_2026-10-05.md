# Historical Research Fix — 2026-10-05

- Uses current EWDS GloFAS historical schema: `year`, `month`, `day`, mandatory `timespan`, and `average_river_discharge_in_the_last_24_hours`.
- Uses operational GloFAS v4.0 historical data with LISFLOOD and the `intermediate` product for near-real-time daily availability.
- Historical requests are grouped by calendar month so requested dates are exact and never become an unintended Cartesian product.
- Small station-centered bounding boxes are used.
- Returned values are nearest-grid GloFAS modelled discharge, not gauge observations.
- Research verification is explicitly GloFAS operational forecast replay vs GloFAS historical modelled discharge.
- Operational forecast replay is guarded to the archive window beginning 2019-11-05.
