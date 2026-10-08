# Gemini prompt — FloodGuard BD GloFAS offline dataset processing

You are helping prepare the data layer for a student software project called FloodGuard BD.

IMPORTANT CONTEXT
- The project will NOT call GloFAS/Copernicus APIs at runtime.
- The downloaded GloFAS historical files are authoritative source data for this project.
- The website must use a local processed dataset only.
- Do not invent, simulate, interpolate, smooth, or fabricate hydrological values.
- Do not mix FFWC observed water level into the GloFAS discharge field. FFWC will be processed later as a separate source.
- The FloodGuard project has six target zones: sylhet, kurigram, sirajganj, rajshahi, faridpur, feni.
- Their target coordinates are:
  sylhet 24.895,91.869
  kurigram 25.805,89.636
  sirajganj 24.453,89.700
  rajshahi 24.374,88.604
  faridpur 23.607,89.842
  feni 23.015,91.396
- These coordinates are GloFAS target locations / nearest model river-grid cells, NOT Bangladesh gauge stations.
- Current GloFAS historical dataset is v5.0, LISFLOOD, consolidated, daily average river discharge in m3/s.

YOUR TASK
I will provide one or more downloaded GloFAS historical NetCDF/GRIB files, possibly one file per year.
Process ALL supplied files and create a clean master CSV for FloodGuard.

OUTPUT 1 — REQUIRED
Create:
`data/glofas_processed.csv`

The CSV must contain exactly one row per station per available calendar day, with these required columns:
`date,station_id,discharge_m3s`

Optional metadata columns may be included after them:
`lat,lon,source_file,glofas_version,product_type,hydrological_model`

Rules:
1. Read every supplied source file. Do not process only the first file.
2. Detect the actual latitude, longitude, time/date, and discharge variable names rather than assuming one fixed NetCDF variable name.
3. For each of the six FloodGuard coordinates, select the nearest VALID GloFAS river-grid cell with finite discharge values. Do not simply choose the nearest geographic cell if it has no river discharge.
4. Preserve every available daily date from every source file.
5. Keep discharge in m3/s without arbitrary scaling.
6. Do not fill missing days with invented values. If a date is absent, leave it absent and report it.
7. Remove exact duplicates for the same station/date. If duplicate values disagree, DO NOT silently average them; report the conflict and identify the source files.
8. Keep source/version/product/model metadata so provenance can be audited.
9. Sort by station_id then date.
10. Validate that all six station IDs appear if source coverage permits.
11. Produce a data-quality report with row count, date range, rows per station, missing-date ranges, duplicate count, invalid/negative discharge count, selected grid coordinates for each station, and source files used.
12. Never call the resulting discharge an observed gauge measurement. It is GloFAS modelled discharge.

OUTPUT 2 — REQUIRED
Create:
`data/DATA_QUALITY_REPORT.md`

Include:
- total rows
- date min/max
- rows per station
- selected GloFAS grid lat/lon for each station
- missing dates or gaps
- duplicates/conflicts
- min/max/median discharge per station
- exact source filenames
- GloFAS version/product/model/variable identified from the files
- any parsing warnings

OUTPUT 3 — REQUIRED
Create:
`data/glofas_manifest.json`

Include machine-readable provenance and processing metadata.

IMPORTANT SIZE RULE
Do NOT commit the full Bangladesh-wide raw NetCDF/GRIB files to the website repository if they are large.
The raw files should remain archived separately. The processed six-zone daily CSV is the runtime dataset.

QUALITY CHECK BEFORE FINISHING
- Verify the CSV opens correctly.
- Verify date values are valid ISO dates.
- Verify discharge is numeric and non-negative.
- Verify no duplicate station/date keys remain.
- Verify all six station IDs are spelled exactly as specified.
- Verify the selected grid cell for each station is actually a valid discharge cell.
- Do not create synthetic values to make the row counts look complete.

Also provide the exact Python commands you used to process the files, so the processing is reproducible.
