# SkyProfile

SkyProfile is a PyQt5 desktop app for querying ADSBexchange globe history for single or multiple aircraft, detecting multi-leg flights, and exporting KML/CSV/JSON/Excel with rich analytics (airports, routes, time buckets, callsigns, FAA metadata).

## Key Features
- Date-range query by ICAO hex; optional tail/reg lookup via ADSBx basic aircraft DB.
- Robust leg detection (ground → air → ground) with air/ground flags, altitude/groundspeed heuristics, 90s ground dwell, 20-minute gap handling, and live callsign history.
- Exports (toggle per type): KML Points, KML Routes 2D, KML Routes 3D, Airport Heatmap KML (white dots sized by activity), CSV, JSON, Excel Summary.
- Batch mode with optional **Consolidate reports**: merges all aircraft into a single set of offline exports (CSV/JSON/KML/Excel). Individual files are suppressed when consolidating.
- Airport matching (nearest within 250 km; synthetic IDs when none) reused across KML and Excel.
- FAA registry lookup for N-number aircraft (local DB in `resources/faa_registry`).
- GUI progress with per-phase status; Run/Stop controls; app icon applied to window/taskbar.
- Settings menu with "Check for Updates" (GitHub tag check) and "About" (version/info + data attributions).

## Batch Mode
- Input: CSV with column `icao_hex` (or `hex`/`icao`) and/or `tail`/`registration`. Each row is one aircraft target.
- Behavior:
  - Runs all listed aircraft over the chosen date range.
  - Writes per-aircraft outputs into `output/<HEX>_<TAIL>/` (hex-only if tail missing).
  - Optionally **Consolidate reports**: when enabled, suppresses individual files and produces a single merged set:
    - `consolidated_<N>ACFT_<start>_<end>.csv/.json` (all hits/JSON payloads)
    - Consolidated KMLs (points, routes, 3D routes, airport heatmap) merged inline for offline viewing
    - Consolidated Excel summary with the same tabs/charts as a normal run, combining all flights
- Caching: ADSBx downloads still cache per-aircraft/day under `output/cache/<hex>/YYYY-MM-DD.json`.

## Install / Run
### From source
```bash
pip install PyQt5 requests simplekml openpyxl lxml numpy
python skyprofile.py
```
### Packaged EXE (Windows)
- Use `dist/SkyProfile.exe` with `dist/resources/` alongside (contains FAA/airport/acdb data).

## Exports & Normalization
- **Leg detection**: ground/air flags, altitude/GS checks, 90s ground dwell, 20-minute gap landing, ground-cluster averaging, callsign history per leg.
- **KML Points**: One placemark per leg arrival with times, duration, callsign, meta.
- **KML Routes 2D/3D**: Full leg tracks (not straight lines); 3D routes include altitude.
- **Airport Heatmap KML**: One marker per visited airport, sized by total visits (arrivals+departures) and colored white for a clean overlay; attributes include arrival/departure dates, hex, tail.
- **CSV**: One row per hit; columns include hex and tail plus flattened `ac_data` (no raw `ac_data_json` column).
- **JSON**: Wrapped payload per aircraft: `{ hex, tail, meta, data: [raw trace_full blobs] }`; consolidated JSON is a list of these payloads.
- **Excel Summary** (full multi-sheet workbook):
  - `Summary`: Metadata (hex, registration, type, owner, model, flags, date range); FAA details when available.
  - `Flights`: One row per detected leg; columns for hex, tail, segment id, dep/arr UTC timestamps, duration, dep/arr airport IDs and names, lat/lon, day-of-week/day-of-month/hour buckets, callsign and history.
  - `Airports`: Top airports by visits (arrivals + departures) with IDs, ICAO/IATA, name, city/country, counts, lat/lon; includes bar chart “Airports by Number of Visits”.
  - `Routes`: Top routes with readable airport labels and counts; includes bar chart “Count of Routes Traveled”.
  - `DayOfWeek`: Counts by day-of-week with bar chart.
  - `DayOfMonth`: Counts by day-of-month with bar chart.
  - `Hours`: Takeoffs vs landings by UTC hour with a dual-series bar chart (Takeoffs blue, Landings orange).
  - `Countries`: Visits by country with bar chart.
  - `Callsigns`: Callsign usage counts.
  - Charts use white backgrounds, integer Y-axes with sensible grid (every 10 or 100 for large counts), and value labels on bars.

## Resources & Caching
- Shared data lives in `resources/` (acdb_cache, airport_db, faa_registry). PyInstaller builds include it via `--add-data` and copy it alongside the EXE.
- ADSBx trace cache stays under `output/cache/<hex>/YYYY-MM-DD.json`.
- Per-aircraft exports are grouped under `output/<HEX>_<TAIL>/` (or hex-only if no tail). Consolidated batch exports use `consolidated_<N>ACFT_<date range>*` in the main `output/` folder.

## Notes & Limitations
- Coverage gaps may still miss exact ground times; nearest-airport matching can fall back to synthetic `LL_lat_lon` IDs.
- Photos are disabled in Excel to avoid corrupted drawings.
- Onefile EXE is large because it bundles Python + Qt + dependencies; use one-dir if you need a smaller EXE.

## License
- Apache License 2.0 (see `LICENSE`). External data/APIs remain under their own terms.
