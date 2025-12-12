# SkyProfile

SkyProfile is a PyQt5 desktop app for querying ADSBexchange globe history for a single aircraft, detecting multi-leg flights, and exporting KML/CSV/JSON/Excel with rich analytics (airports, routes, time buckets, callsigns, FAA metadata).

## Key Features
- Date-range query by ICAO hex; optional tail/reg lookup via ADSBx basic aircraft DB.
- Robust leg detection (ground → air → ground) with air/ground flags, altitude/groundspeed heuristics, 90s ground dwell, 20-minute gap handling, and live callsign updates (history retained per leg).
- Exports (individually toggle): KML Points, KML Routes 2D, KML Routes 3D, CSV, JSON, Excel Summary.
- Airport matching (nearest within 250 km; synthetic IDs when none) reused across KML and Excel.
- FAA registry lookup for N-number aircraft (local DB in `resources/faa_registry`).
- GUI progress with per-phase status; Run/Stop controls; icon applied to window/taskbar.

## Data Sources
- **ADSBexchange globe_history**: `trace_full` JSON (primary flight path).
- **ADSBexchange basic-ac-db**: Registration/type/owner lookup and tail→hex resolution.
- **OpenSky**: Supplemental meta (registration, manufacturer, model, type).
- **Planespotters**: Registration hint (photos disabled in Excel to avoid drawing corruption).
- **OurAirports**: Airport DB for matching/analytics (cached locally).
- **FAA registry**: Local `ReleasableAircraft.zip` parsed from `resources/faa_registry`.

## Install / Run
### From source
```bash
pip install PyQt5 requests simplekml openpyxl lxml numpy
python skyprofile.py
```
### Packaged EXE (Windows)
- Use `dist/SkyProfile.exe` with `dist/resources/` alongside (contains FAA/airport/acdb data).

## Exports & Normalization
- **Leg detection**: ground/air flags, altitude/GS checks, 90s ground dwell, 20‑minute gap landing, ground-cluster averaging, callsign history per leg.
- **KML Points**: One placemark per leg arrival with times, duration, callsign, meta.
- **KML Routes 2D**: Full leg tracks (not straight lines) with metadata.
- **KML Routes 3D**: Altitude-enabled polylines (free-floating).
- **CSV**: One row per hit; columns: hex, segment, idx, timestamp_utc, lat, lon, alt_ft, gs, track, flattened `ac_data` keys (no raw `ac_data_json` column).
- **JSON**: Array of raw `trace_full` blobs exactly as downloaded (no extra wrapping).
- **Excel Summary** (sheets):
  - Summary (meta + FAA if available), Flights (per-leg with callsign history), Airports, Routes, DayOfWeek, DayOfMonth, Hours, Countries, Callsigns.
  - Charts: integer Y-axes with padding to max+1, values on bars, titles per chart (“Airports by Number of Visits,” “Count of Routes Traveled,” “Flights by Day of Week/Month,” “Takeoffs/Landings by Hour of Day,” “Country Visits,” “Count of Callsigns”). Hours legend: blue = Takeoffs, orange = Landings; X labels 0000/0100/…; route/airport labels cleaned.

## Resources & Caching
- Shared data lives in `resources/` (acdb_cache, airport_db, faa_registry). PyInstaller builds include it via `--add-data` and copy it alongside the EXE.
- ADSBx trace cache stays under `output/cache/<hex>/YYYY-MM-DD.json`.

## Notes & Limitations
- Coverage gaps may still miss exact ground times; nearest-airport matching can fall back to synthetic `LL_lat_lon` IDs.
- Photos are disabled in Excel to avoid corrupted drawings.
- Onefile EXE is large because it bundles Python + Qt + deps; use one-dir if you need a smaller EXE.

## License
- Apache License 2.0 (see `LICENSE`). External data/APIs remain under their own terms.

## Attribution & Copyright
- ADSBexchange globe history & basic-ac-db © ADSBexchange.
- OpenSky metadata © OpenSky Network contributors.
- Planespotters API © Planespotters.net & photographers.
- OurAirports data © contributors (CC0/Public Domain) – https://ourairports.com/.
- Icons/images retain their original licenses.
