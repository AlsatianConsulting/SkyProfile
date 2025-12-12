# SkyProfile Release Notes

## 2025-12-12
- Moved shared data to `resources/` (acdb_cache, airport_db, faa_registry) and bundle it with the EXE.
- FAA registry parsing hardened; uses local DB; N-number FAA data shown in Summary.
- Leg detection improved: gap-aware, ground dwell, live callsign history, handles high-alt gaps without false landings.
- Exports reworked:
  - CSV drops raw `ac_data_json`; keeps flattened `ac_data` fields.
  - JSON is raw ADSBexchange blobs (no wrapping).
  - KML routes use full tracks; 3D routes free-floating; 2D routes from actual traces.
  - KML/Excel airports/routes stay in sync; multi-leg flights split correctly.
- Excel summary overhaul:
  - Sheets: Summary, Flights, Airports, Routes, DayOfWeek, DayOfMonth, Hours, Countries, Callsigns.
  - Charts: integer Y-axes with padding, values on bars, cleaned titles/labels; Hours legend (Takeoffs blue, Landings orange), 0000/0100… hour labels.
  - Airports/Routes labels cleaned (no Series1/visit suffix); titles “Airports by Number of Visits,” “Count of Routes Traveled,” etc.
  - Axis labels set via helper; X labels below axis; padding max+1.
- UI: status/progress bar with percent, Run/Stop buttons, resource-based icon applied to window/taskbar.
- Photos sheet disabled to avoid Excel drawing corruption.
- Built onefile EXE with icon; resources copied alongside dist.

## Earlier
- Added multi-format exports (KML/CSV/JSON/Excel), airport/route analytics, OpenSky/Planespotters enrichment, and robust leg detection.
