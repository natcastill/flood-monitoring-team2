# Source-status log

Record every change detected in a source (new file compilation, API change, outage, renamed feed) and notify the professor.

| Date (UTC) | Source | What changed / was observed | Action |
|---|---|---|---|
| 2026-11-06 | noaa_storm_events | Listing has 77 details files; 2025 file compiled `c20260819`, 2026 (partial year) compiled `c20261026` | Watermark = compile suffix per year |
| 2026-11-06 | ncei_ghcnd | 5 stations answered HTTP 200, 365 daily PRCP records each for 2025 | - |
| 2026-11-06 | nws_observations | Reports every ~5 min (plus 15-min and hourly METAR); newest observation 30-65 min old at query time; no ETag/Last-Modified | Watermark on `timestamp` per station, 3 h overlap |
| 2026-11-06 | nws_alerts | 0 active TX alerts at 02:20 UTC, 1 (Flood Advisory, Nueces) at 03:26 UTC; `/alerts` history is a rolling window | Keep all messages; mark missing as inactive |
