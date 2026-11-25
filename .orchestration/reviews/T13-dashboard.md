VERDICT: CHANGES

1. `lab2/dashboard/build_dashboard.py:150` — The reissue-age panel uses `CAP_PARTIAL`, which says 2026 is “shaded,” but `reissue_age_bars()` has age bins on the x-axis and does not shade or otherwise label the 2026 dropdown view as partial. This makes the honesty caption false and leaves the selected 2026 view without an in-chart partial-year marker. Use an age-chart-specific caption that does not claim shading and mark the 2026 dropdown option or chart title as “Jan-Sep”/partial.

Validation: `scripts/test_t13.py` passed; the real-data build produced 6,283,831 bytes with all 28 panels built; 17 representative plotted values matched their CSV rows; plotly.js appeared once; no network-valued HTML attributes were present; and only the section 9 cell differs from the task's `HEAD` version of `main.ipynb`.
