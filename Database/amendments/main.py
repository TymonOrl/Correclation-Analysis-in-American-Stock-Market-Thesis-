"""
Amendment types of the 13F-HR/A filings in the holdings input table (RESTATEMENT / NEW HOLDINGS).

    python main.py                  # all steps, in order
    python main.py parse merge      # only the given steps, in the given order

Functions: amendment_helpers.py; paths, sources and limits: config.py.
"""

import sys

import amendment_helpers as ah

STEPS = {
    "fetch":    ah.fetch_input_rows,        # input table -> data/input_rows.csv
    "download": ah.download_bulk,           # SEC Form 13F data sets -> data/bulk_zips/, data/bulk_extracted/
    "parse":    ah.parse_bulk,              # data sets -> data/bulk_lookup.csv
    "scrape":   ah.scrape_legacy_filings,   # EDGAR, filings not in the data sets -> data/scrape_progress.csv
    "merge":    ah.merge_results,           # -> data/final_amendment_types.csv
}


def main(steps):
    unknown = [s for s in steps if s not in STEPS]
    if unknown:
        raise SystemExit(f"unknown step(s) {unknown}, choose from {list(STEPS)}")

    for step in steps:
        print(f"===== {step}")
        STEPS[step]()


if __name__ == "__main__":
    main(sys.argv[1:] or list(STEPS))
