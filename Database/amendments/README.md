# Amendment types of 13F-HR/A filings

Classifies every 13F-HR/A filing in `input_1.holdings_input_table` as **RESTATEMENT** (replaces the original report)
or **NEW HOLDINGS** (adds positions to it). The result, `data/final_amendment_types.csv`, is imported by
`Initialisation/notebooks/1_holdings_pipeline.ipynb` (section 3), so it has to be rebuilt when the holdings load
changes the set of 13F-HR/A filings.

```
amendments/
├── main.py               runs the steps below
├── amendment_helpers.py  functions, one section per step
├── config.py             paths, input table, amendment forms, data set URLs, request limits
└── data/                 git-ignored except final_amendment_types.csv
```

## Sources

* **SEC Form 13F data sets** (XML filings from mid-2013 on): `COVERPAGE.tsv` has `AMENDMENTTYPE` directly.
* **EDGAR filings** (earlier, text-format filings): the cover page is fetched (first 60 kB, whole file only if the
  markers are missing) and parsed by `parse_amendment_info`: `CONFORMED SUBMISSION TYPE` in the SEC header for
  amendment yes / no, the checked box `IS A RESTATEMENT` / `ADDS NEW HOLDINGS ENTRIES` for the type. Both boxes
  checked gives `AMBIGUOUS`; the earliest filings (~1999–2001) often state no type at all (empty).

## Order of execution

`python main.py` runs all steps in order; `python main.py parse merge` runs only the given ones. Every step is
resumable / skips finished work.

| # | Step | Function | What it does | Writes |
|---|---|---|---|---|
| 1 | `fetch` | `fetch_input_rows` | Distinct 13F-HR/A filings of the input table | `data/input_rows.csv` |
| 2 | `download` | `download_bulk` | Downloads the quarterly data sets (`BULK_13F_URLS` in `config.py`), extracts `SUBMISSION.tsv` and `COVERPAGE.tsv` | `data/bulk_zips/`, `data/bulk_extracted/` |
| 3 | `parse` | `parse_bulk` | One lookup row per accession number | `data/bulk_lookup.csv` |
| 4 | `scrape` | `scrape_legacy_filings` | Filings not in the data sets, from EDGAR (≤ 9 requests/s in total, SEC's limit is 10) | `data/scrape_progress.csv` |
| 5 | `merge` | `merge_results` | Joins 1 with 3 and 4; `source` = `bulk_dataset`, `scrape_xml`, `scrape_legacy_text`, `scrape_inconclusive` or `unresolved` (run 4 again) | `data/final_amendment_types.csv` |

## Setup

* Connection string and the **SEC User-Agent** (`"Name email@example.com"`, required by SEC for automated
  requests) come from `Initialisation/config/local_config.py` (git-ignored, see `Initialisation/README.md`);
  the environment variables `HOLDINGS_DSN` / `SEC_USER_AGENT` override them.
* Python packages: `psycopg2` (step 1 only); the rest uses the standard library.
