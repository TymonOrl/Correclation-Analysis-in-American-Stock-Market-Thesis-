# Database initialisation

Builds the `Holdings_v2` PostgreSQL database (schemas `input_1` = raw / intermediate, `clean_up_1` = final tables)
from 13F holdings, the SEC 13(f) securities lists, OpenFIGI and SEC EDGAR data.

```
Initialisation/
├── notebooks/      the six pipeline notebooks, numbered in order of execution
├── src/            pipeline_helpers.py (execution and logging), load_holdings.py (CSV loader, run by notebook 1)
├── config/         local_config.py (git-ignored), local_config.example.py
├── tools/          scrub_for_git.py
├── data/           loader state (git-ignored): file_adresses.csv, tracking.csv, PAUSE
├── logs/           run logs, logs/<pipeline>/<RUN_ID>/
├── dbdiagrams/     database diagrams in DBML (paste into https://dbdiagram.io)
└── old/            superseded SQL scripts, loaders, playground, notebook backups, Status.md
```

## Setup

* `config/local_config.py` (git-ignored) holds the machine-specific settings: connection string, OpenFIGI API key
  and the paths to data outside the repository. Create it from the template and fill in your values:
  `cp config/local_config.example.py config/local_config.py`.
* `data/file_adresses.csv` (git-ignored) lists the holdings CSV files to load, one absolute path per line under the
  header `address`; `data/tracking.csv` is created from it by the loader.
* Before a commit, run `python tools/scrub_for_git.py`: it replaces the local path prefixes (`SCRUB_PATHS` in
  `config/local_config.py`) in notebook outputs and logs, and reports any local path left in code. `--check` only
  reports.

## Pipelines and order of execution

Run the notebooks from `notebooks/` (the parent of the working directory is taken as `Initialisation/`, for `src/`,
`config/` and `logs/`), top to bottom, in this order:

| # | Notebook | What it does | Needs | Builds |
|---|---|---|---|---|
| 1 | `1_holdings_pipeline.ipynb` | Loads the 13F holdings CSV files, fixes CUSIPs, types and filters the rows, handles amendments (restatements / new holdings) and superseded filings, corrects values and removes ownership outliers | holdings CSVs (`data/file_adresses.csv`), `final_amendment_types.csv` (`../amendments`) | `input_1.holdings_input_table`, `input_1.final_amendment_types`, `clean_up_1.holdings_clean`, `clean_up_1.filings` |
| 2 | `2_f13_securities_pipeline.ipynb` | Loads the SEC Official Lists of 13(f) Securities (1996–2020), repairs OCR'd check digits, builds one row per CUSIP (no option CUSIPs: 13F reports options under the underlying) | list CSVs (`../13f_securities`); `holdings_clean` for the coverage check | `input_1.f13_securities`, `clean_up_1.asset` |
| 3 | `3_openfigi_pipeline.ipynb` | Looks up every underlying CUSIP in the OpenFIGI API (resumable; answers are kept, so a rerun only attaches them) and adds the FIGI columns to the asset table | `clean_up_1.asset` | `input_1.openfigi_lookup`, `input_1.openfigi_record`, FIGI columns of `clean_up_1.asset` |
| 4 | `4_owner_pipeline.ipynb` | Scans the EDGAR submissions files: owners (13F filers, every holdings CIK) and issuers (filers with a SIC code or ticker); current and former names | `Thesis/submission/submissions` (EDGAR `submissions.zip`), `holdings_clean`, `filings` | `input_1.edgar_submissions`, `input_1.owner_holdings`, `clean_up_1.edgar_company`, `clean_up_1.edgar_company_name` |
| 5 | `5_industry_pipeline.ipynb` | Links each asset to its issuer's CIK by name (confirmed by ticker) and loads the Fama-French definitions of all schemes; the industry is chosen at query time via `asset.issuer_cik → edgar_company.sic → sic_ff` | `clean_up_1.asset` (with FIGI tickers), `edgar_company` tables, `Thesis/Siccodes/Siccodes_csv/Siccodes` | `clean_up_1.ff_industry`, `clean_up_1.sic_ff`, `input_1.asset_issuer_match`, `issuer_cik` / `industry_match` on `clean_up_1.asset` |
| 6 | `6_finalisation_pipeline.ipynb` | Copies the final tables into schema `graph_db` with only the columns the analysis needs and real foreign keys; drops holdings whose CUSIP is not in `asset` (0.04%), keeps only the latest report of an owner in a quarter (`rdate` → `ryear` + `rquarter`), sums the sub-positions of a filing into one row per (`cik`, `ryear`, `rquarter`, `cusip`) with a synthetic key `holding_id`, and recomputes `is_owner` | `clean_up_1` tables of steps 1–5 (not `edgar_company_name`: issuer matching only) | `graph_db.ff_industry`, `sic_ff`, `edgar_company`, `asset`, `holdings_clean` |

Rerunning: each notebook rebuilds only its own tables. After rebuilding `clean_up_1.asset` (2), rerun 3 (attaches
the stored FIGIs without new API calls) and 5. After rebuilding the holdings (1), rerun 4 and 5. Rerun 6 after any of
them, so `graph_db` matches `clean_up_1`.

## Common to all notebooks

* Every SQL step shows the database feedback and is appended to `logs/<pipeline>/<RUN_ID>/steps.jsonl`;
  the last section of each notebook compares the run with earlier runs (row counts, durations).
* Section 0.2 checks the connection read-only before anything is changed. Connection string: `DSN` in
  `local_config.py` (the environment variable `HOLDINGS_DSN` overrides it).
* `expected=` / "Previous test" notes give the row counts of the last run, so changes in the data stand out.

## Diagrams (`dbdiagrams/`)

* `final_dbdiagram.txt`: the final schema `graph_db` (built by `6_finalisation_pipeline.ipynb`)
* `final_clean_up_dbdiagram.txt`: the `clean_up_1` schema the pipelines build (FF tables `ff_industry` / `sic_ff`)
* `final_tables_dbdiagram.txt`: earlier snapshot of the database (FF48 columns on `asset`)
* `dbdiagram_go_to.txt`: the target design (no `filings`; Fama-French tables `ff_industry` / `sic_ff` to choose
  the classification scheme)
