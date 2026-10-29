"""Configuration of the 13F amendment-type pipeline: paths, input table, SEC sources and request limits."""

import os
import sys

# --- Machine-specific settings ------------------------------------------
# SEC_USER_AGENT (name + email that SEC requires to identify automated
# requesters, see https://www.sec.gov/os/webmaster-faq#developers) and the
# Postgres DSN live in the git-ignored Initialisation/config/local_config.py
# (template: local_config.example.py); environment variables override both.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))   # .../Database/amendments
sys.path.insert(0, os.path.join(os.path.dirname(BASE_DIR), "Initialisation", "config"))
from local_config import DSN, SEC_USER_AGENT  # noqa: E402

# --- Postgres source ----------------------------------------------------
INPUT_TABLE = "input_1.holdings_input_table"

# Only amendments carry a RESTATEMENT / NEW HOLDINGS ENTRIES distinction --
# an original 13F-HR (or 13F-NT) has no such field, so there's no reason to
# fetch/classify it. Restricting to these submission types up front cuts the
# working set from ~305k filings to ~24.7k.
AMENDMENT_FORMS = ("13F-HR/A",)

# --- Local paths ----------------------------------------------------------
DATA_DIR = os.path.join(BASE_DIR, "data")
BULK_ZIP_DIR = os.path.join(DATA_DIR, "bulk_zips")
BULK_EXTRACT_DIR = os.path.join(DATA_DIR, "bulk_extracted")
BULK_LOOKUP_CSV = os.path.join(DATA_DIR, "bulk_lookup.csv")
INPUT_ROWS_CSV = os.path.join(DATA_DIR, "input_rows.csv")
SCRAPE_PROGRESS_CSV = os.path.join(DATA_DIR, "scrape_progress.csv")
FINAL_OUTPUT_CSV = os.path.join(DATA_DIR, "final_amendment_types.csv")

for _d in (DATA_DIR, BULK_ZIP_DIR, BULK_EXTRACT_DIR):
    os.makedirs(_d, exist_ok=True)

# --- SEC Form 13F data sets (download_bulk) -------------------------------
# Only the two files we need for the amendment-type lookup; the much larger
# INFOTABLE.tsv (individual holdings) is skipped.
BULK_WANTED_MEMBERS = ("SUBMISSION.tsv", "COVERPAGE.tsv")
DOWNLOAD_MAX_RETRIES = 4
DOWNLOAD_RETRY_BACKOFF_S = 5

# Quarterly/period ZIP download URLs, scraped from
# https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets
# on 2026-09-29. Covers July 2013 through August 2026 (SEC's stated scope is
# "XML data submitted from MAY 2013 through current period" -- the 2013q2 file
# is the earliest and smallest, covering May-June 2013).
BULK_13F_URLS = [
    ("2026 June July August 13F", "https://www.sec.gov/files/datastandardsinnovation/data/form-13f-data-sets/01jun2026-31aug2026_form13f.zip"),
    ("2026 March April May 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/01mar2026-31may2026_form13f.zip"),
    ("2025 December 2026 January February 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/01dec2025-28feb2026_form13f.zip"),
    ("2025 September October November 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/01sep2025-30nov2025_form13f.zip"),
    ("2025 June July August 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/01jun2025-31aug2025_form13f.zip"),
    ("2025 March April May 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/01mar2025-31may2025_form13f.zip"),
    ("2024 December 2025 January February 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/01dec2024-28feb2025_form13f.zip"),
    ("2024 September October November 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/01sep2024-30nov2024_form13f.zip"),
    ("2024 June July August 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/01jun2024-31aug2024_form13f.zip"),
    ("2024 March April May 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/01mar2024-31may2024_form13f.zip"),
    ("2024 January February 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/01jan2024-29feb2024_form13f.zip"),
    ("2023 Q4 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2023q4_form13f.zip"),
    ("2023 Q3 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2023q3_form13f.zip"),
    ("2023 Q2 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2023q2_form13f.zip"),
    ("2023 Q1 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2023q1_form13f.zip"),
    ("2022 Q4 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2022q4_form13f.zip"),
    ("2022 Q3 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2022q3_form13f.zip"),
    ("2022 Q2 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2022q2_form13f.zip"),
    ("2022 Q1 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2022q1_form13f.zip"),
    ("2021 Q4 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2021q4_form13f.zip"),
    ("2021 Q3 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2021q3_form13f.zip"),
    ("2021 Q2 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2021q2_form13f.zip"),
    ("2021 Q1 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2021q1_form13f.zip"),
    ("2020 Q4 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2020q4_form13f.zip"),
    ("2020 Q3 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2020q3_form13f.zip"),
    ("2020 Q2 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2020q2_form13f.zip"),
    ("2020 Q1 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2020q1_form13f.zip"),
    ("2019 Q4 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2019q4_form13f.zip"),
    ("2019 Q3 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2019q3_form13f.zip"),
    ("2019 Q2 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2019q2_form13f.zip"),
    ("2019 Q1 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2019q1_form13f.zip"),
    ("2018 Q4 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2018q4_form13f.zip"),
    ("2018 Q3 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2018q3_form13f.zip"),
    ("2018 Q2 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2018q2_form13f.zip"),
    ("2018 Q1 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2018q1_form13f.zip"),
    ("2017 Q4 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2017q4_form13f.zip"),
    ("2017 Q3 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2017q3_form13f.zip"),
    ("2017 Q2 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2017q2_form13f.zip"),
    ("2017 Q1 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2017q1_form13f.zip"),
    ("2016 Q4 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2016q4_form13f.zip"),
    ("2016 Q3 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2016q3_form13f.zip"),
    ("2016 Q2 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2016q2_form13f.zip"),
    ("2016 Q1 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2016q1_form13f.zip"),
    ("2015 Q4 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2015q4_form13f.zip"),
    ("2015 Q3 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2015q3_form13f.zip"),
    ("2015 Q2 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2015q2_form13f.zip"),
    ("2015 Q1 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2015q1_form13f.zip"),
    ("2014 Q4 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2014q4_form13f.zip"),
    ("2014 Q3 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2014q3_form13f.zip"),
    ("2014 Q2 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2014q2_form13f.zip"),
    ("2014 Q1 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2014q1_form13f.zip"),
    ("2013 Q4 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2013q4_form13f.zip"),
    ("2013 Q3 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2013q3_form13f.zip"),
    ("2013 Q2 13F", "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/2013q2_form13f.zip"),
]

# --- EDGAR scraping of filings not in the data sets (scrape_legacy_filings) --
# Respectful of SEC's fair-access rules (https://www.sec.gov/os/webmaster-faq#developers).
EDGAR_BASE = "https://www.sec.gov/Archives/edgar/data/"
SCRAPE_RANGE_BYTES = 60_000  # cover page / coverPage XML is always in this leading chunk
SCRAPE_REQUESTS_PER_SEC = 9.0  # global cap across all threads; SEC's limit is 10/sec
SCRAPE_WORKER_THREADS = 12  # just enough to keep the rate limiter saturated despite per-request latency
SCRAPE_MAX_RETRIES = 5
