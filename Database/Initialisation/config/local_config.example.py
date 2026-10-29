"""
Machine-specific settings of the pipelines: database connection and paths to data outside this repository.

Copy this file to local_config.py (git-ignored) and fill in your values:

    cp local_config.example.py local_config.py

Environment variables override the connection string, the API key and the SEC User-Agent.
"""
import os
from pathlib import Path

# PostgreSQL connection (psycopg2 DSN)
DSN = os.environ.get("HOLDINGS_DSN",
                     "dbname=Holdings_v2 user=<user> password=<password> host=localhost port=5432")

# OpenFIGI API key (optional, raises the rate limits): https://www.openfigi.com/user/signup
OPENFIGI_API_KEY = os.environ.get("OPENFIGI_API_KEY")

# SEC EDGAR User-Agent, required by SEC for automated requests (amendments/): "Name email@example.com".
# Not an API key: https://www.sec.gov/os/webmaster-faq#developers
SEC_USER_AGENT = os.environ.get("SEC_USER_AGENT", "FILL_ME_IN Name email@example.com")

# owner_pipeline: unpacked EDGAR submissions.zip
SUBMISSIONS_DIR = Path("/path/to/submission/submissions")

# industry_pipeline: Kenneth French's industry definitions as CSV
SICCODES_DIR = Path("/path/to/Siccodes/Siccodes_csv/Siccodes")

# scrub_for_git.py: local path prefixes replaced in notebook outputs and logs before a commit (longest first)
SCRUB_PATHS = {
    "/home/<user>/path/to/Thesis": "<THESIS_DIR>",
    "/home/<user>": "<HOME>",
}
