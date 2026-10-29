"""
Functions of the 13F amendment-type pipeline, one section per step (run by main.py):

  1. fetch_input_rows       distinct 13F-HR/A filings of the input table          -> INPUT_ROWS_CSV
  2. download_bulk          SEC Form 13F data sets: SUBMISSION.tsv, COVERPAGE.tsv -> BULK_EXTRACT_DIR
  3. parse_bulk             one lookup row per accession number                   -> BULK_LOOKUP_CSV
  4. scrape_legacy_filings  EDGAR filings not in the data sets (pre-mid-2013)     -> SCRAPE_PROGRESS_CSV
  5. merge_results          1 joined with 3 and 4                                 -> FINAL_OUTPUT_CSV

Every step is idempotent / resumable: re-running skips finished work.
"""

import csv
import os
import re
import threading
import time
import urllib.error
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional

from config import (
    AMENDMENT_FORMS, BULK_13F_URLS, BULK_EXTRACT_DIR, BULK_LOOKUP_CSV, BULK_WANTED_MEMBERS, BULK_ZIP_DIR,
    DOWNLOAD_MAX_RETRIES, DOWNLOAD_RETRY_BACKOFF_S, DSN, EDGAR_BASE, FINAL_OUTPUT_CSV, INPUT_ROWS_CSV,
    INPUT_TABLE, SCRAPE_MAX_RETRIES, SCRAPE_PROGRESS_CSV, SCRAPE_RANGE_BYTES, SCRAPE_REQUESTS_PER_SEC,
    SCRAPE_WORKER_THREADS, SEC_USER_AGENT,
)


def require_user_agent() -> None:
    """SEC requires a real identifying User-Agent for every automated request."""
    if SEC_USER_AGENT.startswith("FILL_ME_IN"):
        raise SystemExit(
            "ERROR: set SEC_USER_AGENT in Initialisation/config/local_config.py (or the SEC_USER_AGENT env var)\n"
            '       e.g. "Jane Doe jane.doe@example.com" -- SEC requires this to identify requesters.'
        )


# =====================================================================
# 1. Input rows
# =====================================================================
# Restricted to amendment submission types (config.AMENDMENT_FORMS) -- only
# amendments have a RESTATEMENT / NEW HOLDINGS ENTRIES distinction at all, so
# there's no point fetching/classifying original 13F-HR filings. This cuts
# the working set from ~305k filings to ~24.7k.
#
# rdate is a bigint YYYYMMDD in this table, reformatted to YYYY-MM-DD in the
# output CSV for readability.

INPUT_ROWS_QUERY = f"""
SELECT DISTINCT
  cik,
  rdate,
  address,
  SPLIT_PART(SPLIT_PART(address, '/', 2), '.', 1) AS accession_number
FROM {INPUT_TABLE}
WHERE form = ANY(%(forms)s)
"""


def format_rdate(rdate) -> str:
    if rdate is None:
        return ""
    s = str(int(rdate))
    if len(s) == 8:
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    return s


def fetch_input_rows() -> None:
    """Run the distinct filing-list query against Postgres and dump it to INPUT_ROWS_CSV."""
    import psycopg2   # only this step needs the database

    conn = psycopg2.connect(DSN)
    try:
        with conn.cursor(name="input_rows_cursor") as cur:  # server-side cursor: streams, doesn't load 302k rows via client-side buffering
            cur.itersize = 5000
            cur.execute(INPUT_ROWS_QUERY, {"forms": list(AMENDMENT_FORMS)})
            with open(INPUT_ROWS_CSV, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["cik", "rdate", "address", "accession_number"])
                n = 0
                for cik, rdate, address, accession_number in cur:
                    writer.writerow([cik, format_rdate(rdate), address, accession_number])
                    n += 1
                    if n % 50000 == 0:
                        print(f"  ...{n} rows written")
        print(f"Wrote {n} rows to {INPUT_ROWS_CSV}")
    finally:
        conn.close()


# =====================================================================
# 2. Download the SEC Form 13F data sets
# =====================================================================
# Downloads the quarterly bulk data ZIPs and extracts only SUBMISSION.tsv and
# COVERPAGE.tsv. Re-running skips ZIPs already downloaded and periods already
# extracted.

def slug_for(url: str) -> str:
    return re.sub(r"\.zip$", "", os.path.basename(url))


def download_one(url: str, dest_path: str) -> None:
    req = urllib.request.Request(url, headers={"User-Agent": SEC_USER_AGENT})
    tmp_path = dest_path + ".part"
    for attempt in range(1, DOWNLOAD_MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp, open(tmp_path, "wb") as out:
                while True:
                    chunk = resp.read(1024 * 256)
                    if not chunk:
                        break
                    out.write(chunk)
            os.replace(tmp_path, dest_path)
            return
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            if attempt == DOWNLOAD_MAX_RETRIES:
                raise
            wait = DOWNLOAD_RETRY_BACKOFF_S * attempt
            print(f"    retry {attempt}/{DOWNLOAD_MAX_RETRIES} after error ({e}); waiting {wait}s")
            time.sleep(wait)


def extract_wanted(zip_path: str, out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)
    with zipfile.ZipFile(zip_path) as z:
        # Most period ZIPs have the TSVs at the archive root, but at least
        # one (01jun2025-31aug2025) nests them under an upper-cased
        # subfolder -- match by basename, case-insensitively, instead of
        # assuming a flat layout.
        by_basename = {}
        for name in z.namelist():
            base = name.rsplit("/", 1)[-1]
            by_basename[base.upper()] = name

        for member in BULK_WANTED_MEMBERS:
            real_name = by_basename.get(member.upper())
            if real_name is None:
                print(f"    WARNING: {member} not found in {os.path.basename(zip_path)}")
                continue
            target = os.path.join(out_dir, member)
            if os.path.exists(target):
                continue
            with z.open(real_name) as src, open(target, "wb") as dst:
                dst.write(src.read())


def download_bulk() -> None:
    """Download every period of BULK_13F_URLS and extract BULK_WANTED_MEMBERS."""
    require_user_agent()

    total = len(BULK_13F_URLS)
    for i, (label, url) in enumerate(BULK_13F_URLS, 1):
        slug = slug_for(url)
        zip_path = os.path.join(BULK_ZIP_DIR, slug + ".zip")
        extract_dir = os.path.join(BULK_EXTRACT_DIR, slug)
        already_extracted = all(
            os.path.exists(os.path.join(extract_dir, m)) for m in BULK_WANTED_MEMBERS
        )

        if already_extracted:
            print(f"[{i}/{total}] {label}: already done, skipping")
            continue

        if not os.path.exists(zip_path):
            print(f"[{i}/{total}] {label}: downloading...")
            download_one(url, zip_path)
        else:
            print(f"[{i}/{total}] {label}: zip already downloaded")

        print(f"[{i}/{total}] {label}: extracting SUBMISSION.tsv + COVERPAGE.tsv")
        extract_wanted(zip_path, extract_dir)

        # Courtesy delay between requests to SEC's servers.
        time.sleep(0.5)

    print("Done.")


# =====================================================================
# 3. Parse the data sets into one lookup
# =====================================================================
# Combines all extracted SUBMISSION.tsv + COVERPAGE.tsv files (one pair per
# quarter/period) into a single lookup CSV keyed by accession_number. This
# covers every 13F-family filing from mid-2013 onward (SEC's bulk dataset
# scope). Filings not found here (older, text-format filings) need the
# scraping fallback of step 4.

BULK_LOOKUP_FIELDS = [
    "accession_number", "cik", "submissiontype", "periodofreport",
    "filing_date", "isamendment", "amendmentno", "amendmenttype",
]


def normalize_accession(raw: str) -> str:
    return raw.strip()


def load_submission_tsv(path, records):
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            acc = normalize_accession(row["ACCESSION_NUMBER"])
            rec = records.setdefault(acc, {})
            rec["cik"] = row.get("CIK", "").strip()
            rec["submissiontype"] = row.get("SUBMISSIONTYPE", "").strip()
            rec["periodofreport"] = row.get("PERIODOFREPORT", "").strip()
            rec["filing_date"] = row.get("FILING_DATE", "").strip()


def load_coverpage_tsv(path, records):
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            acc = normalize_accession(row["ACCESSION_NUMBER"])
            rec = records.setdefault(acc, {})
            rec["isamendment"] = row.get("ISAMENDMENT", "").strip()
            rec["amendmentno"] = row.get("AMENDMENTNO", "").strip()
            rec["amendmenttype"] = row.get("AMENDMENTTYPE", "").strip()


def parse_bulk() -> None:
    """Write BULK_LOOKUP_CSV from every period in BULK_EXTRACT_DIR."""
    records = {}
    periods = sorted(os.listdir(BULK_EXTRACT_DIR))
    if not periods:
        raise SystemExit(f"No extracted periods found in {BULK_EXTRACT_DIR} -- run the download step first")

    for period in periods:
        period_dir = os.path.join(BULK_EXTRACT_DIR, period)
        sub_path = os.path.join(period_dir, "SUBMISSION.tsv")
        cov_path = os.path.join(period_dir, "COVERPAGE.tsv")
        if os.path.exists(sub_path):
            load_submission_tsv(sub_path, records)
        if os.path.exists(cov_path):
            load_coverpage_tsv(cov_path, records)
        print(f"{period}: {len(records)} cumulative accession numbers")

    os.makedirs(os.path.dirname(BULK_LOOKUP_CSV), exist_ok=True)
    with open(BULK_LOOKUP_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=BULK_LOOKUP_FIELDS)
        writer.writeheader()
        for acc, rec in records.items():
            writer.writerow({
                "accession_number": acc,
                "cik": rec.get("cik", ""),
                "submissiontype": rec.get("submissiontype", ""),
                "periodofreport": rec.get("periodofreport", ""),
                "filing_date": rec.get("filing_date", ""),
                "isamendment": rec.get("isamendment", ""),
                "amendmentno": rec.get("amendmentno", ""),
                "amendmenttype": rec.get("amendmenttype", ""),
            })

    print(f"Wrote {len(records)} rows to {BULK_LOOKUP_CSV}")


# =====================================================================
# 4a. Amendment status from a filing's text
# =====================================================================
# Parses a fetched EDGAR 13F filing's raw text (the .txt full-submission
# file, or just the leading chunk of it). Handles both eras:
#   - Modern XML coverPage (~2013+): <isAmendment>, <amendmentNo>,
#     <amendmentType> tags embedded in the SGML-wrapped submission text.
#   - Legacy free-text filings (pre-2013): is_amendment comes from the
#     SEC-HEADER's "CONFORMED SUBMISSION TYPE:" field (e.g. "13F-HR/A"),
#     which is always present near the top of every classic .txt submission
#     -- far more reliable than the free-text cover-page checkbox, which
#     some of the very earliest (1999-2001) filings omit entirely. The
#     RESTATEMENT / NEW HOLDINGS ENTRIES distinction, when present, comes
#     from that checkbox pair: "(X) IS A RESTATEMENT" / "( ) ADDS NEW
#     HOLDINGS ENTRIES", with filers using inconsistent checkbox styles
#     ("(X)", "[X]", etc.) and casing across the years.

_XML_IS_AMENDMENT = re.compile(r"<isAmendment>\s*(true|false)\s*</isAmendment>", re.I)
_XML_AMENDMENT_NO = re.compile(r"<amendmentNo>\s*(\d+)\s*</amendmentNo>", re.I)
_XML_AMENDMENT_TYPE = re.compile(r"<amendmentType>\s*([^<]+?)\s*</amendmentType>", re.I)

_TXT_SUBMISSION_TYPE = re.compile(r"CONFORMED SUBMISSION TYPE:\s*([\w/\-]+)", re.I)

# Filers hand-typed/generated this cover page with wildly inconsistent
# checkbox styles across the years: "(X)", "[X]", "( X )", "|X|", "/X/",
# "{X}", and -- for at least one filer -- "[y]" used in place of "X" on
# every checkbox line indiscriminately (a genuine filer-side inconsistency;
# when that happens both RESTATEMENT and NEW HOLDINGS match, correctly
# surfacing as AMBIGUOUS rather than silently guessing).
_CHECKED_BOX = r"[\(\[\|/\{]\s*[XxYy]\s*[\)\]\|/\}]"
_TXT_AMENDMENT_NO = re.compile(r"AMENDMENT\s+NUMBER:?\s*(\d+)", re.I)
_TXT_RESTATEMENT = re.compile(_CHECKED_BOX + r"\s*IS\s+A\s+RESTATEMENT", re.I)
# "HOLDINGS" is sometimes singular ("adds new holding entries").
_TXT_NEW_HOLDINGS = re.compile(_CHECKED_BOX + r"\s*ADDS?\s+NEW\s+HOLDINGS?", re.I)


@dataclass
class AmendmentInfo:
    is_amendment: Optional[bool]
    amendment_no: Optional[str]
    amendment_type: Optional[str]
    format_detected: str  # 'xml' | 'legacy_text' | 'unknown'
    matched: bool  # whether is_amendment itself was determined decisively


def parse_amendment_info(text: str) -> AmendmentInfo:
    xml_match = _XML_IS_AMENDMENT.search(text)
    if xml_match:
        is_amendment = xml_match.group(1).lower() == "true"
        no_match = _XML_AMENDMENT_NO.search(text)
        type_match = _XML_AMENDMENT_TYPE.search(text)
        return AmendmentInfo(
            is_amendment=is_amendment,
            amendment_no=no_match.group(1) if no_match else None,
            amendment_type=type_match.group(1).strip() if type_match else None,
            format_detected="xml",
            matched=True,
        )

    header_match = _TXT_SUBMISSION_TYPE.search(text)
    if header_match:
        is_amendment = header_match.group(1).strip().upper().endswith("/A")

        amendment_type = None
        amendment_no = None
        if is_amendment:
            restatement = bool(_TXT_RESTATEMENT.search(text))
            new_holdings = bool(_TXT_NEW_HOLDINGS.search(text))
            if restatement and not new_holdings:
                amendment_type = "RESTATEMENT"
            elif new_holdings and not restatement:
                amendment_type = "NEW HOLDINGS ENTRIES"
            elif restatement and new_holdings:
                # Both boxes appear checked -- malformed filing.
                amendment_type = "AMBIGUOUS"
            # else: confirmed amendment, but this filing's cover page text
            # doesn't state RESTATEMENT vs NEW HOLDINGS at all (common in
            # the earliest, ~1999-2001 filings) -- amendment_type stays
            # None, which is itself the correct, final answer, not a
            # parsing failure.

            no_match = _TXT_AMENDMENT_NO.search(text)
            amendment_no = no_match.group(1) if no_match else None

        return AmendmentInfo(
            is_amendment=is_amendment,
            amendment_no=amendment_no,
            amendment_type=amendment_type,
            format_detected="legacy_text",
            matched=True,
        )

    # Neither the XML coverPage nor the SEC-HEADER's CONFORMED SUBMISSION
    # TYPE were found -- our fetched chunk was likely truncated before
    # reaching either (the header is normally within the first couple KB,
    # so this should be rare). Caller decides whether to retry with a
    # bigger fetch.
    return AmendmentInfo(
        is_amendment=None,
        amendment_no=None,
        amendment_type=None,
        format_detected="unknown",
        matched=False,
    )


# =====================================================================
# 4b. Scrape EDGAR filings not in the data sets
# =====================================================================
# Fetches individual EDGAR filings for accession numbers NOT found in the
# bulk 13F dataset (mainly pre-mid-2013 text-format filings). Respectful of
# SEC's fair-access rules:
#   - requires a real identifying User-Agent
#   - a shared, thread-safe rate limiter caps the *aggregate* request rate
#     across all worker threads at SCRAPE_REQUESTS_PER_SEC, comfortably under
#     SEC's 10 req/sec limit -- threads exist only to hide network latency,
#     not to send more traffic than a single well-paced stream would
#   - tries a small byte-range fetch first (the amendment info is always near
#     the top of the file, right after the SEC-HEADER) and only falls back to
#     downloading the whole file if that didn't contain decisive markers
#   - resumable: progress is appended to SCRAPE_PROGRESS_CSV and already-done
#     accession numbers are skipped on re-run

PROGRESS_FIELDS = [
    "accession_number", "cik", "rdate", "address",
    "is_amendment", "amendment_no", "amendment_type",
    "format_detected", "matched", "http_status", "note",
]


class RateLimiter:
    """Thread-safe token-bucket-ish limiter: blocks callers so that the
    combined call rate across all threads never exceeds `per_sec`."""

    def __init__(self, per_sec: float):
        self._interval = 1.0 / per_sec
        self._lock = threading.Lock()
        self._next_slot = time.monotonic()

    def wait(self):
        with self._lock:
            now = time.monotonic()
            start = max(now, self._next_slot)
            self._next_slot = start + self._interval
            sleep_for = start - now
        if sleep_for > 0:
            time.sleep(sleep_for)


_rate_limiter = RateLimiter(SCRAPE_REQUESTS_PER_SEC)


def fetch(url: str, byte_range: str = None):
    headers = {"User-Agent": SEC_USER_AGENT}
    if byte_range:
        headers["Range"] = byte_range
    req = urllib.request.Request(url, headers=headers)
    for attempt in range(1, SCRAPE_MAX_RETRIES + 1):
        _rate_limiter.wait()
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.status, resp.read().decode("utf-8", errors="replace"), resp.getheader("Content-Range")
        except urllib.error.HTTPError as e:
            if e.code == 416:  # range not satisfiable -> file smaller than range
                return e.code, "", None
            if e.code in (429, 503) or e.code >= 500:
                wait = min(30, 2 ** attempt)
                print(f"    HTTP {e.code}, retry {attempt}/{SCRAPE_MAX_RETRIES} after {wait}s: {url}")
                time.sleep(wait)
                continue
            return e.code, "", None
        except (urllib.error.URLError, TimeoutError) as e:
            wait = min(30, 2 ** attempt)
            print(f"    error {e}, retry {attempt}/{SCRAPE_MAX_RETRIES} after {wait}s: {url}")
            time.sleep(wait)
    return None, "", None


def load_done_accessions() -> set:
    done = set()
    if os.path.exists(SCRAPE_PROGRESS_CSV):
        with open(SCRAPE_PROGRESS_CSV, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                done.add(row["accession_number"])
    return done


def load_bulk_accessions() -> set:
    found = set()
    if os.path.exists(BULK_LOOKUP_CSV):
        with open(BULK_LOOKUP_CSV, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                found.add(row["accession_number"])
    return found


def scrape_one(cik, rdate, address, accession_number):
    url = EDGAR_BASE + address
    status, text, content_range = fetch(url, byte_range=f"bytes=0-{SCRAPE_RANGE_BYTES}")
    if status is None:
        return {"http_status": "error", "note": "fetch failed after retries",
                "is_amendment": "", "amendment_no": "", "amendment_type": "",
                "format_detected": "", "matched": "false"}

    info = parse_amendment_info(text)
    note = ""
    # Only worth re-fetching the whole file if our chunk was truncated
    # before we could even find the SEC-HEADER / XML coverPage tag --
    # that's normally within the first couple KB, so this should be rare
    # (a genuinely non-amendment filing still resolves as matched=True from
    # the header, so this doesn't cost extra requests for the common case).
    if not info.matched and status in (200, 206) and content_range:
        status2, text2, _ = fetch(url)
        if status2 in (200, 206):
            info = parse_amendment_info(text2)
            status = status2
            note = "full-file fallback"

    return {
        "http_status": status,
        "note": note,
        "is_amendment": info.is_amendment,
        "amendment_no": info.amendment_no or "",
        "amendment_type": info.amendment_type or "",
        "format_detected": info.format_detected,
        "matched": info.matched,
    }


def _pending_rows():
    bulk_accessions = load_bulk_accessions()
    done = load_done_accessions()
    print(f"{len(bulk_accessions)} accessions covered by bulk dataset; {len(done)} already scraped in a prior run")

    rows = []
    seen = set()
    with open(INPUT_ROWS_CSV, newline="", encoding="utf-8") as in_f:
        for row in csv.DictReader(in_f):
            acc = row["accession_number"]
            if acc in bulk_accessions or acc in done or acc in seen:
                continue
            seen.add(acc)
            rows.append(row)
    return rows


def scrape_legacy_filings() -> None:
    """Scrape every input filing that is neither in BULK_LOOKUP_CSV nor already in SCRAPE_PROGRESS_CSV."""
    require_user_agent()
    if not os.path.exists(INPUT_ROWS_CSV):
        raise SystemExit(f"{INPUT_ROWS_CSV} not found -- run the fetch step first")

    rows = _pending_rows()
    n_todo = len(rows)
    print(f"{n_todo} distinct filings need scraping, using {SCRAPE_WORKER_THREADS} threads "
          f"capped at {SCRAPE_REQUESTS_PER_SEC} req/sec total")

    write_header = not os.path.exists(SCRAPE_PROGRESS_CSV)
    write_lock = threading.Lock()
    n_done = 0
    t0 = time.time()

    with open(SCRAPE_PROGRESS_CSV, "a", newline="", encoding="utf-8") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=PROGRESS_FIELDS)
        if write_header:
            writer.writeheader()
            out_f.flush()

        with ThreadPoolExecutor(max_workers=SCRAPE_WORKER_THREADS) as pool:
            futures = {
                pool.submit(scrape_one, row["cik"], row["rdate"], row["address"], row["accession_number"]): row
                for row in rows
            }
            for future in as_completed(futures):
                row = futures[future]
                result = future.result()
                with write_lock:
                    writer.writerow({
                        "accession_number": row["accession_number"],
                        "cik": row["cik"],
                        "rdate": row["rdate"],
                        "address": row["address"],
                        **result,
                    })
                    out_f.flush()
                    n_done += 1
                    if n_done % 500 == 0:
                        elapsed = time.time() - t0
                        rate = n_done / elapsed
                        eta_min = (n_todo - n_done) / rate / 60 if rate > 0 else float("inf")
                        print(f"  ...{n_done}/{n_todo} ({rate:.1f}/s, ETA {eta_min:.0f} min)")

    print("Done.")


# =====================================================================
# 5. Merge into the final output
# =====================================================================
# Joins INPUT_ROWS_CSV with BULK_LOOKUP_CSV (2013-H2+ filings) and
# SCRAPE_PROGRESS_CSV (pre-2013 / fallback-scraped filings). source is one
# of: bulk_dataset, scrape_xml, scrape_legacy_text, scrape_inconclusive,
# unresolved (not yet scraped -- run the scrape step again).

FINAL_FIELDS = [
    "cik", "rdate", "address", "accession_number", "submission_type",
    "is_amendment", "amendment_no", "amendment_type", "source",
]


def to_bool(s: str):
    return str(s).strip().lower() in ("true", "yes", "y", "1")


def normalize_amendment_type(s: str) -> str:
    # Legacy free-text cover pages say "ADDS NEW HOLDINGS ENTRIES"; the
    # modern XML coverPage's controlled vocabulary just says "NEW HOLDINGS".
    # Normalize to the latter so both sources agree.
    if s == "NEW HOLDINGS ENTRIES":
        return "NEW HOLDINGS"
    return s


def load_bulk():
    lookup = {}
    with open(BULK_LOOKUP_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            lookup[row["accession_number"]] = row
    return lookup


def load_scrape():
    lookup = {}
    with open(SCRAPE_PROGRESS_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            lookup[row["accession_number"]] = row
    return lookup


def merge_results() -> None:
    """Write FINAL_OUTPUT_CSV: one row per input filing with its amendment type and source."""
    bulk = load_bulk()
    try:
        scraped = load_scrape()
    except FileNotFoundError:
        scraped = {}

    n_bulk = n_scrape = n_unresolved = 0
    with open(INPUT_ROWS_CSV, newline="", encoding="utf-8") as in_f, \
         open(FINAL_OUTPUT_CSV, "w", newline="", encoding="utf-8") as out_f:
        reader = csv.DictReader(in_f)
        writer = csv.DictWriter(out_f, fieldnames=FINAL_FIELDS)
        writer.writeheader()

        for row in reader:
            acc = row["accession_number"]
            base = {"cik": row["cik"], "rdate": row["rdate"], "address": row["address"], "accession_number": acc}

            if acc in bulk:
                b = bulk[acc]
                submission_type = b.get("submissiontype", "")
                # SUBMISSIONTYPE (from EDGAR's own filing header) is more
                # reliable than COVERPAGE's self-reported ISAMENDMENT flag,
                # which is occasionally blank even for a confirmed ".../A"
                # submission (a gap in the filer's own XML, not our data).
                is_amendment = to_bool(b.get("isamendment", "")) or submission_type.strip().upper().endswith("/A")
                writer.writerow({
                    **base,
                    "submission_type": submission_type,
                    "is_amendment": is_amendment,
                    "amendment_no": b.get("amendmentno", ""),
                    "amendment_type": normalize_amendment_type(b.get("amendmenttype", "")),
                    "source": "bulk_dataset",
                })
                n_bulk += 1
            elif acc in scraped:
                s = scraped[acc]
                fmt = s.get("format_detected", "unknown")
                matched = to_bool(s.get("matched", ""))
                source = f"scrape_{fmt}" if matched else "scrape_inconclusive"
                writer.writerow({
                    **base,
                    "submission_type": "",
                    "is_amendment": s.get("is_amendment", ""),
                    "amendment_no": s.get("amendment_no", ""),
                    "amendment_type": normalize_amendment_type(s.get("amendment_type", "")),
                    "source": source,
                })
                n_scrape += 1
            else:
                writer.writerow({
                    **base,
                    "submission_type": "",
                    "is_amendment": "",
                    "amendment_no": "",
                    "amendment_type": "",
                    "source": "unresolved",
                })
                n_unresolved += 1

    print(f"bulk_dataset: {n_bulk}")
    print(f"scraped: {n_scrape}")
    print(f"unresolved (run the scrape step again): {n_unresolved}")
    print(f"Wrote {FINAL_OUTPUT_CSV}")
