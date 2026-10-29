"""
SEC Official Lists of Section 13(f) Securities, 1996 Q1 - 2020 Q4: PDF -> fixed-width TXT -> CSV
(the CSVs are loaded by Initialisation/notebooks/2_f13_securities_pipeline.ipynb).

    python main.py                  # all steps, in order
    python main.py csv check        # only the given steps, in the given order

Functions: securities_helpers.py; paths, scanned years and OCR settings: config.py.
"""

import sys

import securities_helpers as sh
from config import (CSV_DIR, OCR_CACHE_DIR, OCR_ISSUES_TSV, OCR_JOBS, PDF_DIR, SCANNED_YEARS, TXT_DIR)


def year_of(path):
    return int(path.stem[7:11])   # 13flistYYYYqN


def pdfs(scanned):
    return [p for p in sorted(PDF_DIR.glob("13flist*q*.pdf")) if (year_of(p) in SCANNED_YEARS) == scanned]


def txts():
    return sorted(TXT_DIR.glob("13flist*q*.txt"))   # file names sort chronologically


def text_pdfs():
    """Text-based PDFs (2004+) -> TXT."""
    failed = sh.text_pdfs_to_txt(pdfs(scanned=False), TXT_DIR)
    if failed:
        raise SystemExit(f"no records parsed from {[p.name for p in failed]}")


def ocr():
    """Scanned PDFs (SCANNED_YEARS) -> TXT; the CUSIPs of the other lists count as attested for the repair."""
    known = [t for t in txts() if year_of(t) not in SCANNED_YEARS]
    sh.ocr_pdfs_to_txt(pdfs(scanned=True), TXT_DIR, OCR_CACHE_DIR, known, OCR_JOBS, OCR_ISSUES_TSV)


def csv():
    """TXT -> CSV."""
    sh.txt_to_csv(TXT_DIR, CSV_DIR)


def check():
    """Agreement of consecutive quarters (reading-accuracy proxy)."""
    sh.check_consistency(txts())


STEPS = {
    "text_pdfs": text_pdfs,   # data/pdf (2004+)      -> data/txt
    "ocr":       ocr,         # data/pdf (1996-2003)  -> data/txt, data/ocr_issues.tsv (needs the 2004+ TXT)
    "csv":       csv,         # data/txt              -> data/csv
    "check":     check,       # data/txt              -> printed table
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
