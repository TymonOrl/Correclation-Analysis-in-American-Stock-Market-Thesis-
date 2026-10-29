"""Configuration of the 13(f) securities lists pipeline: paths, which lists are scanned, OCR settings."""

import os
from pathlib import Path

# --- Local paths ----------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent         # .../Database/13f_securities
DATA_DIR = BASE_DIR / "data"
PDF_DIR = DATA_DIR / "pdf"                          # SEC lists as published, 13flistYYYYqN.pdf (1996 Q1 - 2019 Q4)
TXT_DIR = DATA_DIR / "txt"                          # fixed-width lists: converted / OCR'd, 2020 as published by SEC
CSV_DIR = DATA_DIR / "csv"                          # one CSV per list, loaded by 2_f13_securities_pipeline.ipynb
OCR_CACHE_DIR = DATA_DIR / "ocr_cache"              # hOCR per page, so the OCR is not repeated
OCR_ISSUES_TSV = DATA_DIR / "ocr_issues.tsv"        # repaired / still invalid rows of the OCR'd lists

# --- Which PDFs need OCR --------------------------------------------------
# 1996-2003 lists are scanned images (only the page header is text); from 2004 the PDFs have a text layer.
SCANNED_YEARS = range(1996, 2004)

# --- Text PDFs (pdftotext -bbox) -------------------------------------------
TEXT_PDF_TOL = 3.0  # points of slack when assigning a word to a column

# --- OCR (pdftoppm + tesseract) --------------------------------------------
OCR_DPI = 300
# Share of CUSIPs valid as read, below which image clean-up variants are tried
OCR_DEGRADED_BELOW = 0.90
OCR_JOBS = max(1, (os.cpu_count() or 2) - 2)
