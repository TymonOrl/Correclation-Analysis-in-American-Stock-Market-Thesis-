# SEC Official Lists of Section 13(f) Securities

Turns the SEC's quarterly lists of 13(f) securities (the securities 13F filers must report), 1996 Q1 – 2020 Q4, into
one CSV per quarter. The CSVs are loaded by `Initialisation/notebooks/2_f13_securities_pipeline.ipynb`, which builds
the CUSIP reference table `clean_up_1.asset` from them.

```
13f_securities/
├── main.py                 runs the steps below
├── securities_helpers.py   functions, one section per step
├── config.py               paths, scanned years, OCR settings
├── data/
│   ├── pdf/                lists as published by SEC, 13flistYYYYqN.pdf, 1996 Q1 – 2019 Q4   (git-ignored)
│   ├── txt/                fixed-width lists; 2020 as published by SEC (TXT only)             (git-ignored)
│   ├── csv/                one CSV per list: cusip, has_option, issuer_name, issuer_description, status, flag
│   ├── ocr_issues.tsv      every repaired or still invalid row of the OCR'd lists
│   └── ocr_cache/          hOCR per page, so the OCR is not repeated                          (git-ignored)
└── old/                    create_13f_table.sql, load_13f_csv.sh: first loader (table sec_13f_list),
                            replaced by 2_f13_securities_pipeline.ipynb
```

The PDFs and the 2020 TXT files were downloaded from sec.gov by hand; the 1997 Q4 list is missing.

## Sources and method

* **2004 – 2019: text PDFs.** Word positions from `pdftotext -bbox`; the listing is monospaced, so the spacing inside
  a field is rebuilt from the gaps between words.
* **1996 – 2003: scanned PDFs** (`SCANNED_YEARS`; only the page header is text). Each page is rendered at 300 dpi
  and read by tesseract with per-character boxes. The 1996–2002 printouts are monospaced, so the characters are put
  back on the character grid; the 2003 printouts use a proportional font and are split by word positions. Degraded
  scans are re-read with stroke-thickening clean-up, page by page where it reads more valid CUSIPs.
* **CUSIP repair.** A CUSIP failing its check digit is *repaired* when exactly one attested CUSIP (in a 2004+ list,
  in two or more scanned quarters, or repeated in the same list) is within two edits; else *name-matched* when the
  issuer name appears elsewhere with one CUSIP within four characters; else *guessed* when exactly one valid CUSIP is
  a single OCR-confusable substitution away. All of them, and the rows still invalid, go to `data/ocr_issues.tsv`.
* CALL / PUT rows of the PDFs are dropped: the SEC's TXT format has no option rows (the `*` options flag is kept).

## Order of execution

`python main.py` runs all steps in order; `python main.py csv check` runs only the given ones.

| # | Step | Function | What it does | Writes |
|---|---|---|---|---|
| 1 | `text_pdfs` | `text_pdfs_to_txt` | Text PDFs (2004+) → TXT | `data/txt/` |
| 2 | `ocr` | `ocr_pdfs_to_txt` | Scanned PDFs (1996–2003) → TXT, CUSIP repair against the lists of step 1 and 2020; hours without the cache | `data/txt/`, `data/ocr_issues.tsv`, `data/ocr_cache/` |
| 3 | `csv` | `txt_to_csv` | TXT → CSV | `data/csv/` |
| 4 | `check` | `check_consistency` | Share of CUSIPs in consecutive lists whose name / description / options flag agree: a proxy for the reading accuracy | printed table |

## Requirements

* poppler-utils (`pdftotext`, `pdftoppm`, `pdfinfo`) and tesseract
* Python package `Pillow`
