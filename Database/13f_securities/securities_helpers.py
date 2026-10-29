"""
Functions of the 13(f) securities lists pipeline (run by main.py): SEC Official Lists of Section 13(f) Securities
as PDF -> fixed-width TXT -> CSV.

  0. common                 CUSIP check digit, fixed-width TXT record
  1. text_pdfs_to_txt       text-based PDFs (2004+), word positions from pdftotext -bbox
  2. ocr_pdfs_to_txt        scanned PDFs (1996-2003): tesseract OCR, character grid / word boxes, CUSIP repair
  3. txt_to_csv             fixed-width TXT -> CSV
  4. check_consistency      agreement of consecutive quarters, a proxy for the reading accuracy

TXT record layout (80 chars, 1-based columns), as the SEC publishes it from 2020 onward:
    1-9    CUSIP (6-char issuer + 2-char issue + check digit, no spaces)
    10     '*' if the security has listed options, else blank
    11-40  issuer name (30)
    41-67  issuer description (27)
    68-70  status: '*A*' added, '*D*' deleted, blank otherwise
    71-79  blank
    80     exchange code / extra one-letter flag -- NOT present in the PDFs, left blank

CALL/PUT option rows appear in the PDFs but not in the TXT format, so they are dropped.
"""
import csv
import difflib
import html
import os
import re
import statistics
import subprocess
import tempfile
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from PIL import Image, ImageFilter

from config import OCR_DEGRADED_BELOW, OCR_DPI, TEXT_PDF_TOL


# =====================================================================
# 0. Common
# =====================================================================

def cusip_check_digit(base8):
    total = 0
    for i, ch in enumerate(base8):
        if ch.isdigit():
            v = int(ch)
        elif ch.isalpha():
            v = ord(ch) - ord('A') + 10
        else:
            v = {'*': 36, '@': 37, '#': 38}[ch]
        if i % 2 == 1:
            v *= 2
        total += v // 10 + v % 10
    return str((10 - total % 10) % 10)


def format_record(cusip, flag, name, desc, status, exch=' '):
    for label, value, width in (('name', name, 30), ('desc', desc, 27)):
        if len(value) > width:
            raise ValueError(f'{cusip}: {label} {value!r} longer than {width}')
    return f'{cusip}{flag}{name:<30}{desc:<27}{status:<3}{"":9}{exch}'


# =====================================================================
# 1. Text-based PDFs (2004+)
# =====================================================================
# Word positions come from `pdftotext -bbox` (poppler). The listing is set in a
# monospaced font, so spacing inside a field is rebuilt from the gaps between words.

TEXT_WORD_RE = re.compile(
    r'<word xMin="([\d.]+)" yMin="([\d.]+)" xMax="([\d.]+)" yMax="([\d.]+)">(.*?)</word>')
TEXT_PAGE_RE = re.compile(r'<page\b.*?</page>', re.S)
CUSIP6_RE = re.compile(r'^[0-9A-Z]{6}$')
ISSUE_RE = re.compile(r'^[0-9A-Z]{2}$')
CHECK_RE = re.compile(r'^[0-9]$')
STATUS_CODES = {'ADDED': '*A*', 'DELETED': '*D*'}


def read_pages(pdf):
    out = subprocess.run(['pdftotext', '-bbox', str(pdf), '-'],
                         capture_output=True, text=True, check=True).stdout
    for page in TEXT_PAGE_RE.findall(out):
        words = [(float(a), float(b), float(c), float(d), html.unescape(t))
                 for a, b, c, d, t in TEXT_WORD_RE.findall(page)]
        yield words


def group_lines(words):
    """Group words into text lines by vertical centre."""
    lines = []
    for w in sorted(words, key=lambda w: ((w[1] + w[3]) / 2, w[0])):
        yc = (w[1] + w[3]) / 2
        if lines and abs(lines[-1][0] - yc) < 3.0:
            lines[-1][1].append(w)
        else:
            lines.append([yc, [w]])
    return [sorted(ws, key=lambda w: w[0]) for _, ws in lines]


def text_find_columns(lines):
    """Locate column x-starts from the 'CUSIP NO  ISSUER NAME  ISSUER DESCRIPTION STATUS' header."""
    for ws in lines:
        texts = [w[4] for w in ws]
        if 'STATUS' in texts and texts.count('ISSUER') == 2:
            issuer = [w[0] for w in ws if w[4] == 'ISSUER']
            status = next(w[0] for w in ws if w[4] == 'STATUS')
            return issuer[0], issuer[1], status
    return None


def text_join_field(ws, cw):
    """Rebuild a field's text, restoring runs of spaces from inter-word gaps."""
    s = ''
    prev_end = None
    for x0, _, x1, _, t in ws:
        if prev_end is not None:
            s += ' ' * max(1, round((x0 - prev_end) / cw))
        s += t
        prev_end = x1
    return s


def parse_text_pdf(pdf, stats):
    records = []
    cols = None
    for words in read_pages(pdf):
        lines = group_lines(words)
        cols = text_find_columns(lines) or cols
        if cols is None:
            continue
        name_x, desc_x, status_x = cols
        for ws in lines:
            if len(ws) < 4 or not CUSIP6_RE.match(ws[0][4]) or ws[0][0] > name_x:
                continue
            if not (ISSUE_RE.match(ws[1][4]) and CHECK_RE.match(ws[2][4])):
                continue
            cusip = ws[0][4] + ws[1][4] + ws[2][4]
            rest = ws[3:]
            flag = ' '
            if rest and rest[0][4] == '*' and rest[0][0] < name_x - TEXT_PDF_TOL:
                flag = '*'
                rest = rest[1:]
            # character width of the monospaced listing font, measured on this line
            widths = [(w[2] - w[0]) / len(w[4]) for w in rest if len(w[4]) >= 3]
            cw = statistics.median(widths) if widths else 6.0
            name_ws = [w for w in rest if w[0] < desc_x - TEXT_PDF_TOL]
            desc_ws = [w for w in rest if desc_x - TEXT_PDF_TOL <= w[0] < status_x - TEXT_PDF_TOL]
            stat_ws = [w for w in rest if w[0] >= status_x - TEXT_PDF_TOL]
            name = text_join_field(name_ws, cw)
            desc = text_join_field(desc_ws, cw)
            status_word = ' '.join(w[4] for w in stat_ws)
            if status_word and status_word not in STATUS_CODES:
                stats['warnings'].append(f'{pdf.name}: unexpected status {status_word!r} for {cusip}')
            stats['rows'] += 1
            if desc in ('CALL', 'PUT'):
                # option rows reuse the underlying's check digit, so skip validating them
                stats['options'] += 1
                continue
            if cusip_check_digit(cusip[:8]) != cusip[8]:
                stats['bad_check'].append(cusip)
            records.append((cusip, flag, name, desc, STATUS_CODES.get(status_word, '')))
    return records


def text_pdfs_to_txt(pdfs, outdir):
    """Convert text-based PDFs to fixed-width TXT in outdir. Returns the PDFs nothing could be parsed from."""
    failed = []
    for pdf in pdfs:
        stats = {'rows': 0, 'options': 0, 'bad_check': [], 'warnings': []}
        records = parse_text_pdf(pdf, stats)
        outdir.mkdir(parents=True, exist_ok=True)
        out = outdir / (pdf.stem + '.txt')
        out.write_text(''.join(format_record(*r) + '\n' for r in records))
        print(f'{pdf.name}: {stats["rows"]} rows, {stats["options"]} CALL/PUT dropped, '
              f'{len(records)} written -> {out}'
              + (f', {len(stats["bad_check"])} bad CUSIP check digits' if stats['bad_check'] else ''))
        for w in stats['warnings'][:10]:
            print('  WARN', w)
        if not records:
            print(f'  ERROR: no records parsed from {pdf.name} (scanned image PDF?)')
            failed.append(pdf)
    return failed


# =====================================================================
# 2. Scanned PDFs (1996-2003): OCR
# =====================================================================
# Pipeline per PDF:
#   1. render each page (pdftoppm) and OCR it with tesseract, restricted to
#      the characters the list actually uses, keeping per-character boxes (hOCR); the
#      hOCR is cached so the later stages can be re-run without re-OCRing. Files whose
#      CUSIPs mostly fail to read (broken-stroke scans) are re-OCRed with stroke-thickening
#      clean-up, keeping per page whichever reading gives the most valid CUSIPs
#   2. the 1996-2002 lists are monospaced printouts, so every character is placed on the
#      page's character grid, turning each OCR line back into the fixed-width report line;
#      the 2003 lists are set in a proportional font and are split by word positions.
#      Column positions are learned per file from the data
#   3. validate each CUSIP with its check digit. An invalid one is 'repaired' when exactly
#      one attested CUSIP (a 2004+ list, two or more scanned quarters, or repeated in this
#      list) is within two character edits; else 'name_matched' when the issuer name is
#      listed elsewhere with one CUSIP within four characters; else 'guessed' when exactly
#      one valid CUSIP is a single OCR-confusable substitution away; all are logged
#   4. drop CALL/PUT rows, write the TXT, and log every row that still fails
#      validation to the issues TSV for manual review

WHITELIST = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789/.%&-*$+()':"
ALNUM = set('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789')
# characters OCR commonly confuses in this typeface
CONFUSE = {
    '0': 'ODQ8', 'O': '0DQ', 'D': '0O', 'Q': '0O', '1': 'IL7', 'I': '1L', 'L': '1I',
    '2': 'Z', 'Z': '2', '5': 'S', 'S': '58', '8': 'B3S0', 'B': '8', '6': 'G', 'G': '6C',
    'C': 'G', '7': '1T', 'T': '7', '4': 'A', 'A': '4', '3': '8', '9': 'G', 'U': 'V', 'V': 'U',
}
STATUS_WORDS = {'ADDED': '*A*', 'DELETED': '*D*'}


# ---------------------------------------------------------------- OCR stage

# Image clean-up for badly degraded (broken-stroke) scans: thicken the strokes to close
# the gaps, then soften. It hurts clean scans, so it is only tried on files whose CUSIPs
# mostly fail to read (OCR_DEGRADED_BELOW), and kept page by page only where it reads more valid CUSIPs.
VARIANTS = {
    'thick3blur': lambda im: im.filter(ImageFilter.MinFilter(3)).filter(ImageFilter.GaussianBlur(1)),
    'thick5blur': lambda im: im.filter(ImageFilter.MinFilter(5)).filter(ImageFilter.GaussianBlur(1)),
}


def ocr_page(args):
    pdf, page, out_path, variant = args
    if out_path.exists():
        return
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp) / 'p'
        subprocess.run(['pdftoppm', '-r', str(OCR_DPI), '-gray', '-singlefile', '-f', str(page), '-l', str(page),
                        '-png', str(pdf), str(base)], check=True)
        if variant:
            VARIANTS[variant](Image.open(f'{base}.png')).save(f'{base}.png')
        res = subprocess.run(['tesseract', f'{base}.png', str(base), '--psm', '6',
                              '-c', f'tessedit_char_whitelist={WHITELIST} ',
                              '-c', 'hocr_char_boxes=1', 'hocr'],
                             capture_output=True, text=True, env={**os.environ, 'OMP_THREAD_LIMIT': '1'})
        if res.returncode != 0:
            raise RuntimeError(f'tesseract failed on {pdf} p{page}: {res.stderr}')
        tmp_out = out_path.with_suffix('.part')
        tmp_out.write_text(Path(f'{base}.hocr').read_text())
    tmp_out.rename(out_path)


def page_count(pdf):
    out = subprocess.run(['pdfinfo', str(pdf)], capture_output=True, text=True, check=True).stdout
    return int(re.search(r'^Pages:\s+(\d+)', out, re.M).group(1))


def page_dir(cache, pdf, variant=None):
    return cache / pdf.stem / variant if variant else cache / pdf.stem


def run_ocr(pdfs, cache, jobs, variant=None):
    tasks = []
    for pdf in pdfs:
        d = page_dir(cache, pdf, variant)
        d.mkdir(parents=True, exist_ok=True)
        tasks += [(pdf, p, d / f'{p:04d}.hocr', variant) for p in range(1, page_count(pdf) + 1)]
    todo = [t for t in tasks if not t[2].exists()]
    print(f'OCR{" (" + variant + ")" if variant else ""}: {len(tasks)} pages, '
          f'{len(todo)} not cached yet', flush=True)
    with ProcessPoolExecutor(jobs) as ex:
        for i, _ in enumerate(ex.map(ocr_page, todo, chunksize=4), 1):
            if i % 500 == 0 or i == len(todo):
                print(f'  {i}/{len(todo)} pages', flush=True)


# ---------------------------------------------------------------- grid stage

LINE_RE = re.compile(r"<span class='ocr_line'[^>]*title=\"bbox (\d+) (\d+) (\d+) (\d+)[^\"]*\">(.*?)(?=<span class='ocr_line'|</p>)", re.S)
WORD_RE = re.compile(r"<span class='ocrx_word'[^>]*title='bbox (\d+) \d+ (\d+) \d+;.*?</span>\s*</span>", re.S)
CHAR_RE = re.compile(r"<span class='ocrx_cinfo' title='x_bboxes (\d+) (\d+) (\d+) (\d+);[^']*'>([^<]*)</span>")


def load_lines(path):
    """OCR lines as (y, words) top to bottom; each word is (left, right, text, chars)
    with chars a list of (x_center, char)."""
    out = []
    for m in LINE_RE.finditer(path.read_text()):
        words = []
        for w in WORD_RE.finditer(m.group(5)):
            chars = [((int(a) + int(c)) / 2, html.unescape(t)) for a, _, c, _, t in CHAR_RE.findall(w.group(0))]
            chars = [(x, t) for x, t in chars if t.strip()]
            if chars:
                words.append((int(w.group(1)), int(w.group(2)), ''.join(t for _, t in chars), chars))
        if words:
            out.append(((int(m.group(2)) + int(m.group(4))) / 2, words))
    return out


def is_cusip_start(words):
    """Line begins with a 6-character issuer code followed by a 2-character issue code."""
    return len(words) >= 2 and len(words[0][2]) == 6 and len(words[1][2]) == 2


def file_pitch(pages):
    """Character advance: issuer code centres step one column per character."""
    steps = [(ws[0][3][5][0] - ws[0][3][0][0]) / 5 for _, lines in pages for _, ws in lines if is_cusip_start(ws)]
    return statistics.median(steps) if steps else None


def fit_origin(lines, pitch):
    """x of column 0 as a function of y, fitted to lines that start with a CUSIP
    (absorbs page skew); None when the page has no usable CUSIP lines."""
    pts = [(y, ws[0][3][0][0]) for y, ws in lines if is_cusip_start(ws)]
    if len(pts) < 5:
        return None
    med = statistics.median(x for _, x in pts)
    pts = [(y, x) for y, x in pts if abs(x - med) < pitch]
    if len(pts) < 5:
        return None
    my, mx = statistics.mean(y for y, _ in pts), statistics.mean(x for _, x in pts)
    var = sum((y - my) ** 2 for y, _ in pts)
    slope = sum((y - my) * (x - mx) for y, x in pts) / var if var else 0.0
    return lambda y: mx + slope * (y - my)


def to_grid(words, x0, pitch):
    """Place characters on the monospaced grid. Columns are assigned left to right and
    never collide; a word break always leaves at least one blank column."""
    cells = {}
    col = -2
    for wi, word in enumerate(words):
        for ci, (x, ch) in enumerate(word[3]):
            want = round((x - x0) / pitch)
            floor_ = col + (2 if ci == 0 and wi > 0 else 1)
            col = max(want, floor_)
            cells[col] = ch
    return cells


def grid_line(cells):
    return ''.join(cells.get(i, ' ') for i in range(max(cells) + 1))


def learn_columns(grids):
    """Most common columns of the options flag and the description and status fields."""
    desc_c, stat_c, flag_c = Counter(), Counter(), Counter()
    for line in grids:
        for m in re.finditer(r'\S+', line):
            c, t = m.start(), m.group()
            if 11 <= c <= 25 and t == '*':
                flag_c[c] += 1
            if t in ('CALL', 'PUT', 'COM', 'ORD'):
                desc_c[c] += 1
            elif t in ('ADDED', 'DELETED'):
                stat_c[c] += 1
    flag = flag_c.most_common(1)[0][0] if flag_c else 12
    desc = max((c for c in desc_c if c > flag + 5), key=desc_c.get)
    status = stat_c.most_common(1)[0][0] if sum(stat_c.values()) >= 5 else desc + 20
    return flag, desc, status


def is_monospaced(pages):
    """Monospaced printouts (1996-2002) have a near-constant step between consecutive
    characters of a word; the proportional Times printouts of 2003 do not."""
    gaps = sorted(b[0] - a[0] for _, lines in pages for _, ws in lines for w in ws
                  for a, b in zip(w[3], w[3][1:]))
    trimmed = gaps[len(gaps) // 10:-(len(gaps) // 10) or None]
    return len(trimmed) > 100 and statistics.pstdev(trimmed) < 2.0


def valid_per_page(rows):
    return Counter(r['page'] for r in rows if not is_option(r) and valid(r['cusip']))


def parse_best(pdf, cache, jobs):
    """Parse a PDF's OCR; for a degraded scan, also OCR clean-up variants and use, page
    by page, whichever reading yields the most valid CUSIPs."""
    files = sorted(page_dir(cache, pdf).glob('*.hocr'))
    rows = parse_pdf(files)
    body = [r for r in rows if not is_option(r)]
    rate = sum(valid(r['cusip']) for r in body) / max(1, len(body))
    if rate >= OCR_DEGRADED_BELOW:
        return rows
    print(f'{pdf.name}: only {100 * rate:.0f}% of CUSIPs read validly, trying image clean-up', flush=True)
    best = {f.stem: (valid_per_page(rows)[int(f.stem)], f) for f in files}
    for variant in VARIANTS:
        run_ocr([pdf], cache, jobs, variant)
        vfiles = sorted(page_dir(cache, pdf, variant).glob('*.hocr'))
        counts = valid_per_page(parse_pdf(vfiles))
        for f in vfiles:
            if counts[int(f.stem)] > best[f.stem][0]:
                best[f.stem] = (counts[int(f.stem)], f)
    return parse_pdf(sorted((f for _, f in best.values()), key=lambda f: f.stem))


def parse_pdf(files):
    pages = [(int(p.stem), load_lines(p)) for p in files]
    if not is_monospaced(pages):
        return parse_words([(page, [[w[:3] for w in ws] for _, ws in lines]) for page, lines in pages])
    pitch = file_pitch(pages)
    if not pitch:
        return []
    grids = []  # (page, fixed-width line)
    for page, lines in pages:
        origin = fit_origin(lines, pitch)
        if origin is None:
            continue
        for y, words in lines:
            # punctuation-only specks carry no information and disturb the grid
            words = [w for w in words if not re.fullmatch(r"[.,:'-]+", w[2])]
            cells = to_grid(words, origin(y), pitch)
            cells = {c: ch for c, ch in cells.items() if c >= 0}  # margin specks
            if cells:
                grids.append((page, grid_line(cells)))
    if not grids:
        return []
    flag_c, desc_c, stat_c = learn_columns(g for _, g in grids)

    rows = []
    for page, line in grids:
        line = line.ljust(stat_c + 10)
        # CUSIP in columns 0-10, flag at flag_c, name from flag_c + 2 (ends 2 columns
        # before the description), description, status; field cuts sit mid-gap so a
        # one-column OCR misplacement doesn't move a character into the wrong field
        head = line[:flag_c - 1]
        if ':' in head or head.count('/') >= 2 or head.lstrip().startswith('R'):
            continue  # page header ("RUN DATE: 09/22/99"); no CUSIP starts with R
        toks = head.split()
        if not toks:
            continue
        pieces = [''.join(ch for ch in t if ch in ALNUM) for t in toks]
        pieces = [p for p in pieces if p]
        # any ink in the flag column is the options flag, whatever glyph OCR saw -- unless
        # it is a lone digit completing a CUSIP that a spurious character pushed right
        window = line[flag_c - 1:flag_c + 2].strip()
        if len(window) == 1 and window.isdigit() and len(pieces) == 2:
            pieces.append(window)
            window = ''
        flag = '*' if window else ' '
        chars = ''.join(pieces)
        if not 7 <= len(chars) <= 13 or sum(ch.isdigit() or ch == 'O' for ch in chars) < 4:
            continue  # real CUSIPs have 5+ digits; rejects 'CUSIP NO.' / 'RUN TIME' lines
        name = line[flag_c + 2:desc_c - 1].strip()
        desc = line[desc_c - 1:stat_c - 1].strip()
        status = line[stat_c - 1:].strip()
        if not name:
            continue
        rows.append({'page': page, 'cusip': normalize(chars), 'flag': flag, 'raw': head.strip(),
                     'pieces': pieces, 'name': name, 'desc': desc, 'status': status, 'grid': True})
    return rows


# ---------------------------------------------------------------- word-box parser
# (for the proportional-font printouts of 2003, where there is no character grid)

def find_columns(lines):
    for ws in lines:
        texts = [w[2] for w in ws]
        issuer = [w[0] for w in ws if 'ISSUER' in w[2] or w[2] in ('ISSUEP', '1SSUER')]
        status = [w[0] for w in ws if 'STATUS' in w[2] or w[2] in ('STATU5',)]
        if len(issuer) >= 2 and status and any('CUSIP' in t or 'NO' in t for t in texts):
            return issuer[0], issuer[1], status[0]
    return None


def join_field(ws, pitch):
    s, prev = '', None
    for left, right, text in ws:
        if prev is not None:
            s += ' ' * max(1, round((left - prev) / pitch))
        s += text
        prev = right
    return s


def split_cusip(ws, name_x, pitch):
    """Pull CUSIP + options flag out of the words left of the name column.

    The CUSIP is printed as 'XXXXXX XX X' in character columns 0-10; anything inked
    past column ~11.5 is the options flag, whatever glyph OCR thought it was.
    Returns (cusip, flag, raw) or None when the line isn't a data row."""
    left = [w for w in ws if w[0] < name_x - pitch * 0.6]
    while left and not set(left[0][2]) & ALNUM:  # leading specks
        left.pop(0)
    if not left:
        return None
    raw = ' '.join(w[2] for w in left)
    # page header ("RUN DATE: 09/22/99", "Run Time: ..."); no CUSIP starts with R
    if raw.startswith('R') and ':' in raw or raw.count('/') >= 2:
        return None
    start = left[0][0]
    flag = ' '
    toks = [list(w) for w in left]
    if toks[-1][1] > start + 11.8 * pitch:
        flag = '*'
        if toks[-1][0] >= start + 11.3 * pitch or len(toks[-1][2]) == 1:
            toks.pop()
        else:  # flag glued onto the check digit
            toks[-1][2] = toks[-1][2][:-1]
    pieces = [''.join(c for c in t[2] if c in ALNUM) for t in toks]
    pieces = [p for p in pieces if p]
    chars = ''.join(pieces)
    if not 7 <= len(chars) <= 13 or sum(c.isdigit() or c == 'O' for c in chars) < 4:
        return None  # real CUSIPs have 5+ digits; this rejects 'CUSIP NO.' / 'RUN TIME' lines
    return normalize(chars), flag, raw, pieces


def page_pitch(lines, name_x):
    """Character advance in pixels: the issue code starts 7 columns after the issuer code."""
    steps = [(ws[1][0] - ws[0][0]) / 7 for ws in lines
             if len(ws) >= 3 and len(ws[0][2]) == 6 and len(ws[1][2]) == 2 and ws[1][0] < name_x]
    return statistics.median(steps) if len(steps) >= 5 else None


def page_start(lines):
    """x of the CUSIP column on this page: where 'XXXXXX XX' lines begin."""
    xs = [ws[0][0] for ws in lines
          if len(ws) >= 3 and len(ws[0][2]) == 6 and len(ws[1][2]) == 2]
    return statistics.median(xs) if len(xs) >= 5 else None


def column_samples(lines, start, pitch):
    """x-offsets (from the CUSIP column) where name / description / status words start."""
    name_xs, desc_xs, status_xs = [], [], []
    for ws in lines:
        if abs(ws[0][0] - start) > pitch or len(ws[0][2]) != 6:
            continue
        after = [w for w in ws if w[0] - start > 11.5 * pitch and len(w[2]) > 1]
        if after:
            name_xs.append(after[0][0] - start)
        for w in ws:
            if w[2] in ('CALL', 'PUT', 'COM', 'ORD'):
                desc_xs.append(w[0] - start)
            elif w[2] in ('ADDED', 'DELETED'):
                status_xs.append(w[0] - start)
    return name_xs, desc_xs, status_xs


def name_column(name_xs, pitch):
    bucket = Counter(round(x / (pitch / 2)) for x in name_xs).most_common(1)[0][0]
    return statistics.median(x for x in name_xs if abs(x / (pitch / 2) - bucket) <= 1)


def parse_words(pages):
    """Column positions are learned from the data, not the printed header (which is
    misaligned in some years), as x-offsets from the CUSIP column. Scans drift sideways
    and vary a few percent in scale between pages, so each page uses its own samples
    when it has enough of them, falling back to the file-wide values."""
    pages = [(page, lines, page_pitch(lines, float('inf')), page_start(lines)) for page, lines in pages]
    pitches = [p for _, _, p, _ in pages if p]
    if not pitches:
        return []
    pitch = statistics.median(pitches)  # per-page estimate is coarse (integer pixels / 7)
    per_page, all_n, all_d, all_s, header = {}, [], [], [], []
    for page, lines, _, start in pages:
        if start is None:
            continue
        n, d, st = column_samples(lines, start, pitch)
        per_page[page] = (n, d, st)
        all_n += n
        all_d += d
        all_s += st
        cols = find_columns(lines)
        if cols:
            header.append(cols[2] - cols[1])
    if not all_n or not all_d:
        return []
    f_name = name_column(all_n, pitch)
    f_desc = statistics.median(x for x in all_d if x > f_name + 3 * pitch)
    f_gap = (statistics.median(all_s) - f_desc if len(all_s) >= 5
             else statistics.median(header) if header else 19 * pitch)

    rows = []
    for page, lines, _, start in pages:
        if start is None:
            continue
        n, d, _ = per_page[page]
        name_off = name_column(n, pitch) if len(n) >= 10 else f_name
        if abs(name_off - f_name) > 2 * pitch:
            name_off = f_name
        d = [x for x in d if abs(x - f_desc) < 4 * pitch]
        desc_off = statistics.median(d) if len(d) >= 3 else f_desc
        scale = (desc_off - name_off) / (f_desc - f_name)
        status_off = desc_off + f_gap * scale
        tol = pitch * 0.6
        for ws in lines:
            # drop margin specks / page numbers, and punctuation-only specks
            ws = [w for w in ws if w[0] > start - 1.5 * pitch and not re.fullmatch(r"[.,:'-]+", w[2])]
            if not ws or ws[0][0] > start + name_off - pitch * 4:
                continue
            # anchor on this line's own CUSIP when it is where expected: absorbs page skew
            x0 = ws[0][0] if abs(ws[0][0] - start) < pitch and len(ws[0][2]) >= 5 else start
            name_x, desc_x, status_x = x0 + name_off, x0 + desc_off, x0 + status_off
            got = split_cusip(ws, name_x, pitch)
            if not got:
                continue
            cusip, flag, raw, pieces = got
            rest = [w for w in ws if w[0] >= name_x - tol]
            # names end a column before the description, so a multi-letter word starting
            # within 1.5 columns of it is description text with a sloppy OCR box
            in_desc = [w[0] >= desc_x - (1.5 * pitch if len(w[2]) > 1 else tol) for w in rest]
            name = join_field([w for w, d in zip(rest, in_desc) if not d], pitch)
            desc = join_field([w for w, d in zip(rest, in_desc) if d and w[0] < status_x - tol], pitch)
            status = ' '.join(w[2] for w in rest if w[0] >= status_x - tol)
            if not name:
                continue
            rows.append({'page': page, 'cusip': cusip, 'flag': flag, 'raw': raw, 'pieces': pieces,
                         'name': name.strip(), 'desc': desc.strip(), 'status': status.strip()})
    return rows


# confusions resolvable from position alone: no CUSIP uses the letter O, and positions
# 2-5 (issuer digits) and 9 (check digit) are virtually always numeric
TO_DIGIT = str.maketrans('OQDUILTZSBGA', '000011722864')


def normalize(c):
    c = c.replace('O', '0')
    if len(c) == 9:
        c = c[0] + c[1:5].translate(TO_DIGIT) + c[5:8] + c[8].translate(TO_DIGIT)
    return c


def piece_variants(p, n, digit_pos):
    """Readings of one printed CUSIP piece (issuer 6 / issue 2 / check 1) as n chars,
    with the number of edits each needed."""
    def norm(x):
        x = x.replace('O', '0')
        return ''.join(ch.translate(TO_DIGIT) if i in digit_pos else ch for i, ch in enumerate(x))
    if len(p) == n:
        return {norm(p): 0}
    if len(p) == n + 1:  # one spurious character
        return {norm(p[:i] + p[i + 1:]): 1 for i in range(len(p))}
    if len(p) == n - 1:  # one character lost
        out = {}
        for i in range(n):
            for x in sorted(ALNUM - {'O'}):
                out.setdefault(norm(p[:i] + x + p[i:]), 1)
        return out
    return {}


# ---------------------------------------------------------------- repair stage

def valid(c):
    return len(c) == 9 and set(c) <= ALNUM and c[8].isdigit() and cusip_check_digit(c[:8]) == c[8]


def is_option(row):
    d = re.sub(r'[^A-Z]', '', row['desc'])
    return d in ('CALL', 'PUT', 'CAL', 'PU', 'CALLL') or (d.startswith('CALL') and len(d) <= 5)


def substitutions(c, depth):
    """Strings reachable from c by up to `depth` confusable-character substitutions."""
    frontier = {c}
    seen = {c}
    for _ in range(depth):
        nxt = set()
        for s in frontier:
            for i, ch in enumerate(s):
                for alt in CONFUSE.get(ch, '') + ('0123456789' if i == 8 else ''):
                    cand = s[:i] + alt + s[i + 1:]
                    if cand not in seen:
                        seen.add(cand)
                        nxt.add(cand)
        frontier = nxt
    return seen


class KnownIndex:
    """Attested CUSIPs, searchable by Hamming distance <= 2 (pigeonhole on 3-char blocks)."""

    def __init__(self, cusips):
        self.set = set(cusips)
        self.blocks = defaultdict(set)
        for c in self.set:
            for b in range(3):
                self.blocks[b, c[3 * b:3 * b + 3]].add(c)

    def near(self, s, maxdist=2):
        """Attested CUSIPs within maxdist substitutions of s -> distance."""
        hits = {}
        for b in range(3):
            for c in self.blocks.get((b, s[3 * b:3 * b + 3]), ()):
                d = sum(x != y for x, y in zip(s, c))
                if d <= maxdist:
                    hits[c] = d
        return hits


def readings(row):
    """9-char readings of the OCR'd CUSIP -> edits (insert/delete) needed.

    When OCR kept the printed 'XXXXXX XX X' grouping, length fixes are confined to the
    piece whose length is wrong; otherwise the whole string is treated as one piece."""
    pieces = row['pieces']
    if len(pieces) == 3:
        spec = [(pieces[0], 6, {1, 2, 3, 4}), (pieces[1], 2, set()), (pieces[2], 1, {0})]
    elif len(pieces) == 2 and len(pieces[0]) == 6 and len(pieces[1]) == 2:
        spec = [(pieces[0], 6, {1, 2, 3, 4}), (pieces[1], 2, set()), ('', 1, {0})]
    else:
        spec = [(''.join(pieces), 9, {1, 2, 3, 4, 8})]
    combos = {'': 0}
    for p, n, dpos in spec:
        combos = {a + b: ca + cb for a, ca in combos.items() for b, cb in piece_variants(p, n, dpos).items()}
    return {k: v for k, v in combos.items() if v <= 1}


def name_key(name):
    return re.sub(r'[^A-Z0-9]', '', name)


class NameIndex:
    """Issuer name -> CUSIPs seen under that name in other lists (fuzzy within a prefix bucket)."""

    def __init__(self, pairs):
        self.by_name = defaultdict(set)
        for cusip, name in pairs:
            if name_key(name):
                self.by_name[name_key(name)].add(cusip)
        self.buckets = defaultdict(list)
        for k in self.by_name:
            self.buckets[k[:3]].append(k)

    def cusips(self, name):
        k = name_key(name)
        if k in self.by_name:
            return self.by_name[k]
        close = difflib.get_close_matches(k, self.buckets.get(k[:3], []), n=3, cutoff=0.9)
        return set().union(*(self.by_name[c] for c in close)) if close else set()


def repair(rows, known, names):
    """Fix CUSIPs that fail the check digit.

    'repaired': an attested CUSIP (a 2004+ list, two or more scanned quarters, or
                repeated in this list) is within 2 edits of the OCR reading and is
                strictly closer than any other attested one.
    'name_matched': otherwise, the issuer name (read far more reliably than the digits
                on degraded scans) is listed elsewhere with exactly one CUSIP closest to
                the OCR reading, differing in at most 4 characters.
    'guessed':  nothing attested, but exactly one valid CUSIP is reachable by a single
                OCR-confusable substitution, or by supplying a missing check digit."""
    local = Counter(r['cusip'] for r in rows if valid(r['cusip']))
    index = KnownIndex(known | {c for c, n in local.items() if n > 1})
    for r in rows:
        r['fixed_from'] = r['fix'] = ''
        c = r['cusip']
        if is_option(r) or valid(c):
            continue  # option rows are dropped from the output; their CUSIPs aren't real
        reads = readings(r)
        attested = {}
        for s, cost in reads.items():
            for x, d in index.near(s, 2 - cost).items():
                attested[x] = min(attested.get(x, 9), cost + d)
        best = min(attested.values(), default=None)
        closest = [x for x, d in attested.items() if d == best]
        guesses = set()
        for s, cost in reads.items():
            if cost == 0:
                guesses |= {x for x in substitutions(s, 1) if valid(x)}
            elif len(c) == 8 and s[:8] == c:  # only the check digit was lost
                guesses |= {s} if valid(s) else set()
        by_name = {}
        for x in names.cusips(r['name']):
            for s, cost in reads.items():
                d = cost + sum(a != b for a, b in zip(s, x))
                if d <= 4:
                    by_name[x] = min(by_name.get(x, 9), d)
        nbest = min(by_name.values(), default=None)
        nclosest = [x for x, d in by_name.items() if d == nbest]
        if len(closest) == 1:
            r['fixed_from'], r['cusip'], r['fix'] = c, closest[0], 'cusip_repaired'
        elif len(nclosest) == 1:
            r['fixed_from'], r['cusip'], r['fix'] = c, nclosest[0], 'cusip_name_matched'
        elif not attested and len(guesses) == 1:
            r['fixed_from'], r['cusip'], r['fix'] = c, guesses.pop(), 'cusip_guessed'


def read_status(text):
    """Map the OCR'd status column to *A* / *D*; returns None if unreadable."""
    letters = re.sub(r'[^A-Z]', '', text)
    if len(letters) < 3:
        return ''  # stray specks
    best = max(STATUS_WORDS, key=lambda w: difflib.SequenceMatcher(None, letters, w).ratio())
    if difflib.SequenceMatcher(None, letters, best).ratio() >= 0.6:
        return STATUS_WORDS[best]
    return None


# ---------------------------------------------------------------- output

def ocr_pdfs_to_txt(pdfs, outdir, cache, known_txts, jobs, issues_path):
    """OCR scanned PDFs into fixed-width TXT in outdir; known_txts are lists whose CUSIPs count as attested
    when repairing OCR errors. Repaired and still invalid rows are logged to issues_path."""
    run_ocr(pdfs, cache, jobs)

    parsed = {pdf: parse_best(pdf, cache, jobs) for pdf in pdfs}
    known, pairs = set(), []
    # how each CUSIP's name/description is spelled elsewhere, to restore word spacing that
    # OCR lost in proportional-font scans (2004+ lists first, then grid-read scans)
    spelled = defaultdict(Counter)
    for t in known_txts:
        for line in t.read_text().splitlines():
            known.add(line[:9])
            pairs.append((line[:9], line[10:40]))
            spelled[line[:9], 'name'][line[10:40].strip()] += 1000
            spelled[line[:9], 'desc'][line[40:67].strip()] += 1000
    for rows in parsed.values():
        for r in rows:
            if r.get('grid') and valid(r['cusip']):
                spelled[r['cusip'], 'name'][r['name']] += 1
                spelled[r['cusip'], 'desc'][r['desc']] += 1
    pairs += [(r['cusip'], r['name']) for rows in parsed.values() for r in rows
              if valid(r['cusip']) and not is_option(r)]
    names = NameIndex(pairs)
    # CUSIPs read validly in two or more scanned quarters are attested too
    seen = Counter(c for rows in parsed.values() for c in {r['cusip'] for r in rows if valid(r['cusip'])})
    known |= {c for c, n in seen.items() if n >= 2}

    outdir.mkdir(parents=True, exist_ok=True)
    issues_path.parent.mkdir(parents=True, exist_ok=True)
    with open(issues_path, 'w', newline='') as ih:
        iw = csv.writer(ih, delimiter='\t')
        iw.writerow(['file', 'page', 'problem', 'cusip_ocr', 'cusip_written', 'name', 'desc', 'status'])
        for pdf, rows in parsed.items():
            repair(rows, known, names)
            out_lines, n_opt, n_bad, fixes = [], 0, 0, Counter()
            for r in rows:
                if r['fixed_from']:
                    fixes[r['fix']] += 1
                    iw.writerow([pdf.name, r['page'], r['fix'], r['fixed_from'], r['cusip'],
                                 r['name'], r['desc'], r['status']])
                if is_option(r):
                    n_opt += 1
                    continue
                problems = []
                if not valid(r['cusip']):
                    problems.append('invalid_cusip')
                status = read_status(r['status'])
                if status is None:
                    problems.append('unreadable_status')
                    status = ''
                for field in ('name', 'desc'):
                    same = [v for v, _ in spelled[r['cusip'], field].most_common()
                            if name_key(v) == name_key(r[field])]
                    if same and same[0] != r[field]:
                        r[field] = same[0]
                        fixes['respaced'] += 1
                name, desc = r['name'][:30], r['desc'][:27]
                if len(r['name']) > 30 or len(r['desc']) > 27:
                    problems.append('field_too_long')
                cusip = (r['cusip'] + '?' * 9)[:9]  # keep fixed width even if OCR lost characters
                if problems:
                    n_bad += 1
                    iw.writerow([pdf.name, r['page'], ','.join(problems), r['raw'], cusip,
                                 r['name'], r['desc'], r['status']])
                out_lines.append(format_record(cusip, r['flag'], name, desc, status))
            out = outdir / (pdf.stem + '.txt')
            out.write_text(''.join(line + '\n' for line in out_lines))
            print(f'{pdf.name}: {len(rows)} rows, {n_opt} CALL/PUT dropped, {len(out_lines)} written, '
                  f'{fixes["cusip_repaired"]} CUSIPs repaired, {fixes["cusip_name_matched"]} name-matched, '
                  f'{fixes["cusip_guessed"]} guessed, {fixes["respaced"]} fields respaced, {n_bad} rows flagged')
    print(f'issues log: {issues_path}')


# =====================================================================
# 3. Fixed-width TXT -> CSV
# =====================================================================
# Layout per sec.gov staff guidance (see the module docstring); position 80 is an extra
# one-letter flag, not documented, present in later lists.

CSV_FIELDS = ['cusip', 'has_option', 'issuer_name', 'issuer_description', 'status', 'flag']


def parse_line(line):
    line = line.rstrip('\r\n').ljust(80)
    return [
        line[0:9].strip(),
        'Y' if line[9] == '*' else 'N',
        line[10:40].strip(),
        line[40:67].strip(),
        line[67:70].strip('* '),
        line[79:80].strip(),
    ]


def txt_to_csv(indir, outdir):
    """Convert every TXT list in indir to a CSV with CSV_FIELDS in outdir."""
    outdir.mkdir(parents=True, exist_ok=True)

    for txt in sorted(indir.glob('*.txt')):
        out = outdir / (txt.stem + '.csv')
        with open(txt, encoding='latin-1') as f, open(out, 'w', newline='') as g:
            w = csv.writer(g)
            w.writerow(CSV_FIELDS)
            n = 0
            for line in f:
                if line.strip():
                    w.writerow(parse_line(line))
                    n += 1
        print(f'{txt.name} -> {out.name} ({n} rows)')


# =====================================================================
# 4. Consistency of consecutive quarters
# =====================================================================
# Quality proxy for OCR'd TXT lists: consecutive quarters were OCR'd independently and
# issuer names/descriptions rarely change from one quarter to the next, so the share of
# CUSIPs present in both lists whose fields agree approximates the reading accuracy
# (a disagreement means at least one of the two readings is wrong, or the security
# really changed).

def load_txt(path):
    return {line[:9]: line for line in Path(path).read_text().splitlines()}


def letters(s):
    return re.sub(r'[^A-Z0-9&%$/.-]', '', s)


def check_consistency(paths):
    """Print, for each pair of consecutive lists (paths in chronological order), the share of shared CUSIPs
    whose name, name ignoring spaces, description and options flag agree."""
    print(f'{"quarter pair":28} {"shared":>7} {"name":>7} {"name*":>7} {"desc":>7} {"flag":>7}')
    for a, b in zip(paths, paths[1:]):
        x, y = load_txt(a), load_txt(b)
        common = x.keys() & y.keys()
        if not common:
            continue
        agree = lambda f: 100 * sum(f(x[c]) == f(y[c]) for c in common) / len(common)  # noqa: E731
        print(f'{Path(a).stem[7:]} -> {Path(b).stem[7:]:17} {len(common):7d} '
              f'{agree(lambda l: l[10:40]):6.1f}% {agree(lambda l: letters(l[10:40])):6.1f}% '
              f'{agree(lambda l: l[40:67]):6.1f}% {agree(lambda l: l[9]):6.1f}%')
    print('name* = ignoring spaces')
