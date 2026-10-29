"""
Helpers for the pipeline notebooks (notebooks/*.ipynb): execute SQL blocks / the loader step by step,
show the database feedback and collect it in a run log for later analytics.

Run log layout (logs/<pipeline>/<RUN_ID>/):
  steps.jsonl        one JSON line per step, appended immediately (survives a kernel crash)
  NNN_<step>.csv     result set of a step
  loader_output.txt  full stdout of load_holdings.py
"""
import importlib
import io
import json
import os
import re
import subprocess
import sys
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from textwrap import dedent

import pandas as pd
import psycopg2
from IPython.display import Markdown, display

STATUS_ICON = {"ok": "✅", "error": "❌", "interrupted": "⏸️"}
REPORT_COLS = ["seq", "phase", "step", "status", "statusmessage", "rowcount",
               "expected", "diff", "duration_s"]


def _now():
    return datetime.now().isoformat(timespec="seconds")


# =====================================================================
# Run log + SQL execution
# =====================================================================

class PipelineRun:
    """One execution of the notebook: holds the connection and the run log."""

    def __init__(self, log_root, *, dsn=None, max_rows=1000, exact_counts=False):
        self.dsn = dsn
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_root = Path(log_root)
        self.log_dir = self.log_root / self.run_id
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.log_file = self.log_dir / "steps.jsonl"
        self.max_rows = max_rows
        self.exact_counts = exact_counts      # count(*) is slow on ~125M rows; False = pg_class estimate
        self.log: list[dict] = []
        self.conn = None

    def stage(self, phase):
        """Return a Stage that tags every step with `phase`."""
        return Stage(self, phase)

    # --- logging ---------------------------------------------------------

    def record(self, step, phase, *, kind="python", sql=None, status="ok", result=None,
               rowcount=None, statusmessage=None, expected=None, started_at=None,
               duration_s=None, error=None, note=None, show=True):
        """Store one step in the run log and display its feedback."""
        rec = {
            "run_id": self.run_id, "seq": len(self.log) + 1, "phase": phase, "step": step,
            "kind": kind, "status": status, "statusmessage": statusmessage, "rowcount": rowcount,
            "expected": expected,
            "diff": (rowcount - expected) if (rowcount is not None and expected is not None) else None,
            "duration_s": None if duration_s is None else round(duration_s, 3),
            "started_at": started_at or _now(),
            "error": error, "note": note, "sql": sql, "result_file": None,
        }
        if result is not None and not result.empty:
            path = self.log_dir / f"{rec['seq']:03d}_{step}.csv"
            result.to_csv(path, index=False)
            rec["result_file"] = path.name
        self.log.append(rec)
        with open(self.log_file, "a") as f:
            f.write(json.dumps(rec, default=str) + "\n")

        if show:
            self._show(rec, result)
        return rec

    @staticmethod
    def _show(rec, result):
        parts = [f"{STATUS_ICON.get(rec['status'], '•')} **[{rec['phase']}] {rec['step']}**: `{rec['status']}`"]
        if rec["statusmessage"]:
            parts.append(f"`{rec['statusmessage']}`")
        if rec["rowcount"] is not None and rec["rowcount"] >= 0:
            parts.append(f"rows: **{rec['rowcount']:,}**")
        if rec["expected"] is not None:
            diff = f" (diff {rec['diff']:+,})" if rec["diff"] is not None else ""
            parts.append(f"expected: {rec['expected']:,}{diff}")
        if rec["duration_s"] is not None:
            parts.append(f"{rec['duration_s']:,.1f}s")
        display(Markdown(" | ".join(parts)))
        if rec["note"]:
            display(Markdown(f"> {rec['note']}"))
        if rec["error"]:
            print(rec["error"])
        if result is not None and not result.empty:
            display(result)

    # --- SQL ---------------------------------------------------------------

    def run(self, step, sql, *, phase, expected=None, note=None, fetch_all=False):
        """Execute one SQL block, show the database feedback, log it. Returns the result set
        (at most max_rows rows unless fetch_all=True)."""
        self.ensure_connected()
        sql = dedent(sql).strip()
        started = _now()
        result, status, msg, rc, err = pd.DataFrame(), "ok", None, None, None

        t0 = time.perf_counter()
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql)
                msg, rc = cur.statusmessage, cur.rowcount
                if cur.description:
                    cols = [d[0] for d in cur.description]
                    rows = cur.fetchall() if fetch_all else cur.fetchmany(self.max_rows)
                    result = pd.DataFrame(rows, columns=cols)
            if self.conn.notices:
                notices = " ".join(n.strip() for n in self.conn.notices)
                note = ((note + "\n\n") if note else "") + "NOTICES: " + notices
                self.conn.notices.clear()
        except KeyboardInterrupt:
            status, err = "interrupted", "KeyboardInterrupt"
        except Exception as e:
            status, err = "error", f"{type(e).__name__}: {e}"
        dur = time.perf_counter() - t0

        self.record(step, phase, kind="sql", sql=sql, status=status, result=result, rowcount=rc,
                    statusmessage=msg, expected=expected, started_at=started, duration_s=dur,
                    error=err, note=note)
        if status != "ok":
            raise RuntimeError(f"Step '{step}' {status}: {err}")
        return result

    def copy_csv(self, step, table, path, columns, *, phase, expected=None, note=None):
        """COPY a local CSV (with header) into `table`. Header must match `columns` exactly.
        Unquoted empty fields become NULL."""
        self.ensure_connected()
        cols = ", ".join(columns)
        sql = f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT csv, HEADER MATCH)"
        started = _now()
        status, msg, rc, err = "ok", None, None, None

        t0 = time.perf_counter()
        try:
            with open(path, newline="", encoding="utf-8") as f, self.conn.cursor() as cur:
                cur.copy_expert(sql, f)
                rc = cur.rowcount
                msg = cur.statusmessage or f"COPY {rc}"
        except KeyboardInterrupt:
            status, err = "interrupted", "KeyboardInterrupt"
        except Exception as e:
            status, err = "error", f"{type(e).__name__}: {e}"
        dur = time.perf_counter() - t0

        self.record(step, phase, kind="copy", sql=f"{sql}  -- from {path}", status=status,
                    rowcount=rc, statusmessage=msg, expected=expected, started_at=started,
                    duration_s=dur, error=err, note=note)
        if status != "ok":
            raise RuntimeError(f"Step '{step}' {status}: {err}")

    def row_estimate(self, table, *, phase, tag):
        """Fast row count from pg_class, or count(*) if exact_counts is set."""
        if self.exact_counts:
            return self.run(f"{tag}_count_exact", f"SELECT count(*) AS n FROM {table};", phase=phase)
        return self.run(f"{tag}_count_estimate",
                        f"SELECT reltuples::bigint AS estimate FROM pg_class WHERE oid = '{table}'::regclass;",
                        phase=phase)

    def table_size(self, table, *, phase, tag):
        return self.run(f"{tag}_size", f"""
            SELECT pg_size_pretty(pg_total_relation_size('{table}')) AS total,
                   pg_size_pretty(pg_relation_size('{table}'))       AS heap,
                   pg_size_pretty(pg_indexes_size('{table}'))        AS indexes;
        """, phase=phase)

    # --- connection --------------------------------------------------------

    def check_connection(self, dsn, *, schemas, tables):
        """Read-only connection check. Nothing is committed: READ ONLY transaction + rollback.
        Returns True if the pipeline can run."""
        phase = "connection"
        started = _now()
        t0 = time.perf_counter()
        probe = psycopg2.connect(dsn, connect_timeout=10)
        try:
            probe.set_session(readonly=True, autocommit=False)
            with probe.cursor() as cur:
                server = _query(cur, """
                    SELECT version()                                            AS server_version,
                           current_database()                                   AS database,
                           current_user                                         AS db_user,
                           pg_is_in_recovery()                                  AS is_standby,
                           current_setting('transaction_read_only')             AS txn_read_only,
                           pg_size_pretty(pg_database_size(current_database())) AS db_size,
                           (SELECT count(*) FROM pg_stat_activity
                             WHERE datname = current_database()
                               AND pid <> pg_backend_pid())                     AS other_sessions
                """)
                schema_df = _query(cur, """
                    SELECT s.schema_name,
                           n.oid IS NOT NULL                                    AS schema_exists,
                           CASE WHEN n.oid IS NULL THEN false
                                ELSE has_schema_privilege(n.oid, 'CREATE') END  AS can_create,
                           CASE WHEN n.oid IS NULL THEN false
                                ELSE has_schema_privilege(n.oid, 'USAGE') END   AS can_use
                    FROM unnest(%s::text[]) s(schema_name)
                    LEFT JOIN pg_namespace n ON n.nspname = s.schema_name
                """, (list(schemas),))
                table_df = _query(cur, """
                    SELECT t.tbl                                             AS table_name,
                           to_regclass(t.tbl) IS NOT NULL                    AS exists,
                           c.relpersistence = 'u'                            AS unlogged,
                           c.reltuples::bigint                               AS est_rows,
                           pg_size_pretty(pg_total_relation_size(c.oid))     AS total_size
                    FROM unnest(%s::text[]) t(tbl)
                    LEFT JOIN pg_class c ON c.oid = to_regclass(t.tbl)
                """, (list(tables),))
        finally:
            probe.rollback()
            probe.close()

        problems = []
        if server.at[0, "is_standby"]:
            problems.append("server is a read-only standby")
        missing = schema_df.loc[~schema_df["schema_exists"], "schema_name"].tolist()
        if missing:
            problems.append(f"missing schemas: {missing}")
        no_create = schema_df.loc[schema_df["schema_exists"] & ~schema_df["can_create"], "schema_name"].tolist()
        if no_create:
            problems.append(f"no CREATE privilege on: {no_create}")

        self.record("connection_check_server", phase, status="error" if problems else "ok",
                    result=server, started_at=started, duration_s=time.perf_counter() - t0,
                    error="; ".join(problems) or None,
                    note="READ ONLY transaction, rolled back. Nothing committed.")
        self.record("connection_check_schemas", phase, result=schema_df)
        self.record("connection_check_tables", phase, result=table_df,
                    note="Existing tables will be dropped and recreated by the pipeline.")
        return not problems

    def connect(self, dsn=None):
        """Working connection. autocommit: SET LOGGED / CREATE INDEX / ANALYZE, commit per block."""
        self.dsn = dsn or self.dsn
        if self.dsn is None:
            raise RuntimeError("No DSN: pass it to PipelineRun(...) or connect(dsn)")
        self.conn = psycopg2.connect(self.dsn)
        self.conn.autocommit = True
        print("Connected:", self.conn.dsn)   # psycopg2 masks the password

    def ensure_connected(self):
        """Reconnect if there is no open connection (config cell re-run, kernel restart, dropped connection)."""
        if self.conn is None or self.conn.closed:
            self.connect()
        return self.conn

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None
        print("Run log:", self.log_file)


class Stage:
    """A PipelineRun bound to one phase, so notebook cells don't repeat `phase=`."""

    def __init__(self, pipeline, phase):
        self.p, self.phase = pipeline, phase

    def run(self, step, sql, **kw):
        return self.p.run(step, sql, phase=self.phase, **kw)

    def record(self, step, **kw):
        return self.p.record(step, self.phase, **kw)

    def copy_csv(self, step, table, path, columns, **kw):
        return self.p.copy_csv(step, table, path, columns, phase=self.phase, **kw)

    def row_estimate(self, table, tag):
        return self.p.row_estimate(table, phase=self.phase, tag=tag)

    def table_size(self, table, tag):
        return self.p.table_size(table, phase=self.phase, tag=tag)


def _query(cur, sql, params=None):
    cur.execute(sql, params)
    return pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])


# =====================================================================
# load_holdings.py
# =====================================================================

class Loader:
    """Runs src/load_holdings.py as a subprocess and logs its feedback."""

    def __init__(self, stage, script_dir):
        self.stage = stage
        self.script_dir = Path(script_dir)
        self.script = self.script_dir / "load_holdings.py"
        assert self.script.exists(), f"Loader not found: {self.script}"

        # Use the loader's own paths so PAUSE/tracking files match exactly.
        # reload: the kernel would otherwise keep paths from an older version of the file.
        if str(self.script_dir) not in sys.path:
            sys.path.insert(0, str(self.script_dir))
        import load_holdings as lh
        lh = importlib.reload(lh)
        self.tracking_file = Path(lh.TRACKING_FILE)
        self.addresses_file = Path(lh.ADDRESSES_FILE)
        self.pause_file = Path(lh.PAUSE_FILE)
        self.preflight_ok = False

    def preflight(self):
        """Check that the loader's hardcoded paths and a sample of the CSV addresses exist."""
        rows = [{"check": "loader BASE_DIR", "path": str(self.pause_file.parent), "exists": self.pause_file.parent.is_dir()},
                {"check": "addresses file", "path": str(self.addresses_file), "exists": self.addresses_file.exists()},
                {"check": "tracking file", "path": str(self.tracking_file), "exists": self.tracking_file.exists()}]
        if self.addresses_file.exists():
            sample = pd.read_csv(self.addresses_file, nrows=5)["address"]
            rows += [{"check": "csv sample", "path": p, "exists": Path(p).exists()} for p in sample]
        res = pd.DataFrame(rows)
        # the tracking file may legitimately be missing (fresh load)
        self.preflight_ok = bool(res.loc[res["check"] != "tracking file", "exists"].all())
        self.stage.record("preflight_paths", status="ok" if self.preflight_ok else "error", result=res,
                          error=None if self.preflight_ok else
                          "Paths used by load_holdings.py do not exist; fix BASE_DIR / file_adresses.csv")
        return self.preflight_ok

    def tracking_summary(self, tag):
        """Log status counts and the most frequent errors from tracking.csv."""
        if not self.tracking_file.exists():
            self.stage.record(f"{tag}_tracking_status",
                              note=f"No tracking file at {self.tracking_file}. "
                                   "Fresh load: load_holdings.py creates it from file_adresses.csv when it starts.")
            return
        tr = pd.read_csv(self.tracking_file, usecols=["status", "error"])
        counts = tr["status"].value_counts().rename_axis("status").reset_index(name="files")
        self.stage.record(f"{tag}_tracking_status", result=counts, rowcount=len(tr))
        errs = tr.loc[tr["status"] == "error", "error"].fillna("").str.slice(0, 120)
        if not errs.empty:
            top = errs.value_counts().head(20).rename_axis("error").reset_index(name="files")
            self.stage.record(f"{tag}_tracking_errors", result=top, rowcount=int(errs.size))

    def reset_tracking(self):
        """Move tracking.csv into the run log so the loader reloads ALL files."""
        if self.tracking_file.exists():
            archived = self.stage.p.log_dir / "tracking_before_reset.csv"
            self.tracking_file.replace(archived)
            self.stage.record("reset_tracking", note=f"tracking.csv moved to {archived.name}")

    def run(self):
        """Run the loader. Interrupt the kernel to pause it gracefully (creates the PAUSE file)."""
        if not self.preflight_ok:
            raise RuntimeError("Preflight failed, loader not started (see preflight_paths above)")

        started = _now()
        tail = deque(maxlen=40)
        status, err = "ok", None
        t0 = time.perf_counter()
        with open(self.stage.p.log_dir / "loader_output.txt", "a") as out:
            # text mode turns tqdm's '\r' into line breaks, so progress arrives line by line
            proc = subprocess.Popen([sys.executable, "-u", str(self.script)], cwd=self.script_dir,
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)

            def pump():
                for line in proc.stdout:
                    out.write(line)
                    line = line.rstrip("\n")
                    if not line.strip():
                        continue
                    if "it/s]" in line or "s/it]" in line:      # tqdm progress: overwrite one line
                        print("\r" + line, end="", flush=True)
                    else:
                        print("\n" + line, flush=True)
                        tail.append(line)

            try:
                pump()
            except KeyboardInterrupt:
                self.pause_file.touch()
                status = "interrupted"
                print("\n[PAUSE] Pause file created, waiting for running batches to finish...")
                pump()                                        # drain until the loader exits
            rc = proc.wait()

        if rc != 0 and status == "ok":
            status, err = "error", f"loader exit code {rc}"
        self.stage.record("load_holdings_py", kind="subprocess", status=status, started_at=started,
                          duration_s=time.perf_counter() - t0, error=err,
                          note="Loader summary:\n\n```\n" + "\n".join(tail) + "\n```")


# =====================================================================
# SEC 13(f) securities lists (13flistYYYYqN.csv)
# =====================================================================

class F13ListLoader:
    """Loads the quarterly SEC 13(f) list CSVs into the securities table.

    Same rules as f13_securities/load_13f_securities.py: year/quarter from the file name,
    has_option Y/N -> boolean, blank fields -> NULL, rows without a CUSIP skipped, and a
    re-run replaces a file's rows (DELETE by source_file) instead of duplicating them.
    Each file is loaded in its own transaction; one logged step summarises all files."""

    FILE_RE = re.compile(r"(\d{4})q([1-4])", re.IGNORECASE)
    CSV_COLUMNS = ["cusip", "has_option", "issuer_name", "issuer_description", "status", "flag"]

    def __init__(self, stage, csv_dir, table, encoding="LATIN1"):
        self.stage = stage
        self.csv_dir = Path(csv_dir)
        self.table = table
        self.encoding = encoding

    def files(self):
        return sorted(self.csv_dir.glob("*.csv"))

    def preflight(self):
        """List the files, their quarter and the quarters missing between the first and last one."""
        rows = []
        for f in self.files():
            m = self.FILE_RE.search(f.name)
            rows.append({"file": f.name, "year": int(m.group(1)) if m else None,
                         "quarter": int(m.group(2)) if m else None, "kb": round(f.stat().st_size / 1024)})
        df = pd.DataFrame(rows, columns=["file", "year", "quarter", "kb"])
        problems = []
        if not self.csv_dir.is_dir():
            problems.append(f"directory not found: {self.csv_dir}")
        elif df.empty:
            problems.append(f"no CSV files in {self.csv_dir}")
        bad = df.loc[df["year"].isna(), "file"].tolist()
        if bad:
            problems.append(f"no year/quarter in file name: {bad}")
        missing = []
        ok = df.dropna(subset=["year"])
        if not ok.empty:
            have = set(zip(ok["year"].astype(int), ok["quarter"].astype(int)))
            y0, y1 = int(ok["year"].min()), int(ok["year"].max())
            missing = [f"{y}q{q}" for y in range(y0, y1 + 1) for q in range(1, 5)
                       if (y, q) not in have and (y, q) >= min(have) and (y, q) <= max(have)]
        self.stage.record("preflight_csv_files", status="error" if problems else "ok", result=df,
                          rowcount=len(df), error="; ".join(problems) or None,
                          note=f"{self.csv_dir}  |  missing quarters: {', '.join(missing) or 'none'}")
        return not problems

    def run(self):
        """Load every file. Returns the per-file result (rows in CSV, rows replaced, rows inserted)."""
        conn = self.stage.p.ensure_connected()
        started = _now()
        rows, errors, status = [], [], "ok"
        t0 = time.perf_counter()
        conn.autocommit = False
        try:
            for f in self.files():
                m = self.FILE_RE.search(f.name)
                if not m:
                    continue
                year, quarter = int(m.group(1)), int(m.group(2))
                tf = time.perf_counter()
                try:
                    with conn.cursor() as cur, open(f, "rb") as fh:
                        cur.execute(f"CREATE TEMP TABLE f13_stage ({', '.join(c + ' text' for c in self.CSV_COLUMNS)}) "
                                    "ON COMMIT DROP")
                        cur.copy_expert("COPY f13_stage FROM STDIN WITH "
                                        f"(FORMAT csv, HEADER MATCH, ENCODING '{self.encoding}')", fh)
                        in_csv = cur.rowcount
                        cur.execute(f"DELETE FROM {self.table} WHERE source_file = %s", (f.name,))
                        replaced = cur.rowcount
                        cur.execute(f"""
                            INSERT INTO {self.table}
                                (year, quarter, cusip, has_option, issuer_name,
                                 issuer_description, status, flag, source_file)
                            SELECT %s, %s, TRIM(cusip),
                                   CASE UPPER(TRIM(has_option)) WHEN 'Y' THEN true WHEN 'N' THEN false END,
                                   NULLIF(TRIM(issuer_name), ''), NULLIF(TRIM(issuer_description), ''),
                                   NULLIF(TRIM(status), ''), NULLIF(TRIM(flag), ''), %s
                            FROM f13_stage
                            WHERE NULLIF(TRIM(cusip), '') IS NOT NULL""", (year, quarter, f.name))
                        inserted = cur.rowcount
                    conn.commit()
                    rows.append({"file": f.name, "year": year, "quarter": quarter, "in_csv": in_csv,
                                 "replaced": replaced, "inserted": inserted,
                                 "seconds": round(time.perf_counter() - tf, 2), "error": None})
                except Exception as e:
                    conn.rollback()
                    msg = f"{type(e).__name__}: {str(e).strip().splitlines()[0]}"
                    errors.append(f"{f.name}: {msg}")
                    rows.append({"file": f.name, "year": year, "quarter": quarter, "in_csv": None,
                                 "replaced": None, "inserted": None,
                                 "seconds": round(time.perf_counter() - tf, 2), "error": msg})
        except KeyboardInterrupt:
            conn.rollback()
            status = "interrupted"
        finally:
            conn.autocommit = True

        df = pd.DataFrame(rows)
        if errors and status == "ok":
            status = "error"
        total = int(df["inserted"].fillna(0).sum()) if not df.empty else 0
        self.stage.record("load_csv_files", kind="copy", status=status, result=df, rowcount=total,
                          started_at=started, duration_s=time.perf_counter() - t0,
                          error="; ".join(errors[:10]) or None,
                          note=f"{len(df)} files | skipped rows without CUSIP: "
                               f"{int((df['in_csv'].fillna(0) - df['inserted'].fillna(0)).sum()) if not df.empty else 0}")
        if status != "ok":
            raise RuntimeError(f"load_csv_files {status}: {len(errors)} file(s) failed, see the result table")
        return df


# =====================================================================
# EDGAR submissions (CIK##########.json) -> edgar_company table
# =====================================================================

F13_FORMS = ("13F-HR", "13F-HR/A", "13F-NT", "13F-NT/A")


def _pg_array(values):
    """Python list -> PostgreSQL array literal for COPY (None -> NULL)."""
    def item(v):
        if v is None:
            return "NULL"
        return '"' + str(v).replace("\\", "\\\\").replace('"', '\\"') + '"'
    return "{" + ",".join(item(v) for v in values) + "}"


def _submission_record(task):
    """Worker for EdgarSubmissionLoader (module level so ProcessPoolExecutor can pickle it).

    task = (main file, overflow files, force). Returns None if the CIK filed no 13F form, has no SIC code
    and no ticker and is not forced, otherwise one row for the submissions table (or {"error": ...})."""
    path, overflow, force = task
    try:
        with open(path, "rb") as f:
            raw = f.read()
        blobs = []
        for o in overflow:
            with open(o, "rb") as f:
                blobs.append(f.read())
        # cheap byte tests first: most of the ~975k filers have no 13F form, SIC or ticker
        head = raw[:3000]
        has_13f = b'"13F-' in raw or any(b'"13F-' in b for b in blobs)
        no_sic = b'"sic":""' in head or b'"sic":"0000"' in head
        no_ticker = b'"tickers":[]' in head
        if not (force or has_13f) and no_sic and no_ticker:
            return None

        d = json.loads(raw)
        counts = dict.fromkeys(F13_FORMS, 0)
        filed, reported, total = [], [], 0
        for block in [d.get("filings", {}).get("recent", {})] + [json.loads(b) for b in blobs]:
            forms = block.get("form", [])
            total += len(forms)
            for form, fdate, rdate in zip(forms, block.get("filingDate", []), block.get("reportDate", [])):
                if form in counts:
                    counts[form] += 1
                    if fdate:
                        filed.append(fdate)
                    if rdate and form.startswith("13F-HR"):   # notices have no holdings to report
                        reported.append(rdate)
        sic = d.get("sic") if d.get("sic") not in ("", "0000") else None
        tickers = d.get("tickers") or []
        if not (force or any(counts.values()) or sic or tickers):
            return None

        listed = len(d.get("filings", {}).get("files", []))
        biz = (d.get("addresses") or {}).get("business") or {}
        return {
            "cik": int(d["cik"]), "name": d.get("name"), "entity_type": d.get("entityType"),
            "sic": d.get("sic"), "sic_description": d.get("sicDescription"),
            "ein": d.get("ein"), "lei": d.get("lei"), "category": d.get("category"),
            "tickers": _pg_array(t.upper() for t in tickers),
            "exchanges": _pg_array(d.get("exchanges") or []),
            "state_of_incorporation": d.get("stateOfIncorporation"),
            "fiscal_year_end": d.get("fiscalYearEnd"),
            "business_city": biz.get("city"), "business_state": biz.get("stateOrCountry"),
            "business_state_description": biz.get("stateOrCountryDescription"),
            "former_names": json.dumps(d.get("formerNames") or []),
            "n_13f_hr": counts["13F-HR"], "n_13f_hr_a": counts["13F-HR/A"],
            "n_13f_nt": counts["13F-NT"], "n_13f_nt_a": counts["13F-NT/A"],
            "first_13f_filed": min(filed, default=None), "last_13f_filed": max(filed, default=None),
            "first_13f_report": min(reported, default=None), "last_13f_report": max(reported, default=None),
            "filings_total": total, "overflow_files": len(overflow),
            "overflow_missing": max(listed - len(overflow), 0),
            "forced": bool(force), "source_file": os.path.basename(path),
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}", "source_file": os.path.basename(path)}


class EdgarSubmissionLoader:
    """Builds the EDGAR submissions input table from the SEC EDGAR bulk submissions files
    (https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip).

    One main file per filer (CIK##########.json) with the latest ~1000 filings, older filings in overflow
    files (CIK##########-submissions-NNN.json). A CIK is kept if any of its files contains a 13F form
    (13F-HR, 13F-HR/A, 13F-NT, 13F-NT/A; counted from the parsed `form` arrays, not a text search) or if it
    has a SIC code or a ticker (the issuers, for the industry classification). `extra_ciks` (e.g. the CIKs
    of the holdings table) are kept in any case.

    The files are scanned in parallel; the table is replaced in one transaction (TRUNCATE + COPY) only
    after the scan finished, so an interrupted scan leaves the previous load intact."""

    FILE_RE = re.compile(r"^CIK(\d{10})(-submissions-\d+)?\.json$")
    COLUMNS = ["cik", "name", "entity_type", "sic", "sic_description", "ein", "lei", "category",
               "tickers", "exchanges", "state_of_incorporation", "fiscal_year_end", "business_city",
               "business_state", "business_state_description", "former_names", "n_13f_hr", "n_13f_hr_a",
               "n_13f_nt", "n_13f_nt_a", "first_13f_filed", "last_13f_filed", "first_13f_report",
               "last_13f_report", "filings_total", "overflow_files", "overflow_missing", "forced", "source_file"]

    def __init__(self, stage, submissions_dir, table, workers=None):
        self.stage = stage
        self.dir = Path(submissions_dir)
        self.table = table
        self.workers = workers
        self.main, self.overflow, self.other = {}, {}, []

    def _index(self):
        """cik -> main file and cik -> overflow files, from one directory listing."""
        self.main, self.overflow, self.other = {}, {}, []
        for name in os.listdir(self.dir):
            m = self.FILE_RE.match(name)
            if not m:
                self.other.append(name)
            elif m.group(2):
                self.overflow.setdefault(int(m.group(1)), []).append(str(self.dir / name))
            else:
                self.main[int(m.group(1))] = str(self.dir / name)

    def preflight(self, extra_ciks=()):
        """Directory listing: main / overflow / unexpected files, and which extra CIKs have no main file."""
        problems = []
        if not self.dir.is_dir():
            problems.append(f"directory not found: {self.dir}")
        else:
            self._index()
            if not self.main:
                problems.append(f"no CIK##########.json files in {self.dir}")
        missing = sorted(set(extra_ciks) - set(self.main))
        orphans = sorted(set(self.overflow) - set(self.main))
        df = pd.DataFrame([
            {"check": "main files",                       "n": len(self.main)},
            {"check": "overflow files",                   "n": sum(map(len, self.overflow.values()))},
            {"check": "CIKs with overflow files",         "n": len(self.overflow)},
            {"check": "overflow without main file",       "n": len(orphans)},
            {"check": "unexpected file names",            "n": len(self.other)},
            {"check": "extra CIKs",                       "n": len(set(extra_ciks))},
            {"check": "extra CIKs without main file",     "n": len(missing)},
        ])
        self.stage.record("preflight_submissions", status="error" if problems else "ok", result=df,
                          error="; ".join(problems) or None,
                          note=f"{self.dir}  |  extra CIKs without file (first 20): {missing[:20] or 'none'}")
        return not problems

    def scan(self, extra_ciks=()):
        """Parse the files of every 13F filer, SIC / ticker filer and extra CIK. Returns (rows, errors)."""
        from concurrent.futures import ProcessPoolExecutor
        from tqdm.auto import tqdm
        if not self.main:
            self._index()
        extra = set(extra_ciks)
        tasks = [(path, sorted(self.overflow.get(cik, [])), cik in extra) for cik, path in self.main.items()]
        rows, errors = [], []
        with ProcessPoolExecutor(max_workers=self.workers) as ex:
            for r in tqdm(ex.map(_submission_record, tasks, chunksize=500), total=len(tasks),
                          desc="submissions", unit="file"):
                if r is None:
                    continue
                (errors if "error" in r else rows).append(r)
        return rows, errors

    def run(self, extra_ciks=()):
        """Scan, then replace the table contents. Returns the loaded rows as a DataFrame.
        Interrupt the kernel (■) during the scan to stop without touching the table."""
        conn = self.stage.p.ensure_connected()
        started, t0 = _now(), time.perf_counter()
        status, err, df, errors = "ok", None, pd.DataFrame(columns=self.COLUMNS), []
        try:
            rows, errors = self.scan(extra_ciks)
            df = pd.DataFrame(rows, columns=self.COLUMNS).sort_values("cik")
            buf = io.StringIO()
            df.to_csv(buf, index=False)
            buf.seek(0)
            conn.autocommit = False
            try:
                with conn.cursor() as cur:
                    cur.execute(f"TRUNCATE {self.table}")
                    cur.copy_expert(f"COPY {self.table} ({', '.join(self.COLUMNS)}) "
                                    "FROM STDIN WITH (FORMAT csv, HEADER MATCH)", buf)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.autocommit = True
        except KeyboardInterrupt:
            status, err = "interrupted", "KeyboardInterrupt (table not changed)"
        except Exception as e:
            status, err = "error", f"{type(e).__name__}: {e}"

        if errors and status == "ok":
            status, err = "error", f"{len(errors)} file(s) could not be parsed: " + \
                                   "; ".join(f"{e['source_file']}: {e['error']}" for e in errors[:5])
        hr = (df["n_13f_hr"] + df["n_13f_hr_a"]) > 0
        f13 = df[["n_13f_hr", "n_13f_hr_a", "n_13f_nt", "n_13f_nt_a"]].sum(axis=1) > 0
        sic = df["sic"].notna() & ~df["sic"].isin(["", "0000"])
        ticker = df["tickers"] != "{}"
        summary = pd.DataFrame([
            {"group": "13F filers",                       "ciks": int(f13.sum())},
            {"group": "  with 13F-HR or 13F-HR/A",        "ciks": int(hr.sum())},
            {"group": "  notices only (13F-NT)",          "ciks": int((f13 & ~hr).sum())},
            {"group": "filers with SIC or ticker",        "ciks": int((sic | ticker).sum())},
            {"group": "  of them 13F filers",             "ciks": int(((sic | ticker) & f13).sum())},
            {"group": "extra CIKs without 13F / SIC / ticker", "ciks": int((~f13 & ~sic & ~ticker).sum())},
            {"group": "parse errors",                     "ciks": len(errors)},
        ])
        self.stage.record("load_submissions", kind="copy", status=status, result=summary, rowcount=len(df),
                          started_at=started, duration_s=time.perf_counter() - t0, error=err,
                          note=f"{len(self.main):,} main files scanned | {len(set(extra_ciks)):,} extra CIKs")
        if status != "ok":
            raise RuntimeError(f"load_submissions {status}: {err}")
        return df


# =====================================================================
# Issuer -> EDGAR CIK matching (industry pipeline)
# =====================================================================

# Words written out in one source and abbreviated in the other, mapped to one form before matching
ISSUER_ABBR = {
    "CORPORATION": "CORP", "COMPANY": "CO", "COMPANIES": "COS", "INCORPORATED": "INC", "LIMITED": "LTD",
    "HOLDINGS": "HLDGS", "HOLDING": "HLDG", "INTERNATIONAL": "INTL", "INTERNATL": "INTL",
    "MANAGEMENT": "MGMT", "FINANCIAL": "FINL", "SERVICES": "SVCS", "SERVICE": "SVC", "INDUSTRIES": "INDS",
    "INDUSTRY": "INDS", "ASSOCIATES": "ASSOCS", "BANCORPORATION": "BANCORP", "MARKETS": "MKTS",
    "SYSTEMS": "SYS", "SYSTEM": "SYS", "PRODUCTS": "PRODS", "LABORATORIES": "LABS", "PROPERTIES": "PPTYS",
    "REALTY": "RLTY", "RESOURCES": "RES", "NATIONAL": "NATL", "DEVELOPMENT": "DEV", "INVESTMENT": "INVT",
    "INVESTMENTS": "INVTS", "TRUST": "TR", "FUND": "FD", "SECURITIES": "SECS", "MORTGAGE": "MTG",
    "ELECTRIC": "ELEC", "CAPITAL": "CAP", "MANUFACTURING": "MFG", "SAVINGS": "SVGS", "INSURANCE": "INS",
    "COMMUNITY": "CMNTY", "COMMUNITIES": "CMNTYS", "GENERAL": "GEN", "PETROLEUM": "PETE", "BANK": "BK",
    "RAILWAY": "RY", "RAILROAD": "RR", "LOAN": "LN", "ASSOCIATION": "ASSN",
    "AND": "", "&": "", "THE": "", "OF": "",
}
# Legal-form and state / country words dropped from the end of a name
# ("JACOBS ENGR GROUP INC DEL" -> "JACOBS ENGR GROUP", "TORONTO DOMINION BK ONT" -> "TORONTO DOMINION BK")
ISSUER_SUFFIX = {"INC", "CORP", "CO", "LTD", "PLC", "LLC", "LP", "L", "P", "N", "NV", "SA", "AG", "CL", "A", "B",
                 "NEW", "DEL", "DE", "MASS", "ONT", "QUE", "BC", "IRELAND", "CALIF", "MINN", "WIS", "TEX", "MD",
                 "PA", "VA", "NY", "NJ"}


def issuer_name_key(name):
    """Normalised issuer name: upper case, no punctuation or EDGAR state tags (/DE/), single letters joined
    (E M C -> EMC), common words abbreviated, filler words and legal-form / state suffixes removed.
    'Take-Two Interactive Software, Inc.' -> 'TAKE TWO INTERACTIVE SOFTWARE'."""
    s = str(name or "").upper()
    s = re.sub(r"[/\\][A-Z ]{1,6}[/\\]?", " ", s)
    s = re.sub(r"[^A-Z0-9& ]", " ", s.replace(".", " ").replace("'", "").replace("&", " & "))
    # join runs of single letters: "E M C CORP" -> "EMC CORP", "E I DU PONT" -> "EI DU PONT"
    joined = []
    for t in s.split():
        if len(t) == 1 and t.isalpha() and joined and joined[-1][1]:
            joined[-1] = (joined[-1][0] + t, True)
        else:
            joined.append((t, len(t) == 1 and t.isalpha()))
    toks = [t for t in (ISSUER_ABBR.get(t, t) for t, _ in joined) if t]
    while toks and toks[-1] in ISSUER_SUFFIX:
        toks.pop()
    return " ".join(toks)


def _token_match(a, b):
    """Same word, or one is an abbreviation of the other: same first letter and the shorter (>= 3 letters)
    is a prefix or a subsequence of the longer (TECH ~ TECHNOLOGIES, ELECTRS ~ ELECTRONICS, PWR ~ POWER)."""
    if a == b:
        return True
    if len(a) < 3 or len(b) < 3 or a[0] != b[0]:
        return False
    short, long_ = (a, b) if len(a) <= len(b) else (b, a)
    if long_.startswith(short):
        return True
    it = iter(long_)
    return all(c in it for c in short)


def _name_match(issuer, edgar, truncated):
    """Token-wise match of two keys (lists of words). A truncated issuer name (13F lists cut names at
    ~30 characters) may stop early and its last word may be cut. A one-word name must match exactly
    (abbreviation rules on a single word give too many false hits: FERRO ~ FERREXPO)."""
    if len(issuer) == 1 and not truncated:
        return edgar == issuer
    if truncated:
        n = len(issuer)
        if n > len(edgar) or not all(_token_match(a, b) for a, b in zip(issuer[:-1], edgar)):
            return False
        return edgar[n - 1].startswith(issuer[-1]) or _token_match(issuer[-1], edgar[n - 1])
    return len(issuer) == len(edgar) and all(_token_match(a, b) for a, b in zip(issuer, edgar))


class IssuerMatcher:
    """Links each asset to the EDGAR filer (CIK) of its issuer.

    Candidates: EDGAR names (current and former) of filers with a SIC whose key matches the asset's issuer
    name word by word (`_name_match`; blocked on the first 3 letters of the first word). Names of 28+
    characters are treated as truncated. A ticker alone is never enough: EDGAR only has today's tickers, which
    are often reused or belong to an acquirer.

    Exact name matches are kept over abbreviation matches, and filers with a placeholder SIC (8880 ADR
    registrant, 9995 non-operating shell) are dropped when another candidate remains.

    One CIK is picked per asset, first rule that leaves exactly one:
      name+ticker        a candidate also owns the asset's OpenFIGI ticker (root word) in EDGAR
      name               only one candidate
      name+period        one candidate used the matched name while the asset was listed
      name+same_industry all candidates are in the same FF48 industry, or share a SIC FF48 does not list
                         (the one with the latest name is kept)
      ambiguous          candidates in different industries -> no CIK
      unmatched          no candidate"""

    TRUNCATED_LEN = 28
    PLACEHOLDER_SIC = {8880, 9995}

    def __init__(self, stage, asset_table, company_table, name_table, sic_ff_table, match_table):
        self.stage = stage
        self.asset_table, self.company_table = asset_table, company_table
        self.name_table, self.sic_ff_table, self.match_table = name_table, sic_ff_table, match_table

    def _read(self, cur, sql):
        cur.execute(sql)
        return pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])

    def run(self):
        from psycopg2.extras import execute_values
        conn = self.stage.p.ensure_connected()
        started, t0 = _now(), time.perf_counter()
        with conn.cursor() as cur:
            assets = self._read(cur, f"""
                SELECT cusip, issuer_name, first_year, last_year,
                       upper(split_part(figi_ticker, ' ', 1)) AS ticker
                FROM {self.asset_table}""")
            names = self._read(cur, f"""
                SELECT n.cik, n.name,
                       coalesce(extract(year FROM n.name_from)::int, 1900) AS y_from,
                       coalesce(extract(year FROM n.name_to)::int, 9999)   AS y_to,
                       c.sic::int AS sic
                FROM {self.name_table} n
                JOIN {self.company_table} c USING (cik)
                WHERE c.sic IS NOT NULL""")
            names["name_key"] = names["name"].map(issuer_name_key)
            names = names[names["name_key"] != ""]
            tickers = self._read(cur, f"""
                SELECT DISTINCT ON (t) t AS ticker, cik
                FROM {self.company_table}, unnest(tickers) t
                WHERE sic IS NOT NULL
                ORDER BY t, cik""")
            lookup = dict(self._read(cur, f"""
                SELECT sic::int, industry_number FROM {self.sic_ff_table} WHERE scheme = 48""")
                          .itertuples(index=False, name=None))

        # industry key for the same-industry rule: the FF48 industry, or the SIC itself if FF48 does not list it
        sic_ff = {s: lookup.get(s, ("sic", s)) for s in names["sic"].unique()}

        blocks = {}
        for r in names.itertuples(index=False):
            words = r.name_key.split()
            blocks.setdefault(words[0][:3], []).append((words, r.cik, r.y_from, r.y_to, r.sic))
        ticker_cik = dict(zip(tickers["ticker"], tickers["cik"]))

        rows = []
        for a in assets.itertuples(index=False):
            words = issuer_name_key(a.issuer_name).split()
            trunc = len(a.issuer_name or "") >= self.TRUNCATED_LEN
            cands = [c for c in blocks.get(words[0][:3], []) if _name_match(words, c[0], trunc)] if words else []
            if not cands and trunc and len(words) > 1:
                # the cut may fall inside a dropped suffix: "AUTOMATIC DATA PROCESSING IN(C)"
                cands = [c for c in blocks.get(words[0][:3], []) if _name_match(words[:-1], c[0], True)]
            # an exact name beats abbreviation matches (CARS COM: Cars.com, not CARPARTSONSALE COM)
            exact = [c for c in cands if c[0] == words]
            cands = exact or cands
            # an operating filer beats ADR registrants (SIC 8880) and shells (9995) of the same name
            cands = [c for c in cands if c[4] not in self.PLACEHOLDER_SIC] or cands
            ciks = sorted({c[1] for c in cands})
            t_cik = ticker_cik.get(a.ticker) if a.ticker else None
            cik, method = None, "unmatched"
            if len(ciks) == 1:
                cik, method = ciks[0], "name+ticker" if t_cik == ciks[0] else "name"
            elif ciks:
                in_period = sorted({c[1] for c in cands if c[2] <= a.last_year and c[3] >= a.first_year})
                if t_cik in ciks:
                    cik, method = t_cik, "name+ticker"
                elif len(in_period) == 1:
                    cik, method = in_period[0], "name+period"
                elif len({sic_ff[c[4]] for c in cands}) == 1:
                    cik = max(cands, key=lambda c: (c[3], c[1]))[1]    # name used most recently
                    method = "name+same_industry"
                else:
                    method = "ambiguous"
            rows.append((a.cusip, cik, method, len(ciks), ciks[:20], t_cik))

        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE {self.match_table}")
                execute_values(cur, f"""INSERT INTO {self.match_table}
                    (cusip, cik, method, candidates, candidate_ciks, ticker_cik) VALUES %s""",
                               rows, page_size=5000)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.autocommit = True

        summary = pd.Series([r[2] for r in rows]).value_counts().rename_axis("method").rename("assets").reset_index()
        self.stage.record("match_issuers", kind="python", result=summary, rowcount=len(rows),
                          started_at=started, duration_s=time.perf_counter() - t0,
                          note=f"{len(assets):,} assets | {len(names):,} EDGAR names with SIC | "
                               f"{len(ticker_cik):,} EDGAR tickers")
        return summary


# =====================================================================
# OpenFIGI mapping (https://www.openfigi.com/api)
# =====================================================================

class OpenFigiMapper:
    """Maps CUSIPs to FIGIs with the OpenFIGI v3 mapping API and stores the answers.

    * US CUSIPs (first character a digit) are sent as ID_CUSIP, foreign issuers (CINS, first character a
      letter) as ID_CINS.
    * Every CUSIP sent gets one row in `lookup_table` (status found / not_found / error), so a run can be
      interrupted and resumed: only CUSIPs without a lookup row (or with status error) are sent again.
    * One record per composite (country) is kept in `record_table`: the composite record itself, or the
      first venue record of that composite when OpenFIGI returns only venue records.
    * Rate limits are read from the response headers; HTTP 429 waits for the reset, 5xx is retried.
      Without an API key: 10 CUSIPs per request, 25 requests/min. With a key: 100 per request, 25 per 6 s."""

    URL = "https://api.openfigi.com/v3/mapping"
    RECORD_FIELDS = ["figi", "composite_figi", "share_class_figi", "name", "ticker", "exch_code",
                     "market_sector", "security_type", "security_type2", "security_description"]
    API_FIELDS = {"figi": "figi", "composite_figi": "compositeFIGI", "share_class_figi": "shareClassFIGI",
                  "name": "name", "ticker": "ticker", "exch_code": "exchCode", "market_sector": "marketSector",
                  "security_type": "securityType", "security_type2": "securityType2",
                  "security_description": "securityDescription"}

    def __init__(self, stage, lookup_table, record_table, api_key=None, max_retries=5):
        self.stage = stage
        self.lookup_table, self.record_table = lookup_table, record_table
        self.api_key = api_key
        self.jobs_per_request = 100 if api_key else 10
        self.max_retries = max_retries

    @staticmethod
    def id_type(cusip):
        return "ID_CUSIP" if cusip[:1].isdigit() else "ID_CINS"

    def estimate(self, n):
        """Rough duration for n CUSIPs at the documented rate limits."""
        requests_needed = -(-n // self.jobs_per_request)
        seconds = requests_needed * (6 / 25 if self.api_key else 60 / 25)
        return requests_needed, seconds

    def _post(self, jobs):
        import requests
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["X-OPENFIGI-APIKEY"] = self.api_key
        for attempt in range(self.max_retries):
            r = requests.post(self.URL, json=jobs, headers=headers, timeout=60)
            if r.status_code == 429:
                time.sleep(int(r.headers.get("ratelimit-reset", 10)) + 1)
                continue
            if r.status_code >= 500:
                time.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            if r.headers.get("ratelimit-remaining") == "0":      # pace before the next call
                time.sleep(int(r.headers.get("ratelimit-reset", 1)) + 0.5)
            return r.json()
        raise RuntimeError(f"OpenFIGI: no answer after {self.max_retries} attempts")

    def _store(self, cur, cusips, jobs, answers):
        from psycopg2.extras import execute_values
        lookups, records = [], []
        for cusip, job, item in zip(cusips, jobs, answers):
            data = item.get("data") or []
            if data:
                status, message = "found", None
            elif "warning" in item:
                status, message = "not_found", item["warning"]
            else:
                status, message = "error", item.get("error", "no data")
            lookups.append((cusip, job["idType"], status, message, len(data)))
            # one record per composite (country): the composite record itself when OpenFIGI returns it,
            # otherwise the first venue record of that composite
            per_composite = {}
            for rec in data:
                key = rec.get("compositeFIGI") or rec.get("figi")
                is_composite = rec.get("compositeFIGI") in (None, rec.get("figi"))
                if key not in per_composite or (is_composite and not per_composite[key][1]):
                    per_composite[key] = (rec, is_composite)
            for rec, _ in per_composite.values():
                records.append((cusip,) + tuple(rec.get(self.API_FIELDS[f]) for f in self.RECORD_FIELDS))
        execute_values(cur, f"""
            INSERT INTO {self.lookup_table} (cusip, id_type, status, message, api_records)
            VALUES %s
            ON CONFLICT (cusip) DO UPDATE
            SET id_type = EXCLUDED.id_type, status = EXCLUDED.status, message = EXCLUDED.message,
                api_records = EXCLUDED.api_records, fetched_at = now()""", lookups)
        cur.execute(f"DELETE FROM {self.record_table} WHERE cusip = ANY(%s)", (list(cusips),))
        if records:
            execute_values(cur, f"""
                INSERT INTO {self.record_table} (cusip, {', '.join(self.RECORD_FIELDS)})
                VALUES %s""", records)
        return lookups, len(records)

    def run(self, cusips):
        """Look up the given CUSIPs; one transaction per request, so an interrupt loses nothing saved.
        Interrupt the kernel (■) to stop after the current request."""
        from tqdm.auto import tqdm
        conn = self.stage.p.ensure_connected()
        cusips = list(cusips)
        started, t0 = _now(), time.perf_counter()
        status, err, done, n_records = "ok", None, [], 0
        batches = [cusips[i:i + self.jobs_per_request] for i in range(0, len(cusips), self.jobs_per_request)]
        conn.autocommit = False
        try:
            for batch in tqdm(batches, desc="OpenFIGI", unit="request"):
                jobs = [{"idType": self.id_type(c), "idValue": c} for c in batch]
                answers = self._post(jobs)
                with conn.cursor() as cur:
                    lookups, n = self._store(cur, batch, jobs, answers)
                conn.commit()
                done += lookups
                n_records += n
        except KeyboardInterrupt:
            conn.rollback()
            status = "interrupted"
        except Exception as e:
            conn.rollback()
            status, err = "error", f"{type(e).__name__}: {e}"
        finally:
            conn.autocommit = True

        df = pd.DataFrame(done, columns=["cusip", "id_type", "status", "message", "api_records"])
        summary = (df.groupby(["id_type", "status"]).size().rename("cusips").reset_index()
                   if not df.empty else df)
        self.stage.record("openfigi_lookup", kind="api", status=status, result=summary, rowcount=len(df),
                          started_at=started, duration_s=time.perf_counter() - t0, error=err,
                          note=f"{len(df):,} of {len(cusips):,} CUSIPs looked up | composite records stored: "
                               f"{n_records:,} | {'with' if self.api_key else 'without'} API key")
        if status == "error":
            raise RuntimeError(f"openfigi_lookup failed: {err}")
        return df


# =====================================================================
# Analytics over run logs
# =====================================================================

def load_logs(log_root):
    """All steps.jsonl files under log_root as one DataFrame."""
    frames = [pd.read_json(p, lines=True, dtype={"run_id": str})
              for p in sorted(Path(log_root).glob("*/steps.jsonl")) if p.stat().st_size]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def steps_table(logs, run_id):
    return logs.loc[logs["run_id"] == run_id, REPORT_COLS]


def expected_mismatches(logs, run_id):
    """Steps whose row count differs from the previous run (Status.md)."""
    t = steps_table(logs, run_id)
    return t[t["diff"].notna() & (t["diff"] != 0)]


def phase_durations(logs, run_id):
    t = steps_table(logs, run_id)
    return t.groupby("phase", sort=False)["duration_s"].sum().to_frame("seconds")


def compare_runs(logs):
    """Row count and duration of each step, one column per run."""
    return logs.pivot_table(index=["phase", "step"], columns="run_id",
                            values=["rowcount", "duration_s"], aggfunc="last", sort=False)
