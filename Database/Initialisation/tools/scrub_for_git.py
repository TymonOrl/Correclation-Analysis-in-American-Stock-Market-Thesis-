"""
Replace local path prefixes (SCRUB_PATHS in local_config.py) before a commit:

* notebooks under Initialisation/: in cell outputs only (code is not changed; a local path left in a cell
  source is reported, move it to local_config.py)
* text files under logs/ (steps.jsonl, result CSVs, loader output)

Git-ignored files (config/local_config.py, data/, old/backups/) are skipped.

    python tools/scrub_for_git.py           # rewrite in place
    python tools/scrub_for_git.py --check   # only report, exit code 1 if anything would change
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent                      # .../Database/Initialisation
sys.path.insert(0, str(ROOT / "config"))
from local_config import SCRUB_PATHS

SKIP = [ROOT / "config" / "local_config.py", ROOT / "data", ROOT / "old" / "backups"]
LOG_SUFFIXES = {".jsonl", ".csv", ".txt"}

# longest prefix first, so a shorter one never cuts a longer one apart
PREFIXES = sorted(SCRUB_PATHS.items(), key=lambda kv: -len(kv[0]))


def scrub(text):
    for prefix, placeholder in PREFIXES:
        text = text.replace(prefix, placeholder)
    return text


def scrub_json(value):
    """Scrub every string in a nested notebook output (text, html, plain-text data)."""
    if isinstance(value, str):
        return scrub(value)
    if isinstance(value, list):
        return [scrub_json(v) for v in value]
    if isinstance(value, dict):
        return {k: scrub_json(v) for k, v in value.items()}
    return value


def skipped(path):
    return any(path == s or s in path.parents for s in SKIP)


def scrub_notebook(path, check):
    """Returns (changed, cells whose source still holds a local path)."""
    raw = path.read_text()
    nb = json.loads(raw)
    in_source = []
    for i, cell in enumerate(nb["cells"]):
        if scrub("".join(cell["source"])) != "".join(cell["source"]):
            in_source.append(i)
        if "outputs" in cell:
            cell["outputs"] = scrub_json(cell["outputs"])
    new = json.dumps(nb, indent=1, ensure_ascii=False) + "\n"
    if new != raw and not check:
        path.write_text(new)
    return new != raw, in_source


def main(check):
    changed, problems = [], []
    for path in sorted(ROOT.rglob("*.ipynb")):
        if skipped(path) or ".ipynb_checkpoints" in path.parts:
            continue
        was_changed, in_source = scrub_notebook(path, check)
        if was_changed:
            changed.append(path)
        if in_source:
            problems.append(f"{path.relative_to(ROOT)}: local path in the source of cell(s) {in_source}")

    for path in sorted((ROOT / "logs").rglob("*")):
        if (path.suffix not in LOG_SUFFIXES or not path.is_file() or skipped(path)
                or path.name == "tracking_before_reset.csv"):           # git-ignored, ~300k local paths
            continue
        raw = path.read_text(errors="surrogateescape")
        new = scrub(raw)
        if new != raw:
            changed.append(path)
            if not check:
                path.write_text(new, errors="surrogateescape")

    for path in sorted(p for p in ROOT.rglob("*") if p.suffix in {".py", ".md", ".sql", ".txt"} and p.is_file()):
        if skipped(path) or path.relative_to(ROOT).parts[0] == "logs":
            continue
        if scrub(path.read_text(errors="surrogateescape")) != path.read_text(errors="surrogateescape"):
            problems.append(f"{path.relative_to(ROOT)}: local path in the file, move it to local_config.py")

    verb = "would change" if check else "changed"
    print(f"{len(changed)} files {verb}")
    for path in changed:
        print("  ", path.relative_to(ROOT))
    for problem in problems:
        print("!!", problem)
    return 1 if (check and changed) or problems else 0


if __name__ == "__main__":
    sys.exit(main(check="--check" in sys.argv))
