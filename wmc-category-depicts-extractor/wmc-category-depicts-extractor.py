"""
Wikimedia Commons category "depicts" (P180) extractor

What it does
- Fetches all files in a Wikimedia Commons category (namespace ns=6) and follows API pagination.
- Loads each file's MediaInfo entity (M...) and collects its P180 ("depicts") statements.
- Looks up human‑readable labels for the depicted Wikidata items (Q...) in a preferred
language with sensible fallbacks.
- Writes a delimited text file (CSV‑like) and echoes a human‑readable summary to stdout.

Key features
- Robust requests: retries with backoff, request timeouts, and respectful `maxlag`.
- Efficient batching for both MediaInfo and Wikidata label lookups.
- Language fallback: prefers `LANG`, then falls back to `en`, `de`, `fr`.
- Deterministic filename: `<SanitizedCategory>_thingsdepicted_<LANG>_<ddmmyyyy>.csv`.
- Output uses a multi‑character field delimiter (default `^^^^`) with simple escaping.

Configuration
- `CATEGORY` — Commons category title including the `Category:` prefix.
- `LANG` — ISO 639‑1 code for labels (e.g., `nl`).
- `LIMIT` — optional cap on number of files to process (for testing).

Output
- Columns: `midURL`, `title`, `depicts_count`, `depicts`.
- `depicts` is a ` --- ` joined list like: `Label (https://www.wikidata.org/wiki/Q123)`.
- Filename is built by `build_output_filename()` using the current date (ddmmyyyy).
- Stdout echo per row: `midURL^^^^depicts_count^^^^title^^^^depicts`.


Attribution & license
- Inspired by item 47b in:
https://kbnlwikimedia.github.io/KBCollectionHighlights/stories/Cool%20new%20things%20you%20can%20now%20do%20with%20the%20KB's%20collection%20highlights/Part%205%2C%20Reuse.html
- License: CC0
- Author: Olaf Janssen, Wikimedia coordinator @ KB (National Library of the Netherlands)
- Supported by ChatGPT
- Latest update: 17 October 2025
"""

from __future__ import annotations
import re
from datetime import datetime
from typing import Dict, Iterable, List, Set, Tuple
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

COMMONS_API = "https://commons.wikimedia.org/w/api.php"
WIKIDATA_API = "https://www.wikidata.org/w/api.php"

DEFAULT_UA = (
    "Wikimedia Commons Category Depicts Extractor/1.0 "
    "(User:OlafJanssen; mailto:olaf.janssen@kb.nl)"
)

# ========================
# Configure here
# ========================
CATEGORY = "Category:Atlas_de_Wit_1698"  # e.g., "Category:Atlas_de_Wit_1698"
LANG = "nl"  # preferred language for P180 labels
#LIMIT = 3 # Optional: limit number of files for testing (None = all)
LIMIT = None
# ========================

def make_session(user_agent: str = DEFAULT_UA) -> requests.Session:
    """Create and return a configured :class:`requests.Session`.

    The session includes a polite User-Agent header and an HTTP adapter with
    retry/backoff for transient errors (429/5xx). Use per-request timeouts
    when calling ``session.get(...)``.

    Raises
    ------
    RuntimeError
        If the HTTP session could not be initialized.
    """
    try:
        s = requests.Session()
        s.headers.update({
            "Accept": "application/json",
            "User-Agent": user_agent,
        })
        retries = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retries)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        return s
    except Exception as e:
        raise RuntimeError(f"Failed to initialize HTTP session: {e}") from e


def fetch_category_file_pageids(
    s: requests.Session, category: str, limit: int | None = None
) -> List[int]:
    """Return all pageids (namespace 6) for files in a category, handling continuation."""
    params = {
        "action": "query",
        "generator": "categorymembers",
        "gcmtitle": category,
        "gcmnamespace": 6,  # File namespace
        "gcmtype": "file",
        "gcmlimit": "500",
        "format": "json",
        "formatversion": "2",
        "maxlag": "5",
    }
    pageids: List[int] = []
    while True:
        r = s.get(COMMONS_API, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        for p in data.get("query", {}).get("pages", []):
            if p.get("missing"):
                continue
            pageids.append(p["pageid"])
            if limit and len(pageids) >= limit:
                return pageids[:limit]
        cont = data.get("continue")
        if not cont:
            break
        params.update(cont)
    return pageids


def chunks(lst: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def fetch_mediainfo_entities(
    s: requests.Session, mids: List[str]
) -> Dict[str, dict]:
    """Batch fetch MediaInfo entities (M...) from Commons."""
    out: Dict[str, dict] = {}
    for batch in chunks(mids, 50):  # API limit ~50 for non-bot clients
        params = {
            "action": "wbgetentities",
            "ids": "|".join(batch),
            "format": "json",
            "maxlag": "5",
        }
        r = s.get(COMMONS_API, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        out.update(data.get("entities", {}))
    return out


def extract_p180_qids(entity: dict) -> Tuple[str, List[str]]:
    """Return (title, [QIDs]) from a MediaInfo entity. Skips novalue/somevalue."""
    title = entity.get("title", "")
    stmts = entity.get("statements", {}) or {}
    p180 = stmts.get("P180", []) or []
    qids: List[str] = []
    for st in p180:
        try:
            datavalue = st["mainsnak"].get("datavalue")
            if not datavalue:  # novalue/somevalue
                continue
            qid = datavalue["value"]["id"]
            if isinstance(qid, str) and qid.startswith("Q"):
                qids.append(qid)
        except Exception:
            continue
    return title, qids


def fetch_labels_for_qids(
    s: requests.Session,
    qids: Iterable[str],
    lang: str,
    fallback_langs: List[str] | None = None,
) -> Dict[str, str]:
    """Return preferred labels for QIDs with language fallback."""
    if fallback_langs is None:
        fallback_langs = ["en", "de", "fr"]
    langs = [lang] + [L for L in fallback_langs if L != lang]
    labels: Dict[str, str] = {}
    uniq = sorted(set(qids))
    if not uniq:
        return labels
    for batch in chunks(uniq, 50):  # API limit ~50
        params = {
            "action": "wbgetentities",
            "ids": "|".join(batch),
            "props": "labels",
            "languages": "|".join(langs),
            "format": "json",
            "maxlag": "5",
        }
        r = s.get(WIKIDATA_API, params=params, timeout=30)
        r.raise_for_status()
        ents = r.json().get("entities", {})
        for qid, ent in ents.items():
            lab = None
            labs = ent.get("labels", {}) or {}
            for L in langs:
                if L in labs:
                    lab = labs[L]["value"]
                    break
            labels[qid] = lab or qid  # fall back to QID
    return labels


def sanitize_category_for_filename(cat: str) -> str:
    """Sanitize a Commons category for safe filenames.
    Removes the ``Category:`` prefix and any non-alphanumeric characters,
    then joins what's left without separators (e.g., ``Category:Foo_Bar`` → ``FooBar``).
    Returns ``"CommonsCategory"`` if the result would be empty.
    """
    # Remove namespace prefix and non-alphanumerics
    base = re.sub(r"^Category:", "", cat)
    base = re.sub(r"[^A-Za-z0-9]+", "", base)
    return base


def build_output_filename(category: str, lang: str, *, prefix: str = "", suffix: str = "", datefmt: str = "%d%m%Y") -> str:
    """
    Generic filename builder.
    Example: <SanitizedCategory>_thingsdepicted_<LANG>_<ddmmyyyy>.csv
    - Sanitization removes "Category:" and non-alphanumerics → e.g.,
      "Category:Atlas_de_Wit_1698" → "AtlasdeWit1698"
    - Optional prefix/suffix/datefmt for future flexibility.
    """
    today = datetime.now().strftime(datefmt)
    cat_part = sanitize_category_for_filename(category) or "CommonsCategory"
    components = [prefix, cat_part, "thingsdepicted", lang.upper(), today, suffix]
    components = [c for c in components if c]
    return "_".join(components) + ".csv"


def run(category: str, lang: str = "nl", limit: int | None = None) -> List[dict]:
    s = make_session()
    pageids = fetch_category_file_pageids(s, category, limit=limit)
    mids = [f"M{pid}" for pid in pageids]

    entities = fetch_mediainfo_entities(s, mids)

    rows: List[dict] = []
    all_qids: Set[str] = set()
    parsed: Dict[str, Tuple[str, List[str]]] = {}

    for mid in mids:
        ent = entities.get(mid)
        if not ent:
            continue
        title, qids = extract_p180_qids(ent)
        parsed[mid] = (title, qids)
        all_qids.update(qids)

    labels = fetch_labels_for_qids(s, all_qids, lang, fallback_langs=["en", "de", "fr"])

    for mid, (title, qids) in parsed.items():
        depicts = [f"{labels.get(q, q)} (https://www.wikidata.org/wiki/{q})" for q in qids]
        rows.append(
            {
                "midURL": f'https://commons.wikimedia.org/entity/{mid}',
                "title": title,
                "depicts_count": len(qids),
                "depicts": " --- ".join(depicts),
            }
        )

    return rows

def write_csv(rows: List[dict], path: str, delimiter: str = "^^^^") -> None:
    """
    Write a *delimited* text file using a multi-character delimiter.
    Note: This is not strict RFC CSV because the csv module only supports
    single-character delimiters. We implement simple escaping so field values
    can safely contain the delimiter string:
      - Backslashes are doubled ("\" → "\\")
      - Any occurrence of the delimiter inside a field is prefixed with a backslash
        (e.g., "^^^^" → "\^^^^")
    Downstream readers should reverse this by processing backslash escapes.
    """
    fieldnames = ["midURL", "title", "depicts_count", "depicts"]

    def _escape(val: str) -> str:
        val.replace("\\", "\\\\") # escape backslashes first
        val.replace(delimiter, "\\" + delimiter) # escape the delimiter
        # normalize newlines to spaces so each record stays on one line
        val.replace("\r", " ").replace("\n", " ")
        return val

    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(delimiter.join(fieldnames) + "\n")
        for row in rows:
            parts = [_escape(str(row.get(k, ""))) for k in fieldnames]
            f.write(delimiter.join(parts) + "\n")


# ========================
# Main orchestration
# ========================

if __name__ == "__main__":
    rows = run(CATEGORY, LANG, LIMIT)
    filename = build_output_filename(CATEGORY, LANG)
    write_csv(rows, filename)
    print(f"midURL^^^^title^^^^depicts_count^^^^depicts")
    for r in rows:
        print(f"{r['midURL']}^^^^{r['title']}^^^^{r['depicts_count']}^^^^{r['depicts']}")
    print(f"Wrote {len(rows)} rows to: {filename}")
