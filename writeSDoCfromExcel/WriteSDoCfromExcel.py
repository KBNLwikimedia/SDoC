#!/usr/bin/env python3
"""
Write Structured Data on Commons (SDC) claims from Excel

This script reads an Excel sheet and adds Wikidata item values (QIDs) to
Wikimedia Commons files’ Structured Data using the Commons API. It is designed
for IDE use (PyCharm, VS Code): configure the `CONFIG` dict below and run.

Key features
------------
- Property→QID mapping:
  * Supports one or many properties (e.g., P180 “depicts”, P170 “creator”, …)
  * Reads QIDs from per-property columns defined in `CONFIG["PROP_MAP"]`
- Duplicate safety:
  * Fetches existing QIDs for each property on each file
  * Skips rows where the QID is already present (no overwrite)
- Flexible file identification:
  * Uses `CommonsMid` (M-ID) when present
  * If `CommonsMid` is missing/empty, resolves M-ID from `CommonsFile`
    (accepts full URL, `File:` title, or bare filename)
- Clear audit log:
  * Writes a CSV with one line per attempted add:
    ADDED / SKIPPED_DUPLICATE / SKIPPED_INVALID / ERROR
- Polite and robust:
  * Retries with backoff for transient HTTP errors
  * `PAUSE` between writes and `MAX_EDITS` safety cap
  * `DRY_RUN` mode to simulate edits without changing Commons

How it works (high level)
-------------------------
1. Reads the Excel file and required columns.
2. For each row and mapped property:
   a. Determines the target file’s M-ID (from `CommonsMid`, or by resolving
      from `CommonsFile` via the MediaWiki API).
   b. Validates inputs (M-ID, P-ID, Q-ID).
   c. Fetches existing values for that property on the file.
   d. If the QID is absent:
        - In DRY_RUN: logs “WOULD_ADD”.
        - Otherwise: posts `wbcreateclaim` to add the value.
      If the QID already exists: logs “SKIPPED_DUPLICATE”.
3. Writes a CSV log and prints a summary.

Excel requirements
------------------
- Required column: `CONFIG["FILE_COLUMN"]` (default: "CommonsFile")
  * May be a full Commons URL, a `File:…` title, or a bare filename.
- Optional column: `CONFIG["MID_COLUMN"]` (default: "CommonsMid")
  * If present and valid, used directly; otherwise the script resolves the M-ID.
- For each property in `CONFIG["PROP_MAP"]`, a column containing QIDs
  (e.g., ("P180","QidDepicts")).

Configuration (CONFIG)
----------------------
- EXCEL_PATH: Path to the Excel file to read.
- SHEET_NAME: Name of the sheet to read.
- FILE_COLUMN: Column with Commons file identifiers (URL/title/filename).
- MID_COLUMN: Column with M-IDs (e.g., “M123456”). Optional.
- PROP_MAP: List of (property_id, qid_column) pairs, e.g. [("P180","QidDepicts")].
  * Alternatively, set PROPERTY + QID_COLUMN for a single property.
- DOTENV_PATH: Path to a .env file with credentials (optional).
- USER / PASSWORD: Commons credentials (if not using .env).
- USER_AGENT: User-agent string sent with API requests.
- DRY_RUN: True to simulate, False to write.
- PAUSE: Seconds to sleep between successful edits.
- MAX_EDITS: Stop after this many successful writes (None = no cap).
- VERBOSE: Print extra detail (e.g., the full list of existing QIDs).
- LOG_CSV: Output CSV path; supports {timestamp} placeholder.

Authentication
--------------
- Recommended: use a bot password and set credentials in a `.env` file:
    COMMONS_USER=YourUsername@YourBot
    COMMONS_PASS=YourBotPassword
    COMMONS_USER_AGENT=Your-Tool-Name/1.0 (contact)
- If login fails or no credentials are provided, the script proceeds
  unauthenticated; any edits (if DRY_RUN=False) may be attributed to your IP.

APIs and endpoints
------------------
- Resolve M-ID from file: `action=query&prop=info&titles=File:…`
- Read current SDC values: `action=wbgetentities&ids=M…`
- Add a claim (wikibase-item value): `action=wbcreateclaim`

Logging
-------
The CSV contains:
  timestamp, commons_mid, commons_file, property, qid, action,
  details, edit_id, dry_run

Typical actions:
- ADDED: Claim was added (includes `edit_id` if available).
- SKIPPED_DUPLICATE: QID already present for the property.
- SKIPPED_INVALID: Missing/invalid input (e.g., could not resolve M-ID).
- ERROR: API/Network/other error (details captured).

Rate-limiting & etiquette
-------------------------
- Be courteous to Commons: keep a nonzero PAUSE, and consider small batches
  with MAX_EDITS. When possible, run under a dedicated bot account.

Requirements
------------
- Python 3.10+
- Libraries: requests, pandas, urllib3 (Retry is used), python-dotenv (optional)

Attribution & license
-------
- Author: Olaf Janssen, Wikimedia coordinator @ KB (National Library of the Netherlands), with thanks to User:Multichill
  for the original code
- Supported by ChatGPT
- License: CC0 1.0 (Public Domain).
- Latest update: 17 October 2025
"""

from __future__ import annotations
import csv
import json
import os
import re
import urllib.parse
import time
import datetime as dt
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from os.path import abspath

try:
    from dotenv import load_dotenv
except Exception:  # package optional; script still runs without it
    def load_dotenv(*args, **kwargs):  # no-op fallback
        return False

CONFIG = {
    "EXCEL_PATH": "p180-inputfile-for-repo.xlsx",
    "SHEET_NAME": "CommonsFilesP180Qids",
    "FILE_COLUMN": "CommonsFile",
    "MID_COLUMN": "CommonsMid",
    "PROP_MAP": [("P180", "QidDepicts")], # Make sure 'QidDepicts' is column in your Excel
    # Alternatively, set PROPERTY + QID_COLUMN for a single property.
    "PROPERTY": None,
    "QID_COLUMN": None,
    # .env path (set to None to disable automatic loading. In that case, use the USER/PASSWORD below.)
    "DOTENV_PATH": ".env",  # Adapt .env / .env-example file for your own Wikimedia login credentials and user agent
    "USER": None,
    "PASSWORD": None,
    "USER_AGENT": "KB-Excel-SDoC-Writer/1.0 (User:OlafJanssen; olaf.janssen@kb.nl)", # Adapt for your ow UA string
    "DRY_RUN": True, # True = simulation, False = write to Commons
    "PAUSE": 0.5,
    "MAX_EDITS": None,
    "VERBOSE": False,
    "LOG_CSV": os.path.join("logs", "write_sdoc_log_{timestamp}.csv"), # see logs folder for log files
}

# Adapt .env / .env-example file for your own Wikimedia login credentials and user agent
load_dotenv(abspath(CONFIG.get("DOTENV_PATH") or ".env"))

API_URL = "https://commons.wikimedia.org/w/api.php"

MID_RE = re.compile(r"^M\d+$")
PID_RE = re.compile(r"^P\d+$")
QID_RE = re.compile(r"^Q\d+$")


@dataclass
class Config:
    excel_path: str
    sheet_name: str
    mid_col: str
    file_col: str
    prop_to_qid_col: List[Tuple[str, str]]  # list of (P-id, qid_column)
    user: Optional[str]
    password: Optional[str]
    dry_run: bool
    pause: float
    max_edits: Optional[int]
    log_csv: str
    verbose: bool
    user_agent: str


# ------------------------- HTTP session helpers ------------------------- #

def session_with_retries(user_agent: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": user_agent})
    retry = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist={429, 500, 502, 503, 504},
        allowed_methods=frozenset({"GET", "POST"}),
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def get_login_token(sess: requests.Session) -> str:
    r = sess.post(
        API_URL,
        data={"action": "query", "meta": "tokens", "type": "login", "format": "json"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json().get("query", {}).get("tokens", {}).get("logintoken", "")


def login(sess: requests.Session, user: str, password: str) -> bool:
    token = get_login_token(sess)
    if not token:
        print("[WARN] Could not obtain login token; continuing unauthenticated.")
        return False
    r = sess.post(
        API_URL,
        data={
            "action": "login",
            "format": "json",
            "lgname": user,
            "lgpassword": password,
            "lgtoken": token,
        },
        timeout=30,
    )
    r.raise_for_status()
    res = r.json()
    success = res.get("login", {}).get("result") == "Success"
    if success:
        print(f"[OK] Logged in as {user}")
    else:
        print(f"[WARN] Login failed ({res}); continuing unauthenticated (edits may be attributed to IP).")
    return success


def get_csrf_token(sess: requests.Session) -> str:
    r = sess.post(API_URL, data={"action": "query", "meta": "tokens", "format": "json"}, timeout=30)
    r.raise_for_status()
    return r.json().get("query", {}).get("tokens", {}).get("csrftoken", "")


# --------------------------- API functionality -------------------------- #

def fetch_property_qids(sess: requests.Session, mediaid: str, pid: str) -> List[str]:
    """Return list of QIDs currently present for a property on an M-entity.

    Ignores statements with snaktype other than 'value'.
    """
    params = {
        "action": "wbgetentities",
        "ids": mediaid,
        "format": "json",
    }
    r = sess.get(API_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    entity = data.get("entities", {}).get(mediaid)
    if not entity or entity.get("missing") == "":
        return []

    statements = entity.get("statements", {})
    props = statements.get(pid, [])
    qids: List[str] = []
    for stmt in props:
        mainsnak = stmt.get("mainsnak", {})
        if mainsnak.get("snaktype") != "value":
            continue
        dv = mainsnak.get("datavalue", {}).get("value", {})
        qid = dv.get("id")
        if qid and isinstance(qid, str) and QID_RE.match(qid):
            qids.append(qid)
    return qids


def add_claim(sess: requests.Session, mediaid: str, pid: str, qid: str, summary: str) -> Dict:
    numeric_id = int(qid[1:])
    value = json.dumps({"entity-type": "item", "numeric-id": numeric_id})
    token = get_csrf_token(sess)
    if not token:
        raise RuntimeError("Failed to obtain CSRF token")

    data = {
        "action": "wbcreateclaim",
        "format": "json",
        "entity": mediaid,
        "property": pid,
        "snaktype": "value",
        "value": value,
        "token": token,
        "summary": summary,
        "bot": False,
        "maxlag": 5,  # be polite to the cluster
    }
    r = sess.post(API_URL, data=data, timeout=60)
    r.raise_for_status()
    return r.json()


# ------------------------------- Utilities ------------------------------ #

def valid_or_error(mid: str, pid: str, qid: str) -> Optional[str]:
    if not (isinstance(mid, str) and MID_RE.match(mid)):
        return f"Invalid CommonsMid '{mid}'"
    if not (isinstance(pid, str) and PID_RE.match(pid)):
        return f"Invalid property id '{pid}'"
    if not (isinstance(qid, str) and QID_RE.match(qid)):
        return f"Invalid QID '{qid}'"
    return None


def now_iso() -> str:
    """Return current UTC timestamp in ISO 8601 with 'Z'.

    Uses timezone-aware datetime (datetime.UTC on 3.11+, fallback to datetime.timezone.utc).
    """
    try:
        utc = dt.UTC  # Python 3.11+
    except AttributeError:  # Python <3.11
        utc = dt.timezone.utc
    return dt.datetime.now(utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)


def resolve_log_path(template: str) -> str:
    ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    return template.replace("{timestamp}", ts)

# ------------------------------- Main logic ----------------------------- #

def normalize_file_title(file_value: str) -> Optional[str]:
    """Normalize a Commons file value (URL or title or bare filename) to a proper 'File:…' title."""
    if not file_value:
        return None
    s = str(file_value).strip()
    if not s:
        return None
    # URL form containing '/File:'
    if "/File:" in s:
        part = s.split("/File:", 1)[1]
        return "File:" + urllib.parse.unquote(part)
    # Already a File: title
    if s.startswith("File:"):
        tail = s[len("File:"):]
        return "File:" + urllib.parse.unquote(tail)
    # Bare filename -> prefix
    return "File:" + urllib.parse.unquote(s)


def commons_file_url(file_value: str) -> str:
    """Return full Commons URL for a file value (URL/title/bare filename)."""
    title = normalize_file_title(file_value)
    if not title:
        return str(file_value or "")
    slug = urllib.parse.quote(title.replace(" ", "_"))
    return f"https://commons.wikimedia.org/wiki/{slug}"

def get_commons_m_id(sess: requests.Session, file_value: str) -> Optional[str]:
    """Resolve an M-id for a Commons file (by URL or title). Returns 'M12345' or None."""
    title = normalize_file_title(file_value)
    if not title:
        return None
    params = {
        "action": "query",
        "titles": title,
        "prop": "info",
        "format": "json",
    }
    r = sess.get(API_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    pages = data.get("query", {}).get("pages", {})
    if not pages:
        return None
    page = next(iter(pages.values()))
    pageid = page.get("pageid")
    if pageid:
        return f"M{pageid}"
    return None


def process_rows(cfg: Config) -> None:
    sess = session_with_retries(cfg.user_agent)

    # Optional login
    if cfg.user and cfg.password:
        login(sess, cfg.user, cfg.password)
    else:
        print("[INFO] No credentials provided; proceeding unauthenticated (edits, if any, may be attributed to IP).")

    # Read Excel
    df = pd.read_excel(cfg.excel_path, sheet_name=cfg.sheet_name, header=0)

    # Basic normalization
    # Keep only relevant columns; don't coerce empties to 0
    needed_cols = {cfg.file_col} | {qid_col for _, qid_col in cfg.prop_to_qid_col}
    missing = [c for c in needed_cols if c not in df.columns]
    if missing:
        raise SystemExit(f"Missing required column(s) in sheet: {missing}")

    mid_present = cfg.mid_col in df.columns
    if not mid_present:
        # If we don't have M-ids in the sheet, we must rely on CommonsFile for every row
        if df[cfg.file_col].isna().any() or df[cfg.file_col].astype(str).str.strip().eq("").any():
            raise SystemExit("CommonsMid column is absent and CommonsFile has empty values; cannot resolve M-ids.")

    # Prepare CSV logger
    ensure_parent_dir(cfg.log_csv)
    log_fields = [
        "timestamp",
        "commons_mid",
        "commons_file",
        "property",
        "qid",
        "action",
        "details",
        "edit_id",
        "dry_run",
    ]
    log_fp = open(cfg.log_csv, "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(log_fp, fieldnames=log_fields)
    writer.writeheader()

    added = 0
    skipped_dupe = 0
    skipped_invalid = 0
    errors = 0

    edit_count = 0
    total_rows = len(df)

    # Iterate rows
    mid_cache: Dict[str, str] = {}
    for idx, row in df.iterrows():
        commons_file = str(row[cfg.file_col]).strip() if (cfg.file_col in df.columns and pd.notna(row[cfg.file_col])) else ""
        # Resolve Commons M-ID: prefer explicit MID; otherwise derive from file title/URL
        commons_mid = ""
        if cfg.mid_col in df.columns:
            mid_cell = row[cfg.mid_col]
            commons_mid = str(mid_cell).strip() if pd.notna(mid_cell) else ""

        # Cache M-ID lookups to reduce API calls
        if not MID_RE.match(commons_mid):
            if commons_file:
                norm_title = normalize_file_title(commons_file) or ""
                if norm_title in mid_cache:
                    commons_mid = mid_cache[norm_title]
                else:
                    resolved = get_commons_m_id(sess, commons_file)
                    if resolved:
                        commons_mid = resolved
                        mid_cache[norm_title] = resolved

        for pid, qid_col in cfg.prop_to_qid_col:
            qid_val_raw = row[qid_col] if qid_col in row and pd.notna(row[qid_col]) else ""
            qid = str(qid_val_raw).strip()
            if not qid:
                # Nothing to do for this property
                continue

            # Validate inputs
            err = valid_or_error(commons_mid, pid, qid)
            if err:
                skipped_invalid += 1
                # Be explicit if the M-id is the culprit
                if not MID_RE.match(commons_mid or ""):
                    err = "Missing/invalid CommonsMid and could not resolve from CommonsFile"
                msg = f"SKIPPED_INVALID row={idx+1}: {err}"
                print(msg)
                writer.writerow({
                    "timestamp": now_iso(),
                    "commons_mid": commons_mid,
                    "commons_file": commons_file,
                    "property": pid,
                    "qid": qid,
                    "action": "SKIPPED_INVALID",
                    "details": err,
                    "edit_id": "",
                    "dry_run": cfg.dry_run,
                })
                continue

            try:
                # Duplicate check
                existing_qids = fetch_property_qids(sess, commons_mid, pid)
                if qid in existing_qids:
                    skipped_dupe += 1
                    info = f"SKIPPED_DUPLICATE row={idx+1}: {pid} already has https://www.wikidata.org/wiki/{qid} on https://commons.wikimedia.org/entity/{commons_mid} - {commons_file_url(commons_file)}"
                    if cfg.verbose:
                        info += f" (existing: {sorted(existing_qids)})"
                    print(info)
                    writer.writerow({
                        "timestamp": now_iso(),
                        "commons_mid": commons_mid,
                        "commons_file": commons_file,
                        "property": pid,
                        "qid": qid,
                        "action": "SKIPPED_DUPLICATE",
                        "details": "QID already present",
                        "edit_id": "",
                        "dry_run": cfg.dry_run,
                    })
                    continue

                summary = f"Add {qid} to {pid} on {commons_mid} (= {commons_file}) via Commons API"

                if cfg.dry_run:
                    added += 1
                    print(f"[DRY-RUN] WOULD ADD: https://www.wikidata.org/wiki/{qid} → {pid} on https://commons.wikimedia.org/entity/{commons_mid} (= {commons_file_url(commons_file)})")
                    writer.writerow({
                        "timestamp": now_iso(),
                        "commons_mid": commons_mid,
                        "commons_file": commons_file,
                        "property": pid,
                        "qid": qid,
                        "action": "WOULD_ADD",
                        "details": summary,
                        "edit_id": "",
                        "dry_run": True,
                    })
                else:
                    # Respect --max-edits
                    if cfg.max_edits is not None and edit_count >= cfg.max_edits:
                        print("[INFO] Reached max-edits limit; stopping further writes.")
                        raise StopIteration

                    res = add_claim(sess, commons_mid, pid, qid, summary)
                    edit_id = (
                        res.get("claim", {}).get("id")
                        or res.get("pageinfo", {}).get("lastrevid")
                        or ""
                    )
                    added += 1
                    edit_count += 1
                    print(f"[ADDED] https://www.wikidata.org/wiki/{qid} → {pid} on https://commons.wikimedia.org/entity/{commons_mid} (= {commons_file_url(commons_file)}); edit_id={edit_id}")
                    writer.writerow({
                        "timestamp": now_iso(),
                        "commons_mid": commons_mid,
                        "commons_file": commons_file,
                        "property": pid,
                        "qid": qid,
                        "action": "ADDED",
                        "details": "",
                        "edit_id": edit_id,
                        "dry_run": False,
                    })

                    # Polite pause between edits
                    if cfg.pause > 0:
                        time.sleep(cfg.pause)

            except StopIteration:
                # Graceful break from nested loops
                break
            except Exception as e:
                errors += 1
                print(f"[ERROR] row={idx+1} https://commons.wikimedia.org/entity/{commons_mid} {pid} https://www.wikidata.org/wiki/{qid}: {e}")
                writer.writerow({
                    "timestamp": now_iso(),
                    "commons_mid": commons_mid,
                    "commons_file": commons_file,
                    "property": pid,
                    "qid": qid,
                    "action": "ERROR",
                    "details": str(e),
                    "edit_id": "",
                    "dry_run": cfg.dry_run,
                })
        else:
            # Continue if inner loop wasn't broken by StopIteration
            pass

        # If StopIteration triggered, break outer loop as well
        if cfg.max_edits is not None and edit_count >= (cfg.max_edits or 0):
            break

    log_fp.flush()
    log_fp.close()

    print("-" * 60)
    print(f"Rows processed: {total_rows}")
    print(f"Added: {added}")
    print(f"Skipped (duplicates): {skipped_dupe}")
    print(f"Skipped (invalid): {skipped_invalid}")
    print(f"Errors: {errors}")
    print(f"Log CSV: {cfg.log_csv}")


# ----------------------------- Config builder (no CLI) ---------------------------- #

def build_config_from_constants() -> Config:
    prop_map = CONFIG.get("PROP_MAP") or []
    property_single = CONFIG.get("PROPERTY")
    qid_col_single = CONFIG.get("QID_COLUMN")

    if not prop_map:
        if not property_single or not qid_col_single:
            raise SystemExit("Please set CONFIG['PROP_MAP'] OR both CONFIG['PROPERTY'] and CONFIG['QID_COLUMN'].")
        prop_map = [(property_single, qid_col_single)]

    # Validate PIDs format early
    for pid, _ in prop_map:
        if not PID_RE.match(pid):
            raise SystemExit(f"Invalid property id in PROP_MAP: '{pid}'")

    log_csv = resolve_log_path(CONFIG.get("LOG_CSV") or os.path.join("logs", "write_sdoc_log_{timestamp}.csv"))

    return Config(
        excel_path=CONFIG["EXCEL_PATH"],
        sheet_name=CONFIG["SHEET_NAME"],
        mid_col=CONFIG["MID_COLUMN"],
        file_col=CONFIG["FILE_COLUMN"],
        prop_to_qid_col=prop_map,
        user=CONFIG.get("USER") or os.getenv("COMMONS_USER"),
        password=CONFIG.get("PASSWORD") or os.getenv("COMMONS_PASS"),
        user_agent=CONFIG.get("USER_AGENT") or os.getenv("COMMONS_USER_AGENT"),
        dry_run=bool(CONFIG.get("DRY_RUN", False)),
        pause=float(CONFIG.get("PAUSE", 0.5)),
        max_edits=CONFIG.get("MAX_EDITS"),
        log_csv=log_csv,
        verbose=bool(CONFIG.get("VERBOSE", False)),
    )


def main() -> None:
    cfg = build_config_from_constants()
    print("Config:")
    print(f"  Excel:     {cfg.excel_path}")
    print(f"  Sheet:     {cfg.sheet_name}")
    print(f"  MID col:   {cfg.mid_col}")
    print(f"  File col:  {cfg.file_col}")
    print(f"  Props:     {', '.join([f'{p}:{c}' for p,c in cfg.prop_to_qid_col])}")
    print(f"  Dry run:   {cfg.dry_run}")
    print(f"  Pause:     {cfg.pause}s")
    if cfg.max_edits is not None:
        print(f"  Max edits: {cfg.max_edits}")
    print(f"  Log CSV:   {cfg.log_csv}")

    process_rows(cfg)


if __name__ == "__main__":
    main()
