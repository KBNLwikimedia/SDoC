#!/usr/bin/env python3
"""
Delpher resolver link extractor for Wikimedia Commons (PyCharm-friendly)

This script scans a Wikimedia Commons category for file pages and exports
resolver.kb.nl links found on each page to CSV, with progress logging,
batched writes, optional pausing between batches, and split outputs for
different outcomes. It is configured entirely via constants at the top
of the file—no CLI flags required (ideal for running in PyCharm).

What it does
------------
1) Lists all pages of type File in the Commons category named in CATEGORY
   (pass the category name WITHOUT the "Category:" prefix; e.g.,
   CATEGORY = "Media from Delpher"). The API call uses list=categorymembers.
2) Slices that list by index (START_INDEX inclusive, END_INDEX exclusive)
   to constrain the working range while preserving the category’s natural order.
3) For each selected file page, fetches external links via prop=extlinks and
   continues through pagination (ellimit/​eloffset).
4) Filters those links to the hostname resolver.kb.nl (accepts http or https)
   and normalizes all matches to https (scheme forced to https, fragment dropped,
   query preserved).
5) Writes results in batches of BATCH_SIZE, flushing each batch immediately to disk.
6) Optionally pauses after every batch until you press Enter
   (PAUSE_BETWEEN_BATCHES = True).
7) Splits outputs into three CSVs:
   - OUTPUT_MAIN:          one row per (file_name, kb_url) when resolver links exist
   - *_noresolver.csv:     file_name only, for pages processed but with no resolver link
   - *_nonprocessed.csv:   file_name only, for pages that were skipped (by rule) or errored

Skip rule
---------
If EXCLUDE_MPEG21_PDF is True, file pages with titles ending in "mpeg21).pdf"
(case-insensitive) are treated as NON-PROCESSED and written to *_nonprocessed.csv.

Logging
-------
For each file, the script prints one of:
- [ok]    processed with N resolver link(s), and lists them
- [ok]    processed with no resolver.kb.nl links
- [skip]  excluded by the mpeg21).pdf rule
- [error] HTTP or other error (also counted as NON-PROCESSED)

Batching & pausing
------------------
- Results are buffered and written every BATCH_SIZE files.
- If PAUSE_BETWEEN_BATCHES is True, the script waits for Enter after EACH batch,
  which guarantees a pause after the first 100 when BATCH_SIZE = 100.
  The pause uses getpass/input fallbacks that work well in IDEs like PyCharm.

Outputs
-------
- Main CSV header:      file_name,kb_url
- No-resolver CSV:      file_name
- Non-processed CSV:    file_name
If OUTPUT_NONPROCESSED/OUTPUT_NORESOLVER are None, filenames are derived from
OUTPUT_MAIN by appending _nonprocessed/_noresolver before the extension.

Networking & etiquette
----------------------
- Uses the Commons API: https://commons.wikimedia.org/w/api.php
  - list=categorymembers to enumerate files
  - prop=extlinks for per-page external links (handles continuation)
- Identifies via a custom User-Agent and sets maxlag=5.
- Includes small sleeps to reduce load; adjust if you hit throttling.

Requirements
------------
- Python 3.8+
- requests

Configuration quick guide
-------------------------
- CATEGORY:               Commons category name WITHOUT "Category:"
- START_INDEX / END_INDEX:0-based [start, end) slice of the category file list
- BATCH_SIZE:             number of files to process per batch
- PAUSE_BETWEEN_BATCHES:  True to pause after each batch (press Enter)
- EXCLUDE_MPEG21_PDF:     True to skip titles ending with "mpeg21).pdf"
- OUTPUT_*:               paths for the three CSV outputs (derived if None)

Example (defaults)
------------------
- CATEGORY = "Media from Delpher"
- START_INDEX = 0, END_INDEX = None, BATCH_SIZE = 100
- PAUSE_BETWEEN_BATCHES = True
- EXCLUDE_MPEG21_PDF = True
Running in PyCharm with these settings will process files in 100-file batches,
pause after each batch, and produce three CSVs in the working directory.

------------------

Author: ChatGPT, prompted by Olaf Janssen, Wikimedia coordinatir at KB, Koninklijke Bibliotheek, national library of the Netherlands
Latest update: 2025-09-16
License: CC0
"""

# TODO - IN PROGRESS 1) files that are from Internet Archive (See for example https://commons.wikimedia.org/wiki/File:%27s_Gravenhaagsche_courant_28-09-1825_(IA_ddd_010125125_mpeg21).pdf)
#  have Source: Internet Archive identifier: ddd_010125125_mpeg21 + https://archive.org/download/ddd_010125125_mpeg21/ddd_010125125_mpeg21.pdf
#  in their Wikitexts --> add proper resolver.kb.nl link to the wikitext
#  See: D:\KB-OPEN\___WikimediaKB\github-repos\WikimediaKBURLReplacement\WikimediaReplace_Maart2025\WikimediaReplace-AddDelpherURLs-InternetArchiveFiles.py
#  Status dd 17/9/25: In progress

# TODO: 2) For the non-IA files: expand from more Delpher match pattern than only resolver.kb.nl. For instance, also for http(s)://(www.)delpher.nl or http://kranten.kb.nl etc.
#  See patterns in servicelookups.py

# TODO: 3) only include URLs in the SDoC that are precise enough to lead directly to the resource, not a search page or intermediate page.
#  OK, precise enough: https://resolver.kb.nl/resolve?urn=MMSAK01:164709026:mpeg21:p00004
#  NOT Ok, not precise enough: www.delpher.nl  or www.delpher.nl/nl/kranten

# TODO: 4) SPARQL query to retrieve existing "source of file (P7482) statements" from files in Category:Media from Delpher
#  STATUS = DONE: See MediaFromDelpher-sourceQuery.rq and https://w.wiki/FMar --> 33 results
#  Example of desired target source statement format , see https://commons.wikimedia.org/wiki/File:%27t_Nieuws_voor_Kampen_vol_003_no_177_Televisie_onding_of_zegen.pdf

import csv
import getpass
import re
import sys
from pathlib import Path
# Add the project root to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))
import time
from typing import Dict, Iterable, List, Set, Tuple
from urllib.parse import urlparse, urlunparse

import requests

# ========= CONFIGURE HERE (PyCharm-friendly) =========
CATEGORY = "Media from Delpher"        # Commons category (without "Category:")
START_INDEX = 0                        # 0-based inclusive
END_INDEX = None                       # exclusive (None = go to end)
BATCH_SIZE = 100                       # files per batch
PAUSE_BETWEEN_BATCHES = True           # pause after each batch (press Enter)

EXCLUDE_MPEG21_PDF = True              # exclude titles ending in 'mpeg21).pdf'
OUTPUT_MAIN = "delpher_links.csv"      # rows with resolver URLs (one row per URL)
OUTPUT_NONPROCESSED = None             # CSV for skipped (exclusion/error). None -> derived from OUTPUT_MAIN
OUTPUT_NORESOLVER = None               # CSV for processed-without-resolver. None -> derived from OUTPUT_MAIN
# =====================================================

COMMONS_API = "https://commons.wikimedia.org/w/api.php"
USER_AGENT = "DelpherResolverExtractor-Extlinks/3.0 (contact: your-email@example.com)"
EXCLUDE_SUFFIX_RE = re.compile(r"mpeg21\)\.pdf$", re.IGNORECASE)


# ---------- API helpers ----------
def get_category_files(category: str, session: requests.Session) -> List[str]:
    """Return a list of 'File:...' titles in the Commons category."""
    titles: List[str] = []
    params = {
        "action": "query",
        "format": "json",
        "list": "categorymembers",
        "cmtitle": f"Category:{category}",
        "cmtype": "file",
        "cmlimit": "500",
    }
    while True:
        r = session.get(COMMONS_API, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        for m in data.get("query", {}).get("categorymembers", []):
            t = m.get("title")
            if t and t.startswith("File:"):
                titles.append(t)
        cont = data.get("continue", {})
        if "cmcontinue" in cont:
            params["cmcontinue"] = cont["cmcontinue"]
        else:
            break
        time.sleep(0.1)  # be polite
    return titles


def get_extlinks_for_title(title: str, session: requests.Session) -> List[str]:
    """Fetch extlinks for a single page title (handles continuation)."""
    links: List[str] = []
    params = {
        "action": "query",
        "format": "json",
        "prop": "extlinks",
        "titles": title,
        "ellimit": "500",
    }
    while True:
        r = session.get(COMMONS_API, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        pages: Dict = data.get("query", {}).get("pages", {})
        for _, page in pages.items():
            for el in page.get("extlinks", []) or []:
                url = el.get("*")
                if url:
                    links.append(url)
        cont = data.get("continue", {})
        if "eloffset" in cont:
            params["eloffset"] = cont["eloffset"]
        else:
            break
        time.sleep(0.1)
    return links


# ---------- URL helpers ----------
def is_kb_resolver(url: str) -> bool:
    try:
        p = urlparse(url)
    except Exception:
        return False
    if p.scheme not in ("http", "https"):
        return False
    return (p.hostname or "").lower() == "resolver.kb.nl"


def normalize_to_https(url: str) -> str:
    """Force https, drop fragment, keep query."""
    p = urlparse(url)
    p = p._replace(scheme="https", fragment="")
    path = p.path or "/"
    return urlunparse((p.scheme, p.netloc, path, p.params, p.query, ""))


# ---------- Utility ----------
def chunked(seq: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def wait_for_enter(prompt: str) -> None:
    """
    Reliable 'press Enter to continue' in IDEs and non-TTY contexts.
    getpass.getpass() reads from the controlling terminal on POSIX and the console on Windows.
    """
    try:
        getpass.getpass(prompt)
    except Exception:
        try:
            input(prompt)
        except Exception:
            # Try /dev/tty on POSIX as a last resort
            try:
                with open("/dev/tty", "r") as tty:
                    print(prompt, end="", flush=True)
                    tty.readline()
            except Exception:
                print("[pause] Could not read interactive input; continuing without pause.", flush=True)


# ---------- Main ----------
def main():
    # Resolve file paths
    base = Path(OUTPUT_MAIN)
    skipped_path = Path(OUTPUT_NONPROCESSED) if OUTPUT_NONPROCESSED else base.with_name(f"{base.stem}_nonprocessed{base.suffix}")
    noresolver_path = Path(OUTPUT_NORESOLVER) if OUTPUT_NORESOLVER else base.with_name(f"{base.stem}_noresolver{base.suffix}")

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    session.params = {"maxlag": "5"}

    try:
        print(f"[info] Fetching category listing for: Category:{CATEGORY}", flush=True)
        all_files = get_category_files(CATEGORY, session)
        total_all = len(all_files)
        print(f"[info] Found {total_all} file(s) in Category:{CATEGORY}", flush=True)

        # Slice to preserve the category’s natural order by index
        sliced = all_files[slice(START_INDEX, END_INDEX)]
        base_index = START_INDEX or 0
        total_range = len(sliced)
        last_index = base_index + total_range - 1 if total_range else base_index - 1
        print(f"[info] Working range: {base_index}..{last_index} ({total_range} file(s))", flush=True)

        # Prepare CSVs (write headers once)
        with open(base, "w", newline="", encoding="utf-8") as f_main, \
             open(skipped_path, "w", newline="", encoding="utf-8") as f_skip, \
             open(noresolver_path, "w", newline="", encoding="utf-8") as f_none:

            w_main = csv.writer(f_main)
            w_skip = csv.writer(f_skip)
            w_none = csv.writer(f_none)

            w_main.writerow(["file_name", "kb_url"])
            w_skip.writerow(["file_name"])
            w_none.writerow(["file_name"])

            # Batch loop
            num_batches = (total_range + BATCH_SIZE - 1) // BATCH_SIZE if total_range else 0
            processed_files = 0
            total_rows_main = 0
            total_titles_skipped = 0
            total_titles_noresolver = 0

            for batch_idx, batch_files in enumerate(chunked(sliced, BATCH_SIZE), start=1):
                print(f"\n[batch {batch_idx}/{num_batches}] Starting batch of {len(batch_files)} file(s)...", flush=True)

                batch_rows_main: List[Tuple[str, str]] = []
                batch_titles_skipped: List[str] = []
                batch_titles_noresolver: List[str] = []

                for offset_in_slice, title in enumerate(batch_files):
                    absolute_idx = base_index + processed_files + offset_in_slice

                    # Exclusion
                    if EXCLUDE_MPEG21_PDF and EXCLUDE_SUFFIX_RE.search(title):
                        print(f"[skip #{absolute_idx}] {title}  (excluded: ends with 'mpeg21).pdf')", flush=True)
                        batch_titles_skipped.append(title)
                        continue

                    # Fetch extlinks
                    try:
                        extlinks = get_extlinks_for_title(title, session)
                    except requests.HTTPError as e:
                        print(f"[error #{absolute_idx}] {title}  HTTP error: {e}", flush=True)
                        batch_titles_skipped.append(title)
                        continue
                    except Exception as e:
                        print(f"[error #{absolute_idx}] {title}  Error: {e}", flush=True)
                        batch_titles_skipped.append(title)
                        continue

                    # Filter + dedupe resolver links
                    kb_links = sorted({normalize_to_https(u) for u in extlinks if is_kb_resolver(u)})

                    if kb_links:
                        print(f"[ok   #{absolute_idx}] {title}  -> {len(kb_links)} resolver link(s):", flush=True)
                        for u in kb_links:
                            print(f"              - {u}", flush=True)
                            batch_rows_main.append((title, u))
                    else:
                        print(f"[ok   #{absolute_idx}] {title}  -> no resolver.kb.nl links", flush=True)
                        batch_titles_noresolver.append(title)

                    time.sleep(0.05)  # be polite

                # ---- Write this batch ----
                if batch_rows_main:
                    w_main.writerows(batch_rows_main); f_main.flush()
                if batch_titles_skipped:
                    for t in batch_titles_skipped:
                        w_skip.writerow([t])
                    f_skip.flush()
                if batch_titles_noresolver:
                    for t in batch_titles_noresolver:
                        w_none.writerow([t])
                    f_none.flush()

                processed_files += len(batch_files)
                total_rows_main += len(batch_rows_main)
                total_titles_skipped += len(batch_titles_skipped)
                total_titles_noresolver += len(batch_titles_noresolver)

                print(
                    f"[batch {batch_idx}/{num_batches}] Wrote: "
                    f"{len(batch_rows_main)} main row(s), "
                    f"{len(batch_titles_noresolver)} no-resolver title(s), "
                    f"{len(batch_titles_skipped)} non-processed title(s).",
                    flush=True,
                )
                print(
                    f"                         Totals so far -> "
                    f"main rows: {total_rows_main}, "
                    f"no-resolver titles: {total_titles_noresolver}, "
                    f"non-processed titles: {total_titles_skipped}. "
                    f"Processed files: {processed_files}/{total_range}.",
                    flush=True,
                )

                # ---- Pause after EVERY batch if configured ----
                if PAUSE_BETWEEN_BATCHES:
                    wait_for_enter(f"[pause] Batch {batch_idx}/{num_batches} complete. Press Enter to continue...")

        print(
            f"\n[done] Finished. Range processed: {total_range} file(s). "
            f"Main rows written: {total_rows_main}. "
            f"No-resolver titles: {total_titles_noresolver}. "
            f"Non-processed titles: {total_titles_skipped}.\n"
            f"Outputs:\n  - main:        {base}\n  - nonprocessed:{skipped_path}\n  - noresolver:  {noresolver_path}",
            flush=True,
        )

    except KeyboardInterrupt:
        print("\n[abort] Interrupted by user.", file=sys.stderr); sys.exit(130)
    except requests.HTTPError as e:
        print(f"HTTP error: {e}", file=sys.stderr); sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr); sys.exit(2)


if __name__ == "__main__":
    main()
