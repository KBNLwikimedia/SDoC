# Add Structured Data to Commons files from an Excel sheet

This tool reads an Excel sheet and adds **Wikidata QIDs** to **Structured Data on Commons (SDoC)** for one or more properties (e.g., **P180 – depicts**, **P170 – creator**) on Wikimedia Commons files.

It’s designed for IDE use (PyCharm/VS Code): configure the `CONFIG` dict in the script and run.

See also: [https://commons.wikimedia.org/wiki/Commons:WriteSDoCfromExcel](https://commons.wikimedia.org/wiki/Commons:WriteSDoCfromExcel)

*Latest update*: 17 October 2025

---

## What this script does

* Reads an Excel file (sheet and column names are configurable).
* For each row and each mapped property:

  1. Determines the target file’s **M-ID** (from `CommonsMid`, or—if missing—automatically resolves it from `CommonsFile`, which can be a full URL, `File:` title, or bare filename).
  2. Validates inputs (M-ID, property id, QID).
  3. Fetches the file’s current SDC values for the property.
  4. **Skips duplicates** (won’t add a QID that’s already present).
  5. Adds a new claim via the Commons API (**unless** running in `DRY_RUN` mode).
* Writes a **CSV log** with one line per attempt: `ADDED`, `SKIPPED_DUPLICATE`, `SKIPPED_INVALID`, or `ERROR`.

Example: add [`Q284865`](https://www.wikidata.org/wiki/Q284865) to [`P180`](https://www.wikidata.org/wiki/Property:P180) (depicts) for one or many Commons files listed in your Excel.

---

## Requirements

* **Python 3.10+**
* Libraries:

  * `pandas`
  * `requests`
  * `urllib3` (for `Retry`)
  * `python-dotenv` *(optional, for loading credentials from `.env`)*

Install:

```bash
pip install pandas requests urllib3 python-dotenv
```

---

## Configuration (in the script)

Open the script and edit the top-level `CONFIG` dictionary:

```python
CONFIG = {
    "EXCEL_PATH": "p180-inputfile-for-repo.xlsx",
    "SHEET_NAME": "CommonsFilesP180Qids",

    # Column names in your Excel:
    "FILE_COLUMN": "CommonsFile",   # required; URL, File:Title or bare filename
    "MID_COLUMN": "CommonsMid",     # optional; if missing/empty, script resolves from CommonsFile

    # Choose either:
    "PROP_MAP": [("P180", "QidDepicts")],  # multiple properties supported, e.g. [("P180","QidDepicts"), ("P170","QidCreator")]
    # ...or a single property:
    "PROPERTY": None,
    "QID_COLUMN": None,

    # Credentials (recommended via .env; see below)
    "DOTENV_PATH": ".env", # Adapt .env / .env-example file for your own Wikimedia login credentials and user agent
    "USER": None,
    "PASSWORD": None,

    # HTTP
    "USER_AGENT": "KB-Excel-SDoC-Writer/1.0 (User:OlafJanssen; olaf.janssen@kb.nl)",

    # Behavior
    "DRY_RUN": True,      # True = simulate only; False = actually write to Commons
    "PAUSE": 0.5,         # seconds between successful edits
    "MAX_EDITS": None,    # e.g., 100; None = no cap
    "VERBOSE": False,

    # Logging (timestamp placeholder is supported)
    "LOG_CSV": "logs/write_sdoc_log_{timestamp}.csv",
}
```

### Excel expectations

* **`CommonsFile`** (required): may be

  * a full Commons URL, e.g. `https://commons.wikimedia.org/wiki/File:Example.jpg`
  * a `File:` title, e.g. `File:Example.jpg`
  * a bare filename, e.g. `Example.jpg`
* **`CommonsMid`** (optional): an `M`-ID like `M123456`. If empty or the column is missing, the script automatically resolves the M-ID from `CommonsFile`.
* **QID columns**: for each property in `PROP_MAP`, provide a column with QIDs (e.g. `QidDepicts`).

> Multiple QIDs for the same file/property are handled as separate rows.

---

## Credentials (.env)

Using a bot password is recommended. Create a fresh `.env` - or adapt the provided `.env-example` file next to the script:

```env
COMMONS_USER="YourWikiUsername"
COMMONS_PASS="YourWikiPassword"
COMMONS_USER_AGENT="KB-Excel-SDoC-Writer/1.0 (User:YourWikiUsername; your.email@example.org)"
```
The script loads this automatically (via `python-dotenv`) if `CONFIG["DOTENV_PATH"]` points to it.
If you prefer, set `CONFIG["USER"]` and `CONFIG["PASSWORD"]` directly in the script (not recommended).

> Add your `.env` to your `.gitignore` to avoid committing secrets.

---

## Running

1. Set `DRY_RUN: True` to simulate and review output/logs.

2. Run the script from your IDE (PyCharm/VS Code) or from the shell:

   ```bash
   python WriteSDoCfromExcel.py
   ```

3. Inspect the console output and the CSV in `logs/`.

When satisfied, set `DRY_RUN: False` to perform real edits. Consider keeping a nonzero `PAUSE` and (optionally) a `MAX_EDITS` cap.

---

## Logging

The log CSV contains:

```
timestamp, commons_mid, commons_file, property, qid, action, details, edit_id, dry_run
```

* **ADDED** – claim successfully added (`edit_id` if available).
* **SKIPPED_DUPLICATE** – QID already present for the property.
* **SKIPPED_INVALID** – missing/invalid inputs (e.g., couldn’t resolve M-ID).
* **ERROR** – network/API or other exception (message in `details`).
* **WOULD_ADD** – what would be added in `DRY_RUN` mode.

Console messages include full Commons and Wikidata URLs for clarity.

---

## Notes & etiquette

* The script uses retries for transient HTTP errors and a `maxlag` hint when writing.
* Be courteous to Commons: keep `PAUSE > 0`, use a bot account, and consider small batches (`MAX_EDITS`) for testing.
* The script only adds **wikibase-item** values (QIDs). Other datatypes would need code changes.

---

## Troubleshooting

* **“Missing required column(s)”** – Update `FILE_COLUMN` / QID column names in `CONFIG` or your sheet.
* **Can’t resolve M-ID** – Ensure `CommonsFile` cells are non-empty and valid; URLs, `File:` titles, and bare filenames are accepted.
* **Login failed** – Check `.env` values or `USER`/`PASSWORD`; without login, edits (if `DRY_RUN=False`) may be attributed to your IP.
* **Duplicate skipped** – The file already has that QID for the property; this is expected de-duplication behavior.

---

## Contact & Credits

<image src="../media/icon_kb2.png" width="200" hspace="10" align="right"/>

* Author: Olaf Janssen — Wikimedia coordinator [@KB, national library of the Netherlands](https://www.kb.nl)
* You can find his contact details on his [KB expert page](https://www.kb.nl/over-ons/experts/olaf-janssen) or via his [Wikimedia user page](https://commons.wikimedia.org/wiki/User:OlafJanssen).
* Based on/thanks to original code by [User:Multichill](https://commons.wikimedia.org/wiki/User:Multichill) and the Wikimedia Commons community.

---

## Licensing

<image src="../media/icon_cc0.png" width="100" hspace="10" align="right"/>

Released into the public domain under [CC0 1.0 public domain dedication](LICENSE). Feel free to reuse and adapt. Attribution *(KB, National Library of the Netherlands)* is appreciated but not required.

