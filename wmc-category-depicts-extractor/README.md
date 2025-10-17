# Wikimedia Commons category Depicts (P180) extractor

*Lists all things depicted in all images in a Wikimedia Commons category* 

*Latest update*: 17 October 2025

## Introduction

This repo contains [a Python script](wmc-category-depicts-extractor.py) that lists everything depicted (via [Wikidata property `P180`](https://www.wikidata.org/wiki/Property:P180)) in all files within a given Wikimedia Commons category. It uses the Commons and Wikidata APIs, follows pagination, batches requests for efficiency, and outputs a CSV‑like file while also echoing a compact summary to the console/stdout.

**What it does**

* Fetches all files (namespace ns=6) in a Commons category and handles API continuation.
* Loads each file’s MediaInfo entity (`M-id`) and collects its `P180 (depicts)` statements.
* Looks up human‑readable labels for the depicted Wikidata items (`Q...`) in your preferred language with sensible fallbacks.
* Writes a delimited text file and prints a readable summary to the terminal.

## Quick examples

Below are illustrative examples of the script’s stdout lines for Commons [Category:Atlas de Wit 1698](https://commons.wikimedia.org/wiki/Category:Atlas_de_Wit_1698). These are examples with English P180-labels of what you’ll see, not ground‑truth metadata.

```
midURL^^^^title^^^^depicts_count^^^^depicts
https://commons.wikimedia.org/entity/M3116957^^^^File:Atlas de Wit 1698-pl057-Deventer-KB PPN 145205088.jpg^^^^6^^^^Deventer (https://www.wikidata.org/wiki/Q10001) --- sailboat (https://www.wikidata.org/wiki/Q1075310) --- IJssel (https://www.wikidata.org/wiki/Q217818) --- fortified town (https://www.wikidata.org/wiki/Q677678) --- windmill (https://www.wikidata.org/wiki/Q38720) --- city gate (https://www.wikidata.org/wiki/Q82117)
https://commons.wikimedia.org/entity/M11890942^^^^File:Atlas de Wit 1698-pl044-Utrecht-KB PPN 145205088.jpg^^^^1^^^^Utrecht (https://www.wikidata.org/wiki/Q39297398)
https://commons.wikimedia.org/entity/M23526495^^^^File:Plattegrond van Gorinchem - Gorinchem - 20080113 - RCE.jpg^^^^0^^^^
```

The corresponding CSV‑like file will contain the columns:

* `midURL` : the Commons MediaInfo identifier of the image file, rendered as URL
* `title` : the title/name of the file, including the `File:` prefix
* `depicts_count` : the number of depicted things in the file
* `depicts` (a `---` joined list like `Label (https://www.wikidata.org/wiki/Q123)`)

## How to use

### Prerequisites

* Python 3.9+
* `requests` library (`pip install requests`)

### Configure

Open the script and set the configuration block near the top:

```python
CATEGORY = "Category:Atlas_de_Wit_1698"  # Commons category title (with prefix)
LANG = "nl"  # Preferred label language (ISO 639‑1), eg. 'nl' for Dutch labels, 'en' for English etc.
LIMIT = 13  # Optional: cap number of files (for testing, set to None for all)
```

### Run

From the repository root:

```bash
python3 commons_depicts_extractor.py
```

The script will:

1. Collect file page IDs in `CATEGORY`.
2. Batch‑fetch their MediaInfo `M...` entities and extract `P180` QIDs.
3. Batch‑fetch labels for those QIDs in `LANG` (fallbacks: `en`, `de`, `fr`).
4. Write the output CSV file and print a summary to stdout.

### Output

* **Filename pattern:** `<SanitizedCategory>_thingsdepicted_<LANG>_<ddmmyyyy>.csv`
  Example: `AtlasdeWit1698_thingsdepicted_NL_16102025.csv`
* **CSV Columns:** `midURL, title, depicts_count, depicts`
* **Stdout format:** `midURL^^^^title^^^^depicts_count^^^^depicts`

#### Delimiter & escaping

To facilitate smooth imports of this CSV into Excel, the file uses a multi‑character field delimiter: `^^^^` (four carets). To keep each record on one line and avoid ambiguity, values are lightly escaped:

* Backslashes are doubled (`\` → `\\`).
* Any occurrence of the delimiter inside a value is prefixed with a backslash (e.g., `^^^^` → `\^^^^`).
* Newlines in values are normalized to spaces.

> **Tip:** If you open the file in tools that expect standard CSV, you may first need to replace the `^^^^` with a more common delimiter like `;` or a `TAB`. Make sure the Commons file name (`title`) will not be affected by this replacement.

## Notes on etiquette & performance

* The script uses a polite User‑Agent, retries with backoff, timeouts, and the Wikibase `maxlag` parameter.
* Large categories are handled via API continuation. You may optionally set `LIMIT` while testing.

## Licensing

<image src="../media/icon_cc0.png" width="100" hspace="10" align="right"/>

* **Code:** [CC0 1.0 public domain dedication](LICENSE). Feel free to reuse and adapt.
* **Attribution:** Inspired by item 47b in the KB article:
  [50 cool new things you can now do with KB’s collection highlights - Part 5, Reuse](https://kbnlwikimedia.github.io/KBCollectionHighlights/stories/Cool%20new%20things%20you%20can%20now%20do%20with%20the%20KB's%20collection%20highlights/Part%205%2C%20Reuse.html)

## Contact

<image src="../media/icon_kb2.png" width="200" hspace="10" align="right"/>

* Author: Olaf Janssen — Wikimedia coordinator [@KB, national library of the Netherlands](https://www.kb.nl)
* You can find his contact details on his [KB expert page](https://www.kb.nl/over-ons/experts/olaf-janssen) or via his [Wikimedia user page](https://commons.wikimedia.org/wiki/User:OlafJanssen).
* User agent reference: `Wikimedia Commons Category Depicts Extractor/1.0`



