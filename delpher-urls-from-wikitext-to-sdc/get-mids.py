"""
Media Commons M-ID Extractor
----------------------------

This script processes an Excel file containing Wikimedia Commons file URLs.
For each file, it extracts the unique M-ID (media identifier) using the
Wikimedia Commons API. The M-ID is added as a new column in the Excel file.

Additionally:
- Errors (e.g., failed API lookups, network issues) are logged in a CSV file.
- If an M-ID cannot be found for a URL, "NOT FOUND" is explicitly added.
- Ensures that every row in the input has a corresponding output entry.

Inputs:
    - An Excel file with a column 'FileURL' (Wikimedia Commons file URLs)

Outputs:
    - An updated Excel file with an added 'FileMid' column
    - A CSV file logging any errors or failed lookups

Author: ChatGPT, prompted by Olaf Janssen, Koninklijke Bibliotheek, national library of the Netherlands
Latest update: 2023-10-04
License: CC0
"""

import csv
import urllib.parse
from typing import Optional

import pandas as pd
import requests


def get_commons_m_id(file_url: str) -> Optional[str]:
    """
    Given a Wikimedia Commons file URL, this function extracts the file title and
    queries the Wikimedia Commons API to retrieve the corresponding M-ID.

    Args:
        file_url (str): A URL to a file on Wikimedia Commons.

    Returns:
        Optional[str]: The M-ID (e.g., "M123456") if found, otherwise "NOT FOUND".
    """
    try:
        # Extract file title from URL
        file_name = urllib.parse.unquote(file_url.split("/File:")[1])
        title = f"File:{file_name}"

        # Build API request
        endpoint = "https://commons.wikimedia.org/w/api.php"
        params = {
            "action": "query",
            "titles": title,
            "prop": "info",
            "format": "json"
        }

        response = requests.get(endpoint, params=params, timeout=10)
        response.raise_for_status()  # Raise an error for bad status codes
        data = response.json()

        # Parse response
        page = next(iter(data["query"]["pages"].values()))
        if "pageid" in page:
            return f"M{page['pageid']}"
        else:
            return "NOT FOUND"

    except Exception as e:
        # If any error occurs, log it and return "NOT FOUND"
        error_log.append((file_url, str(e)))
        return "NOT FOUND"

def log_errors_to_csv(errors: list[tuple[str, str]], filename: str = "errors.csv") -> None:
    """
    Write a list of errors to a CSV file.

    Args:
        errors (list): List of (file_url, error_message) tuples.
        filename (str): Output CSV filename.
    """
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["FileURL", "Error"])
        writer.writerows(errors)

def main() -> None:
    """
    Main function to load Excel file, retrieve M-IDs, update the DataFrame,
    and write results to a new Excel file while logging any errors.
    """
    input_file = "Media_from_Delpher-Extracted_copyright_templates-09042025-cleaned-processed.xlsx"
    output_file = "Media_with_MIDs.xlsx"

    # Load the Excel file
    df = pd.read_excel(input_file)

    # Get M-IDs and store them
    df.insert(1, "FileMid", df["FileURL"].apply(get_commons_m_id))

    # Save output Excel
    df.to_excel(output_file, index=False)
    print(f"✅ File saved as: {output_file}")

    # Save errors if any
    if error_log:
        log_errors_to_csv(error_log)
        print(f"⚠️ Errors logged to: errors.csv")

# Error log list (global for simplicity)
error_log: list[tuple[str, str]] = []

# Run the script
if __name__ == "__main__":
    main()
