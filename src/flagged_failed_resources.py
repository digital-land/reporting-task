"""
Script to classify failed resources from Digital Land endpoints,
grouping them by failure type (e.g. document links, WFS errors, auth issues),
and providing suggested resolution details and retirement recommendations.

Usage:
    python classify_failed_resources.py --output-dir ./output
"""

import os
import argparse
import pandas as pd
import requests
from io import StringIO

def is_pdf_url(url):
    """Check if URL points to a PDF by sending a HEAD request and inspecting Content-Type."""
    try:
        response = requests.head(url, allow_redirects=True, timeout=5)
        content_type = response.headers.get("Content-Type", "").lower()
        return "application/pdf" in content_type
    except:
        return False

def fetch_text_content(url):
    """Fetch and return lowercased plain text content of a URL (used for WFS error detection)."""
    try:
        r = requests.get(url, timeout=8)
        if r.status_code == 200:
            return r.text.lower()
        return ""
    except:
        return ""

def classify_issue(row):
    """
    Classify each row by examining the endpoint URL and exception content.
    Returns a tuple (group, details) with the error category and advice.
    """
    url = str(row.get("endpoint_url") or "").strip()
    exc = str(row.get("exception") or "").lower()

    ext_map = {
        ".pdf": "pdf", ".doc": "doc", ".docx": "docx",
        ".xls": "xls", ".xlsx": "xlsx", ".ppt": "ppt", ".pptx": "pptx"
    }

    if ".zip" in url.lower():
        return ("zipped file", "file needs to be unzipped first")

    for ext, label in ext_map.items():
        if ext in url.lower():
            if label == "xls":
                return ("XLS files", "Possible issues with opening xls file")
            return ("active document links", f"{label} file in URL")

    for label in ext_map.values():
        if f"-{label}" in url.lower():
            if label == "xls":
                return ("XLS files", "Possible issues with opening xls file")
            return ("active document links", f"{label} inferred from slug")

    if is_pdf_url(url):
        return ("active document links", "confirmed via Content-Type check")

    try:
        r = requests.get(url, timeout=8)
        if r.headers.get("Content-Type", "").lower().startswith("application/json"):
            json_body = r.json()
            error = json_body.get("error", {})
            if str(error.get("code", "")).strip() == "499" or "token" in error.get("message", "").lower():
                return ("auth error", "Token or authentication required (JSON error response)")
        elif "token required" in r.text.lower():
            return ("auth error", "Token or authentication required (text body)")
    except:
        pass

    if "getfeature" in url.lower() or "wfs" in url.lower():
        text = fetch_text_content(url)
        if "serviceexception" in text and "feature" in text:
            return ("wfs error", "Likely invalid typeName - check WFS GetCapabilities")

    return (None, "")

def main(output_dir):
    # Load failed resources
    csv_url = (
        "https://datasette.planning.data.gov.uk/digital-land.csv?"
        "sql=select+dataset%2C+elapsed%2C+r.end_date%2C+r.start_date%2C+exception%2C+r.resource%2C+status+"
        "from+converted_resource+cr+inner+join+resource+r+on+cr.resource%3Dr.resource+"
        "where+status%3D'failed'+and+(r.end_date+is+null+or+r.end_date%3D'')+"
        "order+by+r.start_date+desc+limit+1000"
    )
    df_failed = pd.read_csv(StringIO(requests.get(csv_url).text))

    # Supporting metadata
    df_endpoint = pd.read_csv("https://datasette.planning.data.gov.uk/digital-land/endpoint.csv?_stream=on")[["endpoint", "endpoint_url"]]
    df_resource_endpoint = pd.read_csv("https://datasette.planning.data.gov.uk/digital-land/resource_endpoint.csv?_stream=on")[["endpoint", "resource"]]
    df_source_raw = pd.read_csv("https://datasette.planning.data.gov.uk/digital-land/source.csv?_stream=on")
    df_source_raw["organisation_ref"] = df_source_raw["organisation"].str.replace(r"^.*?:", "", regex=True).astype(str)
    df_source = df_source_raw[["endpoint", "source", "collection", "organisation_ref"]]

    # Join metadata
    df_resource_endpoint = df_resource_endpoint.drop_duplicates(subset="resource", keep="last")
    df_source = df_source.drop_duplicates(subset="endpoint", keep="last")
    df = df_failed.merge(df_resource_endpoint, on="resource", how="left")
    df = df.merge(df_endpoint, on="endpoint", how="left")
    df = df.merge(df_source, on="endpoint", how="left")

    # Classify
    df[["group", "details"]] = df.apply(lambda row: pd.Series(classify_issue(row)), axis=1)

    # Manual patches
    force_pdf_urls = [
        "https://www.bolton.gov.uk/downloads/file/3212/infrastructure-funding-statement",
        "https://www.bolton.gov.uk/downloads/file/4045/infrastructure-funding-statement-2019-20"
    ]
    df.loc[df["endpoint_url"].isin(force_pdf_urls), "group"] = "active document links"
    df.loc[df["endpoint_url"].isin(force_pdf_urls), "details"] = "manually flagged as pdf"

    # Retirement logic
    df["recommend_retirement"] = df["group"].apply(lambda g: "yes" if g == "active document links" else "no")

    # Output
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "flagged_failed_resources.csv")
    df_out = df[[
        "resource", "source", "collection", "endpoint_url", "group", "details", "recommend_retirement"
    ]]
    df_out.to_csv(output_path, index=False)
    print(f"Saved {len(df_out)} rows to {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Classify failed resources and flag potential retirement.")
    parser.add_argument("--output-dir", type=str, required=True, help="Directory to save the output CSV")
    args = parser.parse_args()
    main(args.output_dir)
