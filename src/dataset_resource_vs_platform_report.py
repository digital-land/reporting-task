"""
Compare entity counts between the Platform and dataset_resource for ODP datasets.

Outputs:
- dataset_resource_odp_detailed_counts.csv: per-endpoint/resource entity counts
- dataset_resource_vs_platform_odp_summary.csv: platform vs dataset_resource comparison
"""

import os
import csv
import json
import argparse
from collections import defaultdict
from io import StringIO

from utils import get_http_session

csv.field_size_limit(10 * 1024 * 1024)

DATASETS = [
    "article-4-direction-area",
    "tree",
    "tree-preservation-zone",
    "listed-building-outline",
    "conservation-area",
]

DATASETTE_BASE = "https://datasette.planning.data.gov.uk"
FILES_BASE = "https://files.planning.data.gov.uk"


def _to_float(val):
    try:
        return float(val) if val not in ("", None) else 0.0
    except (ValueError, TypeError):
        return 0.0


def _to_int(val):
    try:
        return int(val) if val not in ("", None) else 0
    except (ValueError, TypeError):
        return 0


def write_csv(rows, filepath):
    if not rows:
        print(f"Warning: no data to write to {filepath}")
        return
    fieldnames = list(rows[0].keys())
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Saved {len(rows)} rows to {filepath}")


def fetch_historic_endpoints(session):
    """Fetch all rows from reporting_historic_endpoints using rowid pagination."""
    all_rows = []
    last_rowid = 0

    while True:
        url = f"{DATASETTE_BASE}/performance/reporting_historic_endpoints.json?_shape=array&_size=max"
        if last_rowid > 0:
            url += f"&rowid__gt={last_rowid}"

        response = session.get(url, timeout=120)
        response.raise_for_status()
        rows = response.json()

        if not rows:
            break

        all_rows.extend(rows)
        last_rowid = rows[-1]["rowid"]
        print(f"Fetched {len(all_rows)} rows...")

        if len(rows) < 1000:
            break

    print(f"Total historic endpoints: {len(all_rows)} rows")
    return all_rows


def filter_and_deduplicate(rows):
    """Filter to ODP datasets and active endpoints, deduplicate on (resource, dataset)."""
    seen = set()
    filtered = []
    for row in rows:
        if row["dataset"] not in DATASETS:
            continue
        if row["resource_end_date"] != "":
            continue
        key = (row["resource"], row["dataset"])
        if key in seen:
            continue
        seen.add(key)
        filtered.append(row)
    print(f"After filter/dedup: {len(filtered)} rows")
    return filtered


def fetch_dataset_resources(session):
    """Fetch dataset_resource.csv for each dataset; return dict keyed by (dataset, resource)."""
    lookup = {}
    for dataset in DATASETS:
        url = f"{DATASETTE_BASE}/{dataset}/dataset_resource.csv?_stream=on"
        response = session.get(url, timeout=120)
        response.raise_for_status()
        reader = csv.DictReader(StringIO(response.text))
        count = 0
        for row in reader:
            key = (row["dataset"], row["resource"])
            lookup[key] = {
                "entity_count": _to_float(row.get("entity_count", "")),
                "entry_count": _to_int(row.get("entry_count", "")),
                "line_count": _to_int(row.get("line_count", "")),
            }
            count += 1
        print(f"Fetched dataset_resource for {dataset}: {count} resources")
    return lookup


def merge_historic_with_resources(report_he, dr_lookup):
    """Inner join report_he with dataset_resource on (dataset, resource)."""
    merged = []
    for row in report_he:
        key = (row["dataset"], row["resource"])
        if key in dr_lookup:
            merged_row = dict(row)
            merged_row.update(dr_lookup[key])
            merged.append(merged_row)
    print(f"After merge with dataset_resource: {len(merged)} rows")
    return merged


def aggregate_counts(merged):
    """
    Returns:
        summary_rows: grouped by (dataset, name, organisation) with sums + counts
        detailed_rows: grouped by (dataset, name, organisation, endpoint, endpoint_entry_date, resource, resource_start_date)
    """
    summary = defaultdict(lambda: {
        "entity_count": 0.0, "entry_count": 0, "line_count": 0,
        "endpoint_count": 0, "resource_count": 0,
    })

    for row in merged:
        key = (row["dataset"], row["name"], row["organisation"])
        summary[key]["entity_count"] += row["entity_count"]
        summary[key]["entry_count"] += row["entry_count"]
        summary[key]["line_count"] += row["line_count"]
        summary[key]["endpoint_count"] += 1
        summary[key]["resource_count"] += 1

    summary_rows = []
    for (dataset, name, organisation), vals in summary.items():
        summary_rows.append({
            "dataset": dataset,
            "name": name,
            "organisation": organisation,
            **vals,
        })

    detailed = defaultdict(lambda: {"entity_count": 0.0, "entry_count": 0, "line_count": 0})
    for row in merged:
        key = (
            row["dataset"], row["name"], row["organisation"],
            row["endpoint"], row.get("endpoint_entry_date", ""),
            row["resource"], row.get("resource_start_date", ""),
        )
        detailed[key]["entity_count"] += row["entity_count"]
        detailed[key]["entry_count"] += row["entry_count"]
        detailed[key]["line_count"] += row["line_count"]

    detailed_rows = []
    for (dataset, name, org, endpoint, ep_date, resource, res_date), vals in detailed.items():
        detailed_rows.append({
            "dataset": dataset,
            "name": name,
            "organisation": org,
            "endpoint": endpoint,
            "endpoint_entry_date": ep_date,
            "resource": resource,
            "resource_start_date": res_date,
            **vals,
        })

    detailed_rows.sort(key=lambda r: (r["dataset"], r["name"]))

    return summary_rows, detailed_rows


def fetch_organisation_lookup(session):
    """Fetch organisation CSV; return dict keyed by entity -> {organisation-code, organisation-name}."""
    url = f"{DATASETTE_BASE}/digital-land/organisation.csv?_stream=on"
    response = session.get(url, timeout=120)
    response.raise_for_status()
    reader = csv.DictReader(StringIO(response.text))
    lookup = {}
    for row in reader:
        lookup[row["entity"]] = {
            "organisation-code": row["organisation"],
            "organisation-name": row["name"],
        }
    print(f"Fetched {len(lookup)} organisations")
    return lookup


def fetch_platform_data_and_count(session, org_lookup):
    """
    For each dataset, fetch the platform CSV and count entities per (dataset, name, organisation).
    """
    counts = defaultdict(int)

    for dataset in DATASETS:
        url = f"{FILES_BASE}/dataset/{dataset}.csv"
        print(f"Fetching platform data for {dataset}...")
        response = session.get(url, timeout=300, stream=True)
        response.raise_for_status()

        reader = csv.DictReader(response.iter_lines(decode_unicode=True))
        row_count = 0
        for row in reader:
            org_entity = row.get("organisation-entity", "")
            org_info = org_lookup.get(org_entity, {})
            org_name = org_info.get("organisation-name", "")
            org_code = org_info.get("organisation-code", "")
            counts[(dataset, org_name, org_code)] += 1
            row_count += 1
        print(f"  {dataset}: {row_count} entities")

    result = []
    for (dataset, name, organisation), count in counts.items():
        result.append({
            "dataset": dataset,
            "name": name,
            "organisation": organisation,
            "platform_entity_count": count,
        })
    return result


def outer_merge_and_compute_ratio(platform_counts, summary_counts):
    """Outer merge platform entity counts with dataset_resource summary; compute ratio."""
    platform_by_key = {}
    for row in platform_counts:
        key = (row["dataset"], row["name"], row["organisation"])
        platform_by_key[key] = row["platform_entity_count"]

    summary_by_key = {}
    for row in summary_counts:
        key = (row["dataset"], row["name"], row["organisation"])
        summary_by_key[key] = row

    all_keys = set(platform_by_key.keys()) | set(summary_by_key.keys())

    result = []
    for key in sorted(all_keys):
        dataset, name, organisation = key
        platform_count = platform_by_key.get(key)
        summary = summary_by_key.get(key, {})

        dr_entity_count = summary.get("entity_count")
        dr_entry_count = summary.get("entry_count")
        dr_line_count = summary.get("line_count")
        dr_endpoint_count = summary.get("endpoint_count")
        dr_resource_count = summary.get("resource_count")

        ratio = ""
        if platform_count is not None and dr_line_count and dr_line_count > 0:
            ratio = platform_count / dr_line_count

        result.append({
            "dataset": dataset,
            "name": name,
            "organisation": organisation,
            "platform_entity_count": platform_count if platform_count is not None else "",
            "dataset_resource_entity_count": dr_entity_count if dr_entity_count is not None else "",
            "dataset_resource_entry_count": dr_entry_count if dr_entry_count is not None else "",
            "dataset_resource_line_count": dr_line_count if dr_line_count is not None else "",
            "dataset_resource_endpoint_count": dr_endpoint_count if dr_endpoint_count is not None else "",
            "dataset_resource_resource_count": dr_resource_count if dr_resource_count is not None else "",
            "platform_divided_by_dr_line_count": ratio,
        })
    return result


def main(output_dir):
    session = get_http_session()

    # Fetch and filter historic endpoints
    all_rows = fetch_historic_endpoints(session)
    report_he = filter_and_deduplicate(all_rows)

    # Fetch dataset_resource data and merge
    dr_lookup = fetch_dataset_resources(session)
    merged = merge_historic_with_resources(report_he, dr_lookup)

    # Aggregate counts
    summary_counts, detailed_counts = aggregate_counts(merged)

    # Fetch platform data and count entities
    org_lookup = fetch_organisation_lookup(session)
    platform_counts = fetch_platform_data_and_count(session, org_lookup)

    # Merge and compute ratio
    final_summary = outer_merge_and_compute_ratio(platform_counts, summary_counts)

    # Write outputs
    os.makedirs(output_dir, exist_ok=True)
    write_csv(detailed_counts, os.path.join(output_dir, "dataset_resource_odp_detailed_counts.csv"))
    write_csv(final_summary, os.path.join(output_dir, "dataset_resource_vs_platform_odp_summary.csv"))


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compare entity counts between Platform and dataset_resource for ODP datasets"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Directory to save exported CSVs",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(args.output_dir)
