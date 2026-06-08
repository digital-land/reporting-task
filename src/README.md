# Reporting Scripts

This directory contains Python scripts that generate reporting datasets. Each script fetches data from various sources (APIs, CSV feeds, parquet files) and outputs a CSV file for reporting purposes.

## Scripts

### check_deleted_entities.py

Identifies entities that have been removed from datasets but were previously expected to be present.

**What it does:**

- Fetches expectations data to identify datasets with deleted entities
- Extracts entity IDs from the expectation details
- Enriches entity data by looking up names and references from parquet files (one file per dataset)
- Merges with organisation information for context
- Outputs a CSV with all required reporting columns

**Output:** `deleted_entities.csv`

**Run:** `python src/check_deleted_entities.py --output-dir <directory>` (or `python3` depending on your system setup)

---

### listed_building_end_date.py

Extracts listed building end dates associated with organisations.

**What it does:**

- Fetches listed-building and listed-building-outline datasets
- Merges them on the listed-building reference field to associate end dates with outlines
- Enriches with organisation names from the organisation dataset
- Outputs a CSV sorted by organisation name

**Output:** `listed-building-end-date.csv` with columns: `reference`, `end-date`, `organisation-entity`, `organisation`

**Run:** `python src/listed_building_end_date.py --output-dir <directory>` (or `python3` depending on your system setup)

---

### measure_odp_data_quality.py

Generates ODP data quality reporting outputs for provider and dataset coverage.

**What it does:**

- Builds ODP quality scores for each provider across key datasets
- Produces an LPA-by-dataset quality summary table
- Produces a dataset quality criteria detail table by provider
- Writes both reporting tables as CSV files

**Outputs:**

- `quality_ODP_dataset_scores_by_LPA.csv`
- `quality_ODP_dataset_quality_detail.csv`

**Run:** `python src/measure_odp_data_quality.py --output-dir <directory>` (or `python3` depending on your system setup)

---

### dataset_resource_vs_platform_report.py

Compares entity counts between the Platform and dataset_resource data for ODP datasets to identify potential data duplication.

**What it does:**

- Fetches reporting_historic_endpoints from Datasette (paginated JSON)
- Filters to 5 ODP datasets (article-4-direction-area, conservation-area, listed-building-outline, tree, tree-preservation-zone) and active endpoints, deduplicates on resource
- Fetches dataset_resource.csv for each dataset and merges on (dataset, resource)
- Aggregates entity/entry/line counts per LPA
- Fetches platform dataset CSVs and counts entities per organisation
- Compares platform entity counts against dataset_resource line counts via a ratio

**Outputs:**

- `dataset_resource_odp_detailed_counts.csv`
- `dataset_resource_vs_platform_odp_summary.csv`

**Run:** `python src/dataset_resource_vs_platform_report.py --output-dir <directory>` (or `python3` depending on your system setup)

---

### generate_plans_status_csv.py

Generates a plan status CSV summarising endpoint presence against expected plan dataset provisions for the local-plan collection.

**What it does:**

- Retrieves all organisations with a statutory provision to provide plan datasets (where `specification = "local-plan"`)
- Fetches endpoint status from the `reporting_latest_endpoints` table (paginated)
- Checks five pipelines per organisation: `local-plan`, `minerals-plan`, `plan-timetable`, `supplementary-plan`, `waste-plan`
- Rows with no matching endpoint are marked as `No endpoint added`

**Output:** `plan_status.csv`

**Run:** `python src/generate_plans_status_csv.py --output-dir <directory>` (or `python3` depending on your system setup)

---

### generate_plans_issues_csv.py

Generates a detailed issue-level CSV for Plans datasets, joining issue summaries with statutory provision data.

**What it does:**

- Retrieves all organisations with a statutory provision to provide plan datasets (where `specification = "local-plan"`)
- Fetches issue type summaries from `endpoint_dataset_issue_type_summary` for the five plan pipelines (paginated)
- Joins with `endpoint_dataset_summary` on both `endpoint` and `dataset` to retrieve endpoint status metadata
- Merges issues against provisions on `organisation`
- Outputs one row per organisation / pipeline / issue type / field combination

**Output:** `plan_issue.csv`

**Run:** `python src/generate_plans_issues_csv.py --output-dir <directory>` (or `python3` depending on your system setup)

---

## Adding New Scripts

When creating a new reporting script, please add a brief description to this README following the format above. Include:

- What the script does
- What data it outputs
- How to run it
