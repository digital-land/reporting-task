# Reporting Scripts

This directory contains Python scripts that generate reporting datasets. Each script fetches data from various sources (APIs, CSV feeds, parquet files) and outputs a CSV file for reporting purposes.

Scripts are listed alphabetically below. `utils.py` is not a standalone script — it's a shared module (`get_http_session`, `read_csv_with_retry`) imported by many of the scripts below to add retry handling around HTTP/CSV requests.

## Downloading Generated Reports

All scripts in this directory are run automatically every night, and their CSV outputs are published at:

```text
https://files.planning.data.gov.uk/reporting/<filename>.csv
```

just swap in the filename from the script's **Output(s)** entry below — e.g. `deleted_entities.csv` (from `check_deleted_entities.py`) is published at `https://files.planning.data.gov.uk/reporting/deleted_entities.csv`.

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

### dataset_resource_vs_platform_report.py

Compares entity counts between the Platform and dataset_resource data for ODP datasets to identify potential data duplication.

**What it does:**

- Fetches reporting_historic_endpoints from Datasette (paginated JSON)
- Filters to 5 ODP datasets (article-4-direction-area, conservation-area, listed-building-outline, tree, tree-preservation-zone) and active endpoints, deduplicates on resource
- Fetches dataset_resource.csv for each dataset and merges on (dataset, resource)
- Aggregates entity/entry/line counts per LPA
- Fetches platform dataset CSVs and counts entities per organisation, excluding entities with quality "some"
- Compares platform entity counts against dataset_resource line counts via a ratio

**Outputs:**

- `dataset_resource_odp_detailed_counts.csv`
- `dataset_resource_vs_platform_odp_summary.csv`

**Run:** `python src/dataset_resource_vs_platform_report.py --output-dir <directory>` (or `python3` depending on your system setup)

---

### duplicate_geometry_expectations.py

Extracts and enriches duplicate-geometry entity matches flagged by expectation checks, with organisation and ODP context.

**What it does:**

- Fetches the expectations table and filters to `duplicate_geometry_check` operations
- Parses complete/single geometry matches out of the expectation details
- Enriches each matched entity pair (A/B) with name, dates, geometry and organisation from the relevant dataset's parquet file
- Looks up organisation references via each dataset's pipeline lookup file, and flags whether the two matched entities belong to the same organisation
- Flags whether entity B's organisation is part of the open-digital-planning (ODP) project

**Outputs:**

- `duplicate_entity_expectation.csv` (geometry columns dropped, for reporting)
- `duplicate_entity_expectation_geographies.csv` (geometry columns retained, for geospatial analysis)

**Run:** `python src/duplicate_geometry_expectations.py --output-dir <directory>` (or `python3` depending on your system setup)

---

### endpoint_dataset_issue_type_summary.py

Exports the full `endpoint_dataset_issue_type_summary` Datasette performance table as-is.

**What it does:**

- Streams the complete `endpoint_dataset_issue_type_summary` table from the performance Datasette database

**Output:** `endpoint_dataset_issue_type_summary.csv`

**Run:** `python src/endpoint_dataset_issue_type_summary.py --output-dir <directory>` (or `python3` depending on your system setup)

---

### endpoints_missing_doc_urls.py

Reports on endpoints missing a documentation URL, with summary statistics printed to the console.

**What it does:**

- Fetches endpoint/source/organisation metadata via a paginated SQL query against Datasette
- Flags rows where `documentation_url` is blank, and whether the endpoint is active (no `end_date`)
- Prints summary stats to the console: total/percent missing, top affected pipelines, active vs ended counts, most recent missing entry
- Saves all endpoint rows, with the missing-documentation flag, to CSV

**Output:** `all_endpoints_and_documentation_urls.csv`

**Run:** `python src/endpoints_missing_doc_urls.py --output-dir <directory>` (or `python3` depending on your system setup)

---

### flag_endpoints_no_provison.py

Identifies active endpoints that don't correspond to any expected dataset provision.

**What it does:**

- Fetches endpoint, source, organisation, resource and provision tables from Datasette
- Joins endpoints through source/resource to determine the dataset each endpoint supplies
- Compares against the provision table to find endpoint/organisation/dataset combinations with no matching provision
- Splits the missing rows into PDF and non-PDF endpoint URLs, saving both

**Outputs:**

- `flag_endpoints_no_provision.csv` (all endpoints missing provision)
- `flag_endpoints_pdf_only.csv` (PDF-only subset)

**Run:** `python src/flag_endpoints_no_provison.py --output-dir <directory>` (or `python3` depending on your system setup)

---

### flagged_failed_resources.py

Classifies recently failed resource conversions by likely cause and flags candidates for endpoint retirement.

**What it does:**

- Queries the most recent 1000 failed `converted_resource` rows (active resources only) from Datasette
- Joins in endpoint, source and collection metadata
- Classifies each failure by inspecting the endpoint URL and exception text: zipped files, document formats (pdf/doc/xls etc., including a live Content-Type check), auth/token errors, and WFS `ServiceException` errors
- Applies a small manual override list for known PDF endpoints
- Recommends retirement for anything classified as an active document link

**Output:** `flagged_failed_resources.csv`

**Run:** `python src/flagged_failed_resources.py --output-dir <directory>` (or `python3` depending on your system setup)

---

### generate_odp_conformance_csv.py

Calculates field-level conformance scores for ODP cohort organisations against the dataset specification.

**What it does:**

- Downloads the digital-land specification (`specification.csv`) into `--specification-dir` if not already present locally
- Retrieves expected provisions for ODP cohorts, endpoint/resource field-mapping summaries, and issue counts from Datasette (paginated)
- Filters mapped/unmapped fields and issues down to only those fields defined in the specification for each dataset
- Computes, per organisation/dataset/endpoint/resource: fields supplied, fields matched, and error-free fields (as counts and percentages)
- Filters output rows to ODP Track 1-4 cohorts only

**Output:** `odp_conformance.csv`

**Run:** `python src/generate_odp_conformance_csv.py --output-dir <directory> --specification-dir <directory>` (or `python3` depending on your system setup)

---

### generate_odp_issues_csv.py

Generates a detailed issue-level CSV for ODP spatial and document datasets, joined against expected provisions.

**What it does:**

- Retrieves expected ODP provisions (organisation, cohort, cohort start date)
- Fetches `endpoint_dataset_issue_type_summary`, joined with endpoint status/exception details, paginated, across all ODP spatial and document datasets
- Inner-joins issues to provisions on organisation and cohort

**Output:** `odp_issue.csv`

**Run:** `python src/generate_odp_issues_csv.py --output-dir <directory>` (or `python3` depending on your system setup)

---

### generate_odp_status_csv.py

Generates an ODP status CSV showing, per organisation and pipeline, whether an expected endpoint exists and its current status.

**What it does:**

- Retrieves expected ODP provisions and all endpoint reporting rows from `reporting_latest_endpoints` (paginated)
- For each provisioned organisation, checks every pipeline under the article-4-direction, conservation-area, listed-building and tree-preservation-order collections
- Emits one row per matching endpoint, or a `No endpoint added` row where none exists

**Output:** `odp_status.csv`

**Run:** `python src/generate_odp_status_csv.py --output-dir <directory>` (or `python3` depending on your system setup)

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

### logs_by_week.py

Summarises endpoint request status codes (200 vs failed) by week over the last 6 months.

**What it does:**

- Runs a SQL query against the `log` table in Datasette, grouping request counts by week and status (`200` vs `FAIL`)

**Output:** `logs_by_week.csv`

**Run:** `python src/logs_by_week.py --output-dir <directory>` (or `python3` depending on your system setup)

---

### measure_odp_mandated_data_quality.py

Generates ODP + mandated dataset quality reporting outputs for provider and dataset coverage.

**What it does:**

- Builds quality scores for each provider across ODP datasets plus mandated datasets (statutory, or "encouraged" specifically for LPAs - computed live from `provision_rule` rather than hardcoded)
- Determines authoritative sourcing from each dataset's own entity-level `quality` signal (rather than a geospatial join)
- Combines authoritative status with issue severity into a 0-6 quality level (authoritative axis x rung axis), plus criteria pass/fail detail
- Produces an LPA-by-dataset quality summary table covering ODP + mandated dataset columns - organisations with no ODP provision of their own (e.g. a mandated-dataset-only provider) get a blank `cohort`/`start_date`
- Produces a dataset quality criteria detail table by provider
- Writes both reporting tables as CSV files

**Outputs:**

- `quality_ODP_mandated_dataset_scores_by_LPA.csv`
- `quality_ODP_mandated_dataset_quality_detail.csv`

**Run:** `python src/measure_odp_mandated_data_quality.py --output-dir <directory>` (or `python3` depending on your system setup)

---

### measure_single_source_data_quality.py

Generates a data quality detail report for "single source" datasets - everything that isn't ODP-scoped or mandated (see measure_odp_mandated_data_quality.py, which covers those with slightly different checks).

**What it does:**

- Determines single-source pipelines as everything in `provision_rule` that isn't ODP or mandated
- Scores each provider's data on the same 0-6 scale (authoritative axis x rung axis) as the ODP/mandated report
- Applies a staleness cap specific to single-source datasets: data older than 365 days can't score above "usable", since these datasets have no alternative source to cross-check freshness against
- Produces a dataset quality criteria detail table by provider
- Writes the detail table as a CSV file

**Output:** `quality_single_source_dataset_quality_detail.csv`

**Run:** `python src/measure_single_source_data_quality.py --output-dir <directory>` (or `python3` depending on your system setup)

---

### monitoring_active_endpoints_ended_orgs.py

Flags active endpoints that belong to organisations which have since ended.

**What it does:**

- Fetches organisations with an `end_date` set (ended organisations)
- Fetches active endpoints (no `endpoint_end_date`) from `reporting_historic_endpoints`
- Merges the two on organisation code to find active endpoints still attached to ended organisations

**Output:** `ended_orgs_active_endpoints.csv`

**Run:** `python src/monitoring_active_endpoints_ended_orgs.py --output-dir <directory>` (or `python3` depending on your system setup)

---

### monitoring_entities_ended_orgs.py

Flags entities that belong to organisations which have since ended, across all registered datasets.

**What it does:**

- Discovers all registered dataset slugs, then probes and downloads each dataset's `entity.csv` where available
- Fetches organisations with an `end_date` set (ended organisations)
- Merges entities against ended organisations on `organisation_entity`

**Output:** `entities_with_ended_orgs.csv`

**Run:** `python src/monitoring_entities_ended_orgs.py --output-dir <directory>` (or `python3` depending on your system setup)

---

### operational_issues.py

Counts operational issues logged per day over the last 6 months.

**What it does:**

- Runs a SQL query against the `operational_issue` table in Datasette, grouping issue counts by entry date

**Output:** `operational_issues.csv`

**Run:** `python src/operational_issues.py --output-dir <directory>` (or `python3` depending on your system setup)

---

### runaway_resources.py

Flags endpoints producing an unusually high number of resources, indicating a possible "runaway" pipeline producing excess snapshots.

**What it does:**

- Fetches active endpoints (no `endpoint_end_date`) from `reporting_historic_endpoints`
- Counts resources per endpoint, keeping only endpoints with more than one resource
- Flags endpoints that have: a new resource every day for the last 7 days, more than 20 new resources in the last 30 days, and/or a stale resource end date (over 30 days old)

**Output:** `runaway_resources.csv`

**Run:** `python src/runaway_resources.py --output-dir <directory>` (or `python3` depending on your system setup)

---

## Adding New Scripts

When creating a new reporting script, please add a brief description to this README in alphabetical order, following the format above. Include:

- What the script does
- What data it outputs
- How to run it
