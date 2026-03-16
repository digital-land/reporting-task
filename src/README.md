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

## Adding New Scripts

When creating a new reporting script, please add a brief description to this README following the format above. Include:

- What the script does
- What data it outputs
- How to run it
