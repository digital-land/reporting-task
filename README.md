# Build Reporting Datasets

[![License](https://img.shields.io/github/license/mashape/apistatus.svg)](https://github.com/digital-land/digital-land-builder/blob/master/LICENSE)

Builds a set of CSVs and stores them into the collection data bucket under reporting. These CSVs are intended for reporting by the Collection and Management team. All data will be publicly accessible from `files.planning.data.gov.uk/reporting`.

## Repository Structure

- **`src/`** - Python scripts that generate reporting datasets
- **`data/reporting/`** - Output directory for generated CSV files
- **`Makefile`** - Build targets for each report (run `make all` to generate all reports, or `make <target>` for individual reports)
- **`requirements.txt`** - Python dependencies
- **`run.sh`** - Shell script for containerized execution
- **`Dockerfile`** - Container configuration

Each Python script in `src/` can be run individually with `python src/script_name.py --output-dir <directory>` (or `python3` depending on your system setup). Note: if `make all` fails with "python: No such file or directory", your system requires `python3` - edit the Makefile locally to replace `python` with `python3`.

## Licence

The software in this project is open source and covered by the [LICENSE](LICENSE) file.

Individual datasets copied into this repository may have specific copyright and licensing, otherwise all content and data in this repository is
[© Crown copyright](http://www.nationalarchives.gov.uk/information-management/re-using-public-sector-information/copyright-and-re-use/crown-copyright/)
and available under the terms of the [Open Government 3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) licence.
