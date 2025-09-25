# prevent attempt to download centralised config
init::;
	pip install --upgrade pip
	pip install --upgrade -r requirements.txt

.PHONY: \
	compile \
	init \
	run-task \
	test \
	test-unit \
	test-acceptance


init ::
	pip install --upgrade pip
	pip3 install --upgrade -r requirements.txt

data/reporting:
	mkdir -p data/reporting

data/reporting/duplicate_entity_expectation.csv: data/reporting
	python src/duplicate_geometry_expectations.py --output-dir data/reporting

data/reporting/endpoint-dataset-issue-type-summary.csv: data/reporting
	python src/endpoint_dataset_issue_type_summary.py --output-dir data/reporting
	
data/reporting/all-endpoints-and-documentation-urls.csv: data/reporting
	python src/endpoints_missing_doc_urls.py --output-dir data/reporting

# produces two files but leave for now
data/reporting/flag_endpoints_no_provision.csv: data/reporting
	python src/flag_endpoints_no_provison.py --output-dir data/reporting

data/reporting/flagged_failed_resources.csv: data/reporting
	python src/flagged_failed_resources.py --output-dir data/reporting

# src/generate_odp_conformance_csv.py <- fix this one

data/reporting/odp-issue.csv: 
	python src/generate_odp_issues_csv.py --output-dir data/reporting

data/reporting/odp-status.csv: 
	python src/generate_odp_status_csv.py --output-dir data/reporting

data/reporting/logs-by-week.csv:
	python src/logs_by_week.py --output-dir data/reporting

data/reporting/odp-conformance.csv:
	python src/generate_odp_conformance_csv.py --output-dir data/reporting --specification-dir data/specification
# src/operational_issues.py <- fix this one

# data/reporting/operational-issues.csv: data/reporting
# 	python src/operational_issues.py --output-dir data/reporting

data/reporting/runaway_resources.csv: data/reporting
	python src/runaway_resources.py --output-dir data/reporting

.PHONY: all
all: data/reporting/duplicate_entity_expectation.csv \
	data/reporting/endpoint-dataset-issue-type-summary.csv \
	data/reporting/all-endpoints-and-documentation-urls.csv \
	data/reporting/flag_endpoints_no_provision.csv \
	data/reporting/flagged_failed_resources.csv \
	data/reporting/odp-issue.csv \
	data/reporting/odp-status.csv \
	data/reporting/logs-by-week.csv \
	data/reporting/runaway_resources.csv\
	data/reporting/odp-conformance.csv