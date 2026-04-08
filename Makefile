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

make clobber:
	rm -rf data/reporting

data/reporting:
	mkdir -p data/reporting

data/reporting/deleted_entities.csv: data/reporting
	python src/check_deleted_entities.py --output-dir data/reporting

data/reporting/duplicate_entity_expectation.csv: data/reporting
	python src/duplicate_geometry_expectations.py --output-dir data/reporting

data/reporting/endpoint_dataset_issue_type_summary.csv: data/reporting
	python src/endpoint_dataset_issue_type_summary.py --output-dir data/reporting
	
data/reporting/all_endpoints_and_documentation_urls.csv: data/reporting
	python src/endpoints_missing_doc_urls.py --output-dir data/reporting

# produces two files but leave for now
data/reporting/flag_endpoints_no_provision.csv: data/reporting
	python src/flag_endpoints_no_provison.py --output-dir data/reporting

data/reporting/flagged_failed_resources.csv: data/reporting
	python src/flagged_failed_resources.py --output-dir data/reporting

# src/generate_odp_conformance_csv.py <- fix this one

data/reporting/odp_issue.csv: 
	python src/generate_odp_issues_csv.py --output-dir data/reporting

data/reporting/odp_status.csv: 
	python src/generate_odp_status_csv.py --output-dir data/reporting

data/reporting/listed_building_end_date.csv:
	python src/listed_building_end_date.py --output-dir data/reporting

data/reporting/logs_by_week.csv:
	python src/logs_by_week.py --output-dir data/reporting

data/reporting/odp_conformance.csv:
	python src/generate_odp_conformance_csv.py --output-dir data/reporting --specification-dir data/specification

data/reporting/quality_ODP_dataset_scores_by_LPA.csv data/reporting/quality_ODP_dataset_quality_detail.csv: data/reporting
	python src/measure_odp_data_quality.py --output-dir data/reporting
# src/operational_issues.py <- fix this one

# data/reporting/operational_issues.csv: data/reporting
# 	python src/operational_issues.py --output-dir data/reporting

data/reporting/entities_with_ended_orgs.csv:
	python src/monitoring_entities_ended_orgs.py --output-dir data/reporting

data/reporting/ended_orgs_active_endpoints.csv:
	python src/monitoring_active_endpoints_ended_orgs.py --output-dir data/reporting

data/reporting/runaway_resources.csv: data/reporting
	python src/runaway_resources.py --output-dir data/reporting

.PHONY: all
all: data/reporting/deleted_entities.csv \
	data/reporting/duplicate_entity_expectation.csv \
	data/reporting/endpoint_dataset_issue_type_summary.csv \
	data/reporting/all_endpoints_and_documentation_urls.csv \
	data/reporting/flag_endpoints_no_provision.csv \
	data/reporting/flagged_failed_resources.csv \
	data/reporting/odp_issue.csv \
	data/reporting/odp_status.csv \
	data/reporting/listed_building_end_date.csv \
	data/reporting/logs_by_week.csv \
	data/reporting/runaway_resources.csv\
	data/reporting/odp_conformance.csv\
	data/reporting/quality_ODP_dataset_scores_by_LPA.csv\
	data/reporting/quality_ODP_dataset_quality_detail.csv\
	data/reporting/entities_with_ended_orgs.csv\
	data/reporting/ended_orgs_active_endpoints.csv