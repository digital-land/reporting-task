"""
Builds a single-source dataset quality CSV report by combining task.csv (the platform's own
list of per-organisation/per-dataset quality tasks) with organisation lookups and a lightweight
active-endpoint query to determine which provisions are currently live. Single-source datasets
are everything that isn't ODP-scoped or "mandated" (see measure_odp_mandated_data_quality.py,
which covers those with slightly different checks). It maps issue-type tasks to quality
criteria, calculates provider-dataset quality levels on a 0-6 scale (authoritative axis x rung
axis) plus criteria pass/fail detail, applies a staleness cap (a criterion specific to
single-source datasets, which have no alternative source to cross-check freshness against, and
which task.csv carries no date field for - resource age is queried separately for this one
purpose), and writes the detail CSV output.
"""

from __future__ import annotations

import argparse
import json
import os

import numpy as np
from utils import datasette_query, datasette_query_paginated, read_csv_with_retry

TASK_CSV_URL = "https://files.planning.data.gov.uk/dataset/task.csv"
STALENESS_AGE_DAYS = 365


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)

    issue_lookup = datasette_query(
        "digital-land",
        """
        SELECT issue_type,
               quality_criteria_level || ' - ' || quality_criteria AS quality_criteria,
               quality_criteria_level AS quality_level
        FROM issue_type
        """,
    )

    # single-source datasets are everything NOT in ODP scope and NOT "mandated" (statutory,
    # or encouraged-for-LPAs - see measure_odp_mandated_data_quality.py, which covers those
    # with different checks). Computed from every dataset in provision_rule, not just active ones.
    provision_rule = datasette_query(
        "digital-land",
        "SELECT dataset, project, provision_reason, role FROM provision_rule",
    )
    odp_datasets = set(provision_rule.loc[provision_rule["project"] == "open-digital-planning", "dataset"])
    mandated_datasets = set(provision_rule.loc[
        (provision_rule["provision_reason"] == "statutory")
        | ((provision_rule["provision_reason"] == "encouraged") & (provision_rule["role"] == "local-planning-authority")),
        "dataset",
    ])
    single_source_pipelines = sorted(set(provision_rule["dataset"].dropna().unique()) - odp_datasets - mandated_datasets)

    # active-endpoint population: task.csv only ever lists problems, so it can't tell us which
    # provisions currently have live data at all - a provision with a perfectly clean endpoint
    # and no tasks would otherwise be invisible. This query establishes that base population
    # (and, uniquely for this report, resource age for the staleness cap below); it is not
    # itself a quality signal. Restricted to single-source pipelines at the query itself, so
    # ODP/mandated data never enters this report.
    quoted_pipelines = ", ".join(f"'{p}'" for p in single_source_pipelines)
    active_endpoints = datasette_query_paginated(
        "performance",
        f"""
        SELECT rhe.organisation,
               rhe.name AS organisation_name,
               rhe.collection,
               rhe.pipeline,
               rhe.endpoint,
               rhe.resource,
               CAST(JULIANDAY('now') - JULIANDAY(rhe.resource_start_date) AS int) AS resource_age_days
        FROM reporting_historic_endpoints rhe
        WHERE rhe.endpoint_end_date = ''
          AND rhe.resource_end_date = ''
          AND rhe.latest_status = 200
          AND rhe.pipeline IN ({quoted_pipelines})
        """,
    )

    org_lookup = datasette_query(
        "digital-land",
        """
        SELECT entity AS organisation_entity,
               name AS organisation_name,
               organisation,
               end_date,
               local_planning_authority AS LPACD,
               CASE
                 WHEN local_planning_authority != '' OR organisation IN ('local-authority:NDO', 'local-authority:PUR') THEN 1
                 ELSE 0
               END AS lpa_flag
        FROM organisation
        WHERE name != 'Waveney District Council'
        """,
    )
    org_lookup[["lpa_flag", "organisation_entity"]] = org_lookup[["lpa_flag", "organisation_entity"]].astype(int)
    # end_date is '' (not SQL NULL) for an active org - normalise so .isnull() below works,
    # same as pd.read_csv's default handling of a blank CSV field.
    org_lookup["end_date"] = org_lookup["end_date"].replace("", None)

    # exclude organisations that have ended - a still-active endpoint record can linger in the
    # source data after an organisation's own end_date is set, and closed/merged organisations
    # shouldn't appear in the quality report
    active_organisations = set(org_lookup.loc[org_lookup["end_date"].isnull(), "organisation"])
    active_endpoints = active_endpoints[active_endpoints["organisation"].isin(active_organisations)]

    # task.csv is the source of truth for quality signals: one row per flagged condition, keyed
    # by dataset/organisation(/endpoint/resource), with a `details` JSON blob whose shape
    # depends on task_source. Only "issue" and "provision" (authoritativeness) tasks are used by
    # this report, restricted to single-source pipelines to match active_endpoints above.
    task_df = read_csv_with_retry(TASK_CSV_URL, dtype="str").rename(columns={"dataset": "pipeline"})
    task_df = task_df[
        task_df["organisation"].isin(active_organisations) & task_df["pipeline"].isin(single_source_pipelines)
    ]

    # Authoritative-source signal: a "provision" task is only raised when an organisation's data
    # for a dataset is confirmed NOT authoritative (details.quality is "some" or "none") - so
    # presence of a task means not authoritative, and absence means authoritative (task.csv has
    # no separate "checked and passed" state, unlike the old per-pipeline entity-table query).
    auth_lookup = task_df[task_df["task_source"] == "provision"][["pipeline", "organisation"]].drop_duplicates()
    auth_lookup["is_authoritative"] = False

    # Absence of a provision task doesn't always mean authoritative, though - confirmed with the
    # data team: it can also mean the organisation isn't the designated provider for that dataset
    # at all (e.g. a central government body supplying a fallback "alternative" source for what's
    # normally an LPA's own dataset - task.csv would never raise a task against them, since
    # asking them to be more authoritative wouldn't fix anything). The general (non-ODP-scoped)
    # provision table flags exactly these cases via provision_reason='alternative', so they're
    # excluded from the "no task -> authoritative" inference rather than defaulting to True.
    alternative_providers = datasette_query(
        "digital-land",
        "SELECT dataset AS pipeline, organisation FROM provision WHERE project = '' AND provision_reason = 'alternative'",
    ).drop_duplicates()
    alternative_providers["authoritative_check_available"] = False

    # Issue-type tasks - one row per (organisation, pipeline, endpoint, resource, issue_type).
    # Left-merged onto active_endpoints so a resource with no issue tasks still gets a row
    # (issue_type NaN), same as the old SQL LEFT JOIN onto endpoint_dataset_issue_type_summary.
    issue_tasks = task_df[task_df["task_source"] == "issue"].copy()
    issue_tasks["issue_type"] = issue_tasks["details"].apply(lambda d: json.loads(d).get("issue_type"))
    issue_tasks = issue_tasks[["organisation", "pipeline", "endpoint", "resource", "issue_type"]]

    endpoints_with_issues = active_endpoints.merge(
        issue_tasks, how="left", on=["organisation", "pipeline", "endpoint", "resource"]
    )

    # ISSUES TABLE - flagging when provisions have data quality issues (authoritative status
    # and staleness are separate axes, handled below, not concatenated in here)
    qual_all = endpoints_with_issues.merge(issue_lookup, how="left", on="issue_type")[[
        "collection", "pipeline", "organisation", "organisation_name", "issue_type", "quality_criteria", "quality_level",
    ]]

    # 0-6 scale: authoritative axis (confirmed authoritative-sourced data?) crossed with rung
    # axis (some data -> usable -> trustworthy). 0 is for provisions with no data at all -
    # either no active endpoint, or an active endpoint that produced zero actual entities.
    level_map = {
        6: "6. trustworthy data",
        5: "5. usable data",
        4: "4. authoritative data",
        3: "3. verifiable data",
        2: "2. indicative data",
        1: "1. some data",
        0: "0. no data",
    }

    qual_summary = (
        qual_all.groupby(["collection", "pipeline", "organisation", "organisation_name"], as_index=False, dropna=False)
        .agg(severity_level=("quality_level", "min"))
    )
    qual_summary["severity_level"] = qual_summary["severity_level"].replace(np.nan, 4)
    qual_summary["quality_rung"] = qual_summary["severity_level"] - 1

    # bring in authoritative status - missing a match now means "confirmed authoritative"
    # (see auth_lookup above), unlike the old entity-table lookup where a missing match meant
    # "not checked, treated as non-authoritative".
    qual_summary = qual_summary.merge(
        auth_lookup[["organisation", "pipeline", "is_authoritative"]],
        how="left",
        on=["organisation", "pipeline"],
    )
    qual_summary["is_authoritative"] = qual_summary["is_authoritative"].fillna(True)

    qual_summary = qual_summary.merge(
        alternative_providers[["organisation", "pipeline", "authoritative_check_available"]],
        how="left",
        on=["organisation", "pipeline"],
    )
    qual_summary["authoritative_check_available"] = qual_summary["authoritative_check_available"].fillna(True)
    qual_summary.loc[~qual_summary["authoritative_check_available"], "is_authoritative"] = False

    qual_summary["quality_level"] = np.where(
        qual_summary["is_authoritative"], qual_summary["quality_rung"] + 3, qual_summary["quality_rung"]
    ).astype(int)
    qual_summary["quality_level_label"] = qual_summary["quality_level"].map(level_map)
    qual_summary = qual_summary.drop(columns=["severity_level", "quality_rung"])

    # staleness acts as another criterion gating the top rung: a stale provision can't be
    # "trustworthy" and gets capped down to "usable" instead, but a provision already at
    # "usable" or "some data" isn't pushed down any further. Provisions already at 0 ("no
    # data") are left alone - there's nothing left to downgrade.
    stale = active_endpoints[active_endpoints["resource_age_days"] > STALENESS_AGE_DAYS][["pipeline", "organisation"]].drop_duplicates()
    stale["is_stale"] = True

    qual_summary = qual_summary.merge(stale, how="left", on=["pipeline", "organisation"])
    qual_summary["is_stale"] = qual_summary["is_stale"].eq(True)

    cap_mask = qual_summary["is_stale"] & (qual_summary["quality_level"] > 0)
    rung = np.where(qual_summary["is_authoritative"], qual_summary["quality_level"] - 3, qual_summary["quality_level"])
    capped_rung = np.minimum(rung, 2)
    capped_quality_level = np.where(qual_summary["is_authoritative"], capped_rung + 3, capped_rung)
    qual_summary.loc[cap_mask, "quality_level"] = capped_quality_level[cap_mask]
    qual_summary.loc[cap_mask, "quality_level_label"] = qual_summary.loc[cap_mask, "quality_level"].map(level_map)

    # bring in resource age for the detail output (not just the pass/fail is_stale flag) -
    # max across resources if an org has more than one for a pipeline
    age = active_endpoints.groupby(["pipeline", "organisation"], as_index=False).agg(resource_age_days=("resource_age_days", "max"))
    qual_summary = qual_summary.merge(age, how="left", on=["pipeline", "organisation"])

    qual_cat_count = qual_all.groupby(
        ["pipeline", "organisation", "organisation_name", "quality_criteria"],
        as_index=False,
    ).agg(n_issues=("quality_level", "count"))

    prov = qual_all[["pipeline", "organisation", "organisation_name"]].drop_duplicates()
    prov["key"] = 1
    qual_cat = qual_all[qual_all["quality_criteria"].notnull()][["quality_criteria"]].drop_duplicates()
    qual_cat["key"] = 1

    qual_cat_summary = prov.merge(qual_cat, how="left", on="key")
    qual_cat_summary = qual_cat_summary.merge(
        qual_cat_count,
        how="left",
        on=["pipeline", "organisation", "organisation_name", "quality_criteria"],
    )
    qual_cat_summary["issue_flag"] = np.where(qual_cat_summary["n_issues"] > 0, False, True)

    qual_cat_summary_wide = qual_cat_summary.pivot(
        columns="quality_criteria",
        values="issue_flag",
        index=["pipeline", "organisation", "organisation_name"],
    ).reset_index().merge(
        qual_summary[["pipeline", "organisation", "quality_level_label"]],
        how="left",
        on=["pipeline", "organisation"],
    )

    # bring in the authoritative-source and staleness checks (separate axes, not part of
    # the severity quality_criteria pivot above)
    qual_cat_summary_wide = qual_cat_summary_wide.merge(
        qual_summary[[
            "organisation", "pipeline", "is_authoritative", "authoritative_check_available",
            "resource_age_days", "is_stale",
        ]].drop_duplicates(),
        how="left",
        on=["organisation", "pipeline"],
    )

    # criteria columns come out of the pivot as issue_flag (True = no issue), which reads
    # backwards against a column literally named e.g. "2 - duplicate reference values" - invert
    # to TRUE/FALSE strings so TRUE means "yes, this issue occurred", matching
    # measure_odp_mandated_data_quality.py
    non_criteria_cols = [
        "pipeline", "organisation", "organisation_name", "resource_age_days", "quality_level_label",
        "is_authoritative", "authoritative_check_available", "is_stale",
    ]
    qual_criteria_cols = [c for c in qual_cat_summary_wide.columns if c not in non_criteria_cols]
    flag_map = {True: "FALSE", False: "TRUE", 1: "FALSE", 0: "TRUE", 1.0: "FALSE", 0.0: "TRUE"}
    for col in qual_criteria_cols:
        qual_cat_summary_wide[col] = qual_cat_summary_wide[col].map(flag_map)

    # these are already true/false in their natural sense (unlike the issue_flag-derived
    # criteria columns above, which invert), so map straight through to TRUE/FALSE strings
    # for consistent CSV formatting rather than leaving them as Python True/False/blank.
    bool_cols = ["is_authoritative", "authoritative_check_available", "is_stale"]
    bool_map = {True: "TRUE", False: "FALSE", 1: "TRUE", 0: "FALSE", 1.0: "TRUE", 0.0: "FALSE"}
    for col in bool_cols:
        qual_cat_summary_wide[col] = qual_cat_summary_wide[col].map(bool_map)

    # quality_level_label last, matching the notebook's column ordering
    qual_cat_summary_wide = qual_cat_summary_wide[
        [c for c in qual_cat_summary_wide.columns if c != "quality_level_label"] + ["quality_level_label"]
    ]

    out_detail = os.path.join(output_dir, "quality_single_source_dataset_quality_detail.csv")
    qual_cat_summary_wide.to_csv(out_detail, index=False)

    print(f"Saved {out_detail} ({len(qual_cat_summary_wide)} rows)")


if __name__ == "__main__":
    main()
