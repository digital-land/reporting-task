"""
Builds four quality CSV reports - an ODP pair and a "mandated" dataset pair - by combining
task.csv (the platform's own list of per-organisation/per-dataset quality tasks) with
provision/organisation lookups and a lightweight active-endpoint query to determine which
provisions are currently live. It maps issue-type tasks to quality criteria, calculates
provider-dataset quality levels on a 0-6 scale (authoritative axis x rung axis) plus criteria
pass/fail detail, and writes the four CSV outputs.
"""

from __future__ import annotations

import argparse
import json
import os

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely.wkt
from utils import read_csv_with_retry, datasette_query, datasette_query_paginated

TASK_CSV_URL = "https://files.planning.data.gov.uk/dataset/task.csv"

ODP_DATASETS = [
    "conservation-area",
    "conservation-area-document",
    "article-4-direction-area",
    "article-4-direction",
    "listed-building-outline",
    "tree",
    "tree-preservation-zone",
    "tree-preservation-order",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args()


def get_pdp_gdf(dataset: str, geometry_field: str, usecols: list = None) -> gpd.GeoDataFrame:
    df = read_csv_with_retry(
        f"https://files.planning.data.gov.uk/dataset/{dataset}.csv",
        dtype="str",
        usecols=usecols,
    )
    df.columns = [c.replace("-", "_") for c in df.columns]
    df = df[df[geometry_field].notnull()].copy()
    df[geometry_field] = df[geometry_field].apply(shapely.wkt.loads)
    gdf = gpd.GeoDataFrame(df, geometry=geometry_field)
    gdf.set_crs(epsg=4326, inplace=True)
    return gdf


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

    provision = datasette_query_paginated(
        "digital-land",
        "SELECT * FROM provision WHERE project = 'open-digital-planning'",
    ).rename(columns={"dataset": "pipeline"})

    # "mandated" datasets (statutory, or "encouraged" specifically for LPAs) - computed live
    # from provision_rule rather than hardcoded, since this list can change over time. ODP and
    # mandated datasets are reported as separate CSV pairs below, since mandated datasets have
    # no "cohort"/provision concept and no expected-provision list to backfill missing
    # organisations against, unlike ODP.
    provision_rule = datasette_query(
        "digital-land",
        "SELECT dataset, project, provision_reason, role FROM provision_rule",
    )
    mandated_datasets = sorted(set(provision_rule.loc[
        (provision_rule["provision_reason"] == "statutory")
        | ((provision_rule["provision_reason"] == "encouraged") & (provision_rule["role"] == "local-planning-authority")),
        "dataset",
    ]))

    # active-endpoint population: task.csv only ever lists problems, so it can't tell us which
    # provisions currently have live data at all - a provision with a perfectly clean endpoint
    # and no tasks would otherwise be invisible. This query establishes that base population;
    # it is not itself a quality signal, just scope (mirrors the old endpoint_issues query minus
    # the issue join, which now comes from task.csv instead).
    active_endpoints = datasette_query_paginated(
        "performance",
        """
        SELECT rhe.organisation,
               rhe.name AS organisation_name,
               rhe.collection,
               rhe.pipeline,
               rhe.endpoint,
               rhe.resource
        FROM reporting_historic_endpoints rhe
        WHERE rhe.endpoint_end_date = ''
          AND rhe.resource_end_date = ''
          AND rhe.latest_status = 200
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

    # exclude organisations that have ended - a still-active endpoint or provision record can
    # linger in the source data after an organisation's own end_date is set, and closed/merged
    # organisations shouldn't appear in the quality report
    active_organisations = set(org_lookup.loc[org_lookup["end_date"].isnull(), "organisation"])
    active_endpoints = active_endpoints[active_endpoints["organisation"].isin(active_organisations)]
    provision = provision[provision["organisation"].isin(active_organisations)]

    lpa_gdf = get_pdp_gdf("local-planning-authority", "geometry", usecols=["reference", "name", "geometry"]).rename(
        columns={"reference": "LPACD", "name": "lpa_name"}
    )

    lpa_live = lpa_gdf[["LPACD", "geometry"]].merge(
        org_lookup[org_lookup["end_date"].isnull()][["LPACD", "organisation", "organisation_name", "organisation_entity"]],
        how="inner",
        on="LPACD",
    )

    base = lpa_live[["LPACD", "organisation"]].merge(active_endpoints, how="outer", on="organisation")

    # task.csv is the source of truth for quality signals: one row per flagged condition,
    # keyed by dataset/organisation(/endpoint/resource), with a `details` JSON blob whose shape
    # depends on task_source (issue / provision / expectation / log - see quality_dimension
    # values for the full breakdown). "issue", "provision" (authoritativeness) and the
    # expectation "count_lpa_boundary" operation (entities outside the LPA boundary) are used.
    # No task.csv equivalent was found for the old manual-count-match expectation check, which
    # is dropped rather than guessed at.
    task_df = read_csv_with_retry(TASK_CSV_URL, dtype="str").rename(columns={"dataset": "pipeline"})
    task_df = task_df[task_df["organisation"].isin(active_organisations)]

    # Authoritative-source signal: a "provision" task is only raised when an organisation's data
    # for a dataset is confirmed NOT authoritative (details.quality is "some" or "none") - so
    # presence of a task means not authoritative, and absence means authoritative (task.csv has
    # no separate "checked and passed" state, unlike the old per-pipeline entity-table query).
    auth_lookup = task_df[task_df["task_source"] == "provision"][["pipeline", "organisation"]].drop_duplicates()
    auth_lookup["is_authoritative"] = False

    # Absence of a provision task doesn't always mean authoritative, though - confirmed with the
    # data team: it can also mean the organisation isn't the designated provider for that dataset
    # at all (e.g. MHCLG/Historic England supplying a fallback "alternative" source for what's
    # normally an LPA's own dataset - task.csv would never raise a task against them, since
    # asking them to be more authoritative wouldn't fix anything). The general (non-ODP-scoped)
    # provision table flags exactly these cases via provision_reason='alternative', so they're
    # excluded from the "no task -> authoritative" inference rather than defaulting to True.
    alternative_providers = datasette_query(
        "digital-land",
        "SELECT dataset AS pipeline, organisation FROM provision WHERE project = '' AND provision_reason = 'alternative'",
    ).drop_duplicates()
    alternative_providers["authoritative_check_available"] = False

    # Boundary-check signal: replaces the old expectation-table query for geometries recorded
    # outside the LPA boundary. For conservation-area specifically, this operation is known to
    # actually be a different check (a manual-count comparison, not a boundary-violation count -
    # its "count" can exceed an org's total entity count, e.g. local-authority:EHA: 94 vs 58
    # total conservation-area entities) - tracked as a data-quality bug in a separate GitHub
    # issue against task.csv itself. Per product decision, it's used as-is here regardless.
    bounds_tasks = task_df[task_df["task_source"] == "expectation"].copy()
    bounds_tasks["operation"] = bounds_tasks["details"].apply(lambda d: json.loads(d).get("operation"))
    bounds_tasks = bounds_tasks[bounds_tasks["operation"] == "count_lpa_boundary"][["pipeline", "organisation"]].drop_duplicates()

    qual_bounds = lpa_live.merge(bounds_tasks, how="inner", on="organisation")[["LPACD", "organisation", "organisation_name", "pipeline"]]
    qual_bounds["quality_criteria"] = "3 - entities within LPA boundary"
    qual_bounds["quality_level"] = 3

    # Issue-type tasks - one row per (organisation, pipeline, endpoint, resource, issue_type).
    # Left-merged onto `base` so a resource with no issue tasks still gets a row (issue_type
    # NaN), same as the old SQL LEFT JOIN onto endpoint_dataset_issue_type_summary.
    issue_tasks = task_df[task_df["task_source"] == "issue"].copy()
    issue_tasks["issue_type"] = issue_tasks["details"].apply(lambda d: json.loads(d).get("issue_type"))
    issue_tasks = issue_tasks[["organisation", "pipeline", "endpoint", "resource", "issue_type"]]

    base_with_issues = base.merge(issue_tasks, how="left", on=["organisation", "pipeline", "endpoint", "resource"])

    qual_issues = base_with_issues.merge(issue_lookup, how="left", on="issue_type")[[
        "LPACD",
        "collection",
        "pipeline",
        "organisation",
        "organisation_name",
        "issue_type",
        "quality_criteria",
        "quality_level",
    ]]

    # severity-only (authoritative status is a separate axis, handled via auth_lookup below,
    # not concatenated in here - this replaces the old geospatial-join-based qual_prov table)
    qual_all = pd.concat([qual_bounds, qual_issues], ignore_index=True)

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
        qual_all.groupby(["LPACD", "pipeline", "organisation", "organisation_name"], as_index=False, dropna=False)
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

    # subset to ODP datasets and pivot for the ODP scores-by-LPA CSV. cohort/start_date are an
    # organisation-level attribute of ODP provision (constant across an org's ODP pipelines),
    # not a per-pipeline one, so they're looked up per-organisation.
    org_cohort_lookup = provision[["organisation", "cohort", "start_date"]].drop_duplicates()

    odp_lpa_summary = qual_summary[qual_summary["pipeline"].isin(ODP_DATASETS)].merge(
        org_cohort_lookup,
        how="left",
        on="organisation",
    )

    odp_lpa_summary_wide = (
        odp_lpa_summary.pivot(
            columns="pipeline",
            values="quality_level_label",
            index=["cohort", "start_date", "organisation", "organisation_name"],
        )
        .reset_index()
        .sort_values(["cohort", "organisation_name"])
    )
    # fill missing pipeline scores with "0. no data"
    odp_pipeline_cols = [c for c in odp_lpa_summary_wide.columns if c not in ["cohort", "start_date", "organisation", "organisation_name"]]
    odp_lpa_summary_wide[odp_pipeline_cols] = odp_lpa_summary_wide[odp_pipeline_cols].fillna("0. no data")

    # flag whether LPAs are "ready for ODP" (must be in the authoritative branch for all
    # geography datasets) - min_quality_level >= 4 means every geography dataset must be in
    # the authoritative branch (4-6), replacing the old 1-4 scale's >= 2 threshold. This is an
    # ODP-only concept, so it only appears in the ODP scores-by-LPA CSV.
    ready = qual_summary[
        qual_summary["pipeline"].isin(
            [
                "article-4-direction-area",
                "conservation-area",
                "listed-building-outline",
                "tree",
                "tree-preservation-zone",
            ]
        )
    ].groupby("organisation", as_index=False).agg(
        area_dataset_count=("pipeline", "count"),
        min_quality_level=("quality_level", "min"),
    )
    ready["ready_for_ODP_adoption"] = np.where(
        (ready["area_dataset_count"] == 5) & (ready["min_quality_level"] >= 4),
        "yes",
        "no",
    )
    odp_lpa_summary_wide = odp_lpa_summary_wide.merge(
        ready[["organisation", "ready_for_ODP_adoption"]],
        how="left",
        on="organisation",
    )
    odp_lpa_summary_wide["ready_for_ODP_adoption"] = odp_lpa_summary_wide["ready_for_ODP_adoption"].fillna("no")

    # subset to mandated datasets and pivot for the mandated scores-by-LPA CSV. Mandated
    # datasets have no "cohort"/provision concept and no expected-provision list to backfill
    # missing organisations against (unlike ODP below), so this is a simple pivot of whatever
    # live data exists.
    mandated_lpa_summary_wide = (
        qual_summary[qual_summary["pipeline"].isin(mandated_datasets)]
        .pivot(columns="pipeline", values="quality_level_label", index=["organisation", "organisation_name"])
        .reset_index()
        .sort_values("organisation_name")
    )
    mandated_pipeline_cols = [c for c in mandated_lpa_summary_wide.columns if c not in ["organisation", "organisation_name"]]
    mandated_lpa_summary_wide[mandated_pipeline_cols] = mandated_lpa_summary_wide[mandated_pipeline_cols].fillna("0. no data")

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

    # bring in the authoritative-source status (a separate axis, not part of the severity
    # quality_criteria pivot above)
    qual_cat_summary_wide = qual_cat_summary_wide.merge(
        qual_summary[["organisation", "pipeline", "is_authoritative", "authoritative_check_available"]].drop_duplicates(),
        how="left",
        on=["organisation", "pipeline"],
    )

    odp_qual_summary = qual_cat_summary_wide[
        qual_cat_summary_wide["pipeline"].isin(ODP_DATASETS)
    ].copy()

    odp_qual_summary = odp_qual_summary.merge(
        provision[["organisation", "pipeline", "cohort", "start_date"]],
        on=["organisation", "pipeline"],
        how="left",
    )

    non_criteria_cols = [
        "pipeline", "organisation", "organisation_name", "cohort", "start_date",
        "quality_level_label", "is_authoritative", "authoritative_check_available",
    ]
    qual_criteria_cols = [c for c in odp_qual_summary.columns if c not in non_criteria_cols]
    flag_map = {True: "FALSE", False: "TRUE", 1: "FALSE", 0: "TRUE", 1.0: "FALSE", 0.0: "TRUE"}
    for col in qual_criteria_cols:
        odp_qual_summary[col] = odp_qual_summary[col].map(flag_map)

    # these are already true/false in their natural sense (unlike the issue_flag-derived
    # criteria columns above, which invert), so map straight through to TRUE/FALSE strings
    # for consistent CSV formatting rather than leaving them as 1.0/0.0/blank.
    bool_cols = ["is_authoritative", "authoritative_check_available"]
    bool_map = {True: "TRUE", False: "FALSE", 1: "TRUE", 0: "FALSE", 1.0: "TRUE", 0.0: "FALSE"}
    for col in bool_cols:
        odp_qual_summary[col] = odp_qual_summary[col].map(bool_map)

    # Add missing ODP LPAs to scores CSV
    all_odp_combos = provision[["cohort", "start_date", "organisation", "pipeline"]].merge(
        org_lookup[["organisation", "organisation_name"]].drop_duplicates(),
        on="organisation",
        how="left"
    )[["cohort", "start_date", "organisation", "organisation_name"]].drop_duplicates()

    existing_combos = odp_lpa_summary_wide[["cohort", "organisation", "organisation_name"]].drop_duplicates()
    missing_combos = all_odp_combos[~all_odp_combos[["cohort", "organisation"]].apply(tuple, axis=1).isin(
        existing_combos[["cohort", "organisation"]].apply(tuple, axis=1)
    )]

    if len(missing_combos) > 0:
        missing_rows = missing_combos.copy()
        for col in ODP_DATASETS:
            missing_rows[col] = "0. no data"
        missing_rows["ready_for_ODP_adoption"] = "no"
        odp_lpa_summary_wide = pd.concat([odp_lpa_summary_wide, missing_rows], ignore_index=True)
        odp_lpa_summary_wide = odp_lpa_summary_wide.sort_values(["cohort", "organisation_name"]).reset_index(drop=True)

    # Add missing org+pipeline combos to the ODP detail CSV - there's no equivalent "expected
    # provision" list in the `provision` table to backfill against for mandated datasets, which
    # only ever appear in the mandated detail CSV where they have live data (see below).
    all_odp_org_pipeline = provision[["organisation", "pipeline", "cohort", "start_date"]].merge(
        org_lookup[["organisation", "organisation_name"]].drop_duplicates(),
        on="organisation",
        how="left"
    )[["organisation", "pipeline", "cohort", "start_date", "organisation_name"]].drop_duplicates()

    existing_org_pipeline = odp_qual_summary[["organisation", "pipeline"]].drop_duplicates()
    missing_org_pipeline = all_odp_org_pipeline[~all_odp_org_pipeline[["organisation", "pipeline"]].apply(tuple, axis=1).isin(
        existing_org_pipeline[["organisation", "pipeline"]].apply(tuple, axis=1)
    )]

    if len(missing_org_pipeline) > 0:
        missing_detail_rows = missing_org_pipeline.copy()
        for col in qual_criteria_cols:
            missing_detail_rows[col] = np.nan
        missing_detail_rows["is_authoritative"] = np.nan
        missing_detail_rows["authoritative_check_available"] = np.nan
        missing_detail_rows["quality_level_label"] = "0. no data"
        odp_qual_summary = pd.concat([odp_qual_summary, missing_detail_rows], ignore_index=True)

    odp_qual_summary = odp_qual_summary.sort_values(["pipeline", "organisation"]).reset_index(drop=True)

    # quality_level_label last, matching the notebook's column ordering
    front_cols = ["pipeline", "cohort", "start_date", "organisation", "organisation_name"]
    other_cols = [c for c in odp_qual_summary.columns if c not in front_cols and c != "quality_level_label"]
    odp_qual_summary = odp_qual_summary[front_cols + other_cols + ["quality_level_label"]]

    # mandated detail CSV: no cohort/provision concept and no expected-provision list to
    # backfill missing org+pipeline rows against, so this only ever contains rows with live data.
    mandated_qual_summary = qual_cat_summary_wide[
        qual_cat_summary_wide["pipeline"].isin(mandated_datasets)
    ].copy()
    mandated_non_criteria_cols = [
        "pipeline", "organisation", "organisation_name", "quality_level_label",
        "is_authoritative", "authoritative_check_available",
    ]
    mandated_criteria_cols = [c for c in mandated_qual_summary.columns if c not in mandated_non_criteria_cols]
    for col in mandated_criteria_cols:
        mandated_qual_summary[col] = mandated_qual_summary[col].map(flag_map)
    for col in bool_cols:
        mandated_qual_summary[col] = mandated_qual_summary[col].map(bool_map)

    mandated_qual_summary = mandated_qual_summary.sort_values(["pipeline", "organisation"]).reset_index(drop=True)

    mandated_front_cols = ["pipeline", "organisation", "organisation_name"]
    mandated_other_cols = [c for c in mandated_qual_summary.columns if c not in mandated_front_cols and c != "quality_level_label"]
    mandated_qual_summary = mandated_qual_summary[mandated_front_cols + mandated_other_cols + ["quality_level_label"]]

    out_odp_scores = os.path.join(output_dir, "quality_ODP_dataset_scores_by_LPA.csv")
    out_mandated_scores = os.path.join(output_dir, "quality_mandated_dataset_scores_by_LPA.csv")
    out_odp_detail = os.path.join(output_dir, "quality_ODP_dataset_quality_detail.csv")
    out_mandated_detail = os.path.join(output_dir, "quality_mandated_dataset_quality_detail.csv")

    odp_lpa_summary_wide.to_csv(out_odp_scores, index=False)
    mandated_lpa_summary_wide.to_csv(out_mandated_scores, index=False)
    odp_qual_summary.to_csv(out_odp_detail, index=False)
    mandated_qual_summary.to_csv(out_mandated_detail, index=False)

    print(f"Saved {out_odp_scores} ({len(odp_lpa_summary_wide)} rows)")
    print(f"Saved {out_mandated_scores} ({len(mandated_lpa_summary_wide)} rows)")
    print(f"Saved {out_odp_detail} ({len(odp_qual_summary)} rows)")
    print(f"Saved {out_mandated_detail} ({len(mandated_qual_summary)} rows)")


if __name__ == "__main__":
    main()
