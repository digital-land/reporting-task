"""
Builds two ODP quality CSV reports (covering ODP datasets plus "mandated" datasets) by
combining active endpoint issue data, provision/organisation lookups, and each dataset's own
entity-level `quality` signal (rather than a geospatial join) to determine authoritative sourcing.
It maps issues to quality criteria, calculates provider-dataset quality levels on a 0-6 scale
(authoritative axis x rung axis) plus criteria pass/fail detail, writes the two CSV outputs.
"""

from __future__ import annotations

import argparse
import os
import urllib.parse

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely.wkt
from utils import read_csv_with_retry

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


def datasette_query(db: str, sql: str) -> pd.DataFrame:
    params = urllib.parse.urlencode({"sql": sql, "_size": "max"})
    return read_csv_with_retry(f"https://datasette.planning.data.gov.uk/{db}.csv?{params}")


def datasette_query_paginated(db: str, sql: str, page_size: int = 1000) -> pd.DataFrame:
    frames = []
    offset = 0

    while True:
        page_sql = f"{sql}\nLIMIT {page_size} OFFSET {offset}"
        page_df = datasette_query(db, page_sql)
        if page_df.empty:
            break

        frames.append(page_df)

        if len(page_df) < page_size:
            break
        offset += page_size

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def get_entity_quality(pipeline: str) -> pd.DataFrame:
    # per-entity authoritative-source signal, computed by the platform itself against each
    # dataset's own database. Not every dataset has an entity/quality/organisation_entity
    # column (e.g. pure reference/enum datasets), so failures are swallowed and return empty.
    sql = """
        SELECT organisation_entity, quality, COUNT(*) as n
        FROM entity
        WHERE organisation_entity IS NOT NULL AND organisation_entity != ''
        GROUP BY organisation_entity, quality
    """
    try:
        df = datasette_query(pipeline, sql)
    except Exception:
        return pd.DataFrame(columns=["organisation_entity", "quality", "n", "pipeline"])

    df["pipeline"] = pipeline
    return df


def make_authoritative_lookup(entity_quality_raw: pd.DataFrame, quality_priority: dict, org_lookup: pd.DataFrame) -> pd.DataFrame:
    # flags whether a provision's data is confirmed to come from the authoritative source,
    # using the platform's own per-entity `quality` field rather than a geospatial approximation
    # - an organisation can be the "expected" provider for a dataset yet still have its area
    # covered by an alternative source's entities. Aggregated leniently: if ANY entity attributed
    # to an organisation is quality-tier "authoritative" or better, the whole organisation+pipeline
    # provision counts as authoritative.

    df = entity_quality_raw.copy()
    df["priority"] = df["quality"].map(quality_priority)
    df = df.dropna(subset=["priority", "organisation_entity"])

    authoritative_priority = quality_priority["authoritative"]
    df["is_authoritative_entity"] = df["priority"] >= authoritative_priority

    summary = df.groupby(["pipeline", "organisation_entity"], as_index=False).agg(
        is_authoritative=("is_authoritative_entity", "max")
    )
    summary["organisation_entity"] = summary["organisation_entity"].astype(int)

    summary = summary.merge(
        org_lookup[["organisation_entity", "organisation", "organisation_name"]],
        how="left",
        on="organisation_entity",
    )
    summary["authoritative_check_available"] = True

    return summary[["pipeline", "organisation", "organisation_name", "is_authoritative", "authoritative_check_available"]]


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
    # from provision_rule rather than hardcoded, since this list can change over time. Both
    # the detail and scores-by-LPA CSVs now cover ODP_DATASETS + mandated_datasets; since
    # mandated datasets have no "cohort" concept in the provision table, organisations with
    # no ODP provision of their own get a blank cohort/start_date in the scores-by-LPA CSV.
    provision_rule = datasette_query(
        "digital-land",
        "SELECT dataset, project, provision_reason, role FROM provision_rule",
    )
    mandated_datasets = sorted(set(provision_rule.loc[
        (provision_rule["provision_reason"] == "statutory")
        | ((provision_rule["provision_reason"] == "encouraged") & (provision_rule["role"] == "local-planning-authority")),
        "dataset",
    ]))
    detail_datasets = ODP_DATASETS + mandated_datasets

    quality_lookup = datasette_query("digital-land", "SELECT quality, priority FROM quality")
    quality_priority = dict(zip(quality_lookup["quality"], quality_lookup["priority"]))

    endpoint_issues = datasette_query_paginated(
        "performance",
        """
        SELECT rhe.organisation,
               rhe.name AS organisation_name,
               rhe.collection,
               rhe.pipeline,
               rhe.endpoint,
               rhe.resource,
               its.issue_type
        FROM reporting_historic_endpoints rhe
        LEFT JOIN endpoint_dataset_issue_type_summary its ON rhe.resource = its.resource
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

    lpa_gdf = get_pdp_gdf("local-planning-authority", "geometry", usecols=["reference", "name", "geometry"]).rename(
        columns={"reference": "LPACD", "name": "lpa_name"}
    )

    lpa_live = lpa_gdf[["LPACD", "geometry"]].merge(
        org_lookup[org_lookup["end_date"].isnull()][["LPACD", "organisation", "organisation_name", "organisation_entity"]],
        how="inner",
        on="LPACD",
    )

    base = lpa_live[["LPACD", "organisation"]].merge(endpoint_issues, how="outer", on="organisation")

    # Authoritative-source signal: query each active pipeline's own entity table for its
    # platform-computed `quality` field, keyed by organisation_entity. This is a much better
    # signal than a geospatial join - it reflects the real source of the data actually held,
    # not just who is registered as the expected provider. Pipelines without a usable
    # entity/quality/organisation_entity column just come back empty from get_entity_quality.
    entity_quality_frames = []
    for pipeline in endpoint_issues["pipeline"].dropna().unique():
        pipeline_entity_quality = get_entity_quality(pipeline)
        if not pipeline_entity_quality.empty:
            entity_quality_frames.append(pipeline_entity_quality)
    entity_quality_raw = (
        pd.concat(entity_quality_frames, ignore_index=True)
        if entity_quality_frames
        else pd.DataFrame(columns=["organisation_entity", "quality", "n", "pipeline"])
    )

    auth_lookup = make_authoritative_lookup(entity_quality_raw, quality_priority, org_lookup)

    qual_match_orgs = datasette_query(
        "digital-land",
        """
        SELECT DISTINCT organisation
        FROM expectation
        WHERE name = 'Check number of conservation-area entities inside the local planning authority boundary matches the manual count'
          AND passed = 'False'
        """,
    )
    qual_match = lpa_live.merge(qual_match_orgs, how="inner", on="organisation")[["LPACD", "organisation", "organisation_name"]]
    qual_match["collection"] = "conservation-area"
    qual_match["pipeline"] = "conservation-area"
    qual_match["quality_criteria"] = "3 - entity count matches LPA"
    qual_match["quality_level"] = 3

    bounds_orgs = datasette_query(
        "digital-land",
        """
        SELECT DISTINCT organisation, dataset AS pipeline
        FROM expectation
        WHERE name LIKE '%outside%'
          AND message NOT LIKE '%error%'
          AND passed = 'False'
        """,
    )
    qual_bounds = lpa_live.merge(bounds_orgs, how="inner", on="organisation")[["LPACD", "organisation", "organisation_name", "pipeline"]]
    qual_bounds["quality_criteria"] = "3 - entities within LPA boundary"
    qual_bounds["quality_level"] = 3

    qual_issues = base.merge(issue_lookup, how="left", on="issue_type")[[
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
    qual_all = pd.concat([qual_match, qual_bounds, qual_issues], ignore_index=True)

    # 0-6 scale: authoritative axis (confirmed authoritative-sourced data?) crossed with rung
    # axis (some data -> usable -> trustworthy). 0 is for provisions with no data at all -
    # either no active endpoint, or an active endpoint that produced zero actual entities.
    level_map = {
        6: "6. authoritative trustworthy data",
        5: "5. authoritative usable data",
        4: "4. authoritative data",
        3: "3. non-authoritative trustworthy data",
        2: "2. non-authoritative usable data",
        1: "1. non-authoritative/some data",
        0: "0. no data",
    }

    qual_summary = (
        qual_all.groupby(["LPACD", "pipeline", "organisation", "organisation_name"], as_index=False, dropna=False)
        .agg(severity_level=("quality_level", "min"))
    )
    qual_summary["severity_level"] = qual_summary["severity_level"].replace(np.nan, 4)
    qual_summary["quality_rung"] = qual_summary["severity_level"] - 1

    # bring in authoritative status. missing a match means "not checked", treated the same as
    # non-authoritative (not proven authoritative -> not elevated), but flagged separately so
    # it's distinguishable from a provision that was actually checked and found non-authoritative.
    qual_summary = qual_summary.merge(
        auth_lookup[["organisation", "pipeline", "is_authoritative", "authoritative_check_available"]],
        how="left",
        on=["organisation", "pipeline"],
    )
    qual_summary["is_authoritative"] = qual_summary["is_authoritative"].fillna(False).astype(bool)
    qual_summary["authoritative_check_available"] = qual_summary["authoritative_check_available"].fillna(False).astype(bool)

    qual_summary["quality_level"] = np.where(
        qual_summary["is_authoritative"], qual_summary["quality_rung"] + 3, qual_summary["quality_rung"]
    ).astype(int)
    qual_summary["quality_level_label"] = qual_summary["quality_level"].map(level_map)
    qual_summary = qual_summary.drop(columns=["severity_level", "quality_rung"])

    # an active endpoint that produced zero entities has nothing meaningful for the severity/
    # authoritative axes to score - force it to "no data" rather than whatever rung/authoritative
    # combination the (empty) issue/entity metadata would otherwise imply. Only overrides
    # pipelines whose entity table was actually queried successfully (present in
    # entity_quality_raw) - if the fetch failed for a pipeline entirely, we don't know its
    # entity counts, so those scores are left as computed.
    queryable_pipelines = entity_quality_raw["pipeline"].unique()
    has_entities = entity_quality_raw[["pipeline", "organisation_entity"]].drop_duplicates().copy()
    has_entities["organisation_entity"] = has_entities["organisation_entity"].astype(int)
    has_entities = has_entities.merge(
        org_lookup[["organisation_entity", "organisation"]], how="left", on="organisation_entity"
    )[["pipeline", "organisation"]].drop_duplicates()
    has_entities["has_entities"] = True

    qual_summary = qual_summary.merge(has_entities, how="left", on=["pipeline", "organisation"])
    qual_summary["has_zero_entities"] = qual_summary["pipeline"].isin(queryable_pipelines) & qual_summary["has_entities"].isna()
    zero_entity_mask = qual_summary["has_zero_entities"]
    qual_summary.loc[zero_entity_mask, "quality_level"] = 0
    qual_summary.loc[zero_entity_mask, "quality_level_label"] = level_map[0]
    qual_summary = qual_summary.drop(columns=["has_entities"])

    # subset to ODP + mandated datasets and pivot. cohort/start_date are an organisation-level
    # attribute of ODP provision (constant across an org's ODP pipelines), not a per-pipeline
    # one, so they're looked up per-organisation and left blank for organisations with no ODP
    # provision of their own (e.g. a mandated-dataset-only provider).
    org_cohort_lookup = provision[["organisation", "cohort", "start_date"]].drop_duplicates()

    odp_lpa_summary = qual_summary[qual_summary["pipeline"].isin(detail_datasets)].merge(
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
    # fill missing pipeline scores with "0. no data", but leave cohort/start_date NaN (blank)
    # for organisations with no ODP provision of their own
    pipeline_cols = [c for c in odp_lpa_summary_wide.columns if c not in ["cohort", "start_date", "organisation", "organisation_name"]]
    odp_lpa_summary_wide[pipeline_cols] = odp_lpa_summary_wide[pipeline_cols].fillna("0. no data")

    # flag whether LPAs are "ready for ODP" (must be in the authoritative branch for all
    # geography datasets) - min_quality_level >= 4 means every geography dataset must be in
    # the authoritative branch (4-6), replacing the old 1-4 scale's >= 2 threshold
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

    # bring in the authoritative-source and zero-entity checks (separate axes, not part of
    # the severity quality_criteria pivot above)
    qual_cat_summary_wide = qual_cat_summary_wide.merge(
        qual_summary[["organisation", "pipeline", "is_authoritative", "authoritative_check_available", "has_zero_entities"]].drop_duplicates(),
        how="left",
        on=["organisation", "pipeline"],
    )

    odp_qual_summary = qual_cat_summary_wide[
        qual_cat_summary_wide["pipeline"].isin(detail_datasets)
    ].copy()

    odp_qual_summary = odp_qual_summary.merge(
        provision[["organisation", "pipeline", "cohort", "start_date"]],
        on=["organisation", "pipeline"],
        how="left",
    )

    non_criteria_cols = [
        "pipeline", "organisation", "organisation_name", "cohort", "start_date",
        "quality_level_label", "is_authoritative", "authoritative_check_available", "has_zero_entities",
    ]
    qual_criteria_cols = [c for c in odp_qual_summary.columns if c not in non_criteria_cols]
    flag_map = {True: "FALSE", False: "TRUE", 1: "FALSE", 0: "TRUE", 1.0: "FALSE", 0.0: "TRUE"}
    for col in qual_criteria_cols:
        odp_qual_summary[col] = odp_qual_summary[col].map(flag_map)

    # these are already true/false in their natural sense (unlike the issue_flag-derived
    # criteria columns above, which invert), so map straight through to TRUE/FALSE strings
    # for consistent CSV formatting rather than leaving them as 1.0/0.0/blank.
    bool_cols = ["is_authoritative", "authoritative_check_available", "has_zero_entities"]
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
        for col in detail_datasets:
            missing_rows[col] = "0. no data"
        missing_rows["ready_for_ODP_adoption"] = "no"
        odp_lpa_summary_wide = pd.concat([odp_lpa_summary_wide, missing_rows], ignore_index=True)
        odp_lpa_summary_wide = odp_lpa_summary_wide.sort_values(["cohort", "organisation_name"]).reset_index(drop=True)

    # Add missing org+pipeline combos to detail CSV (ODP-only, same as before - there's no
    # equivalent "expected provision" list in the `provision` table to backfill against for
    # mandated datasets, which only ever appear in the detail CSV where they have live data)
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
        missing_detail_rows["has_zero_entities"] = np.nan
        missing_detail_rows["quality_level_label"] = "0. no data"
        odp_qual_summary = pd.concat([odp_qual_summary, missing_detail_rows], ignore_index=True)

    odp_qual_summary = odp_qual_summary.sort_values(["pipeline", "organisation"]).reset_index(drop=True)

    # quality_level_label last, matching the notebook's column ordering
    front_cols = ["pipeline", "cohort", "start_date", "organisation", "organisation_name"]
    other_cols = [c for c in odp_qual_summary.columns if c not in front_cols and c != "quality_level_label"]
    odp_qual_summary = odp_qual_summary[front_cols + other_cols + ["quality_level_label"]]

    out_scores = os.path.join(output_dir, "quality_ODP_mandated_dataset_scores_by_LPA.csv")
    out_detail = os.path.join(output_dir, "quality_ODP_mandated_dataset_quality_detail.csv")

    odp_lpa_summary_wide.to_csv(out_scores, index=False)
    odp_qual_summary.to_csv(out_detail, index=False)

    print(f"Saved {out_scores} ({len(odp_lpa_summary_wide)} rows)")
    print(f"Saved {out_detail} ({len(odp_qual_summary)} rows)")


if __name__ == "__main__":
    main()
