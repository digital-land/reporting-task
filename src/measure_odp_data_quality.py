
"""
Builds two ODP quality CSV reports by combining active endpoint issue data, provision/organisation lookups, and geospatial checks against LPA boundaries.
It maps issues to quality criteria, calculates provider-dataset quality levels plus criteria pass/fail detail, writes the two CSV outputs.
"""

from __future__ import annotations

import argparse
import os
import urllib.parse

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely.wkt

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
    return pd.read_csv(f"https://datasette.planning.data.gov.uk/{db}.csv?{params}")


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


def get_pdp_gdf(dataset: str, geometry_field: str) -> gpd.GeoDataFrame:
    df = pd.read_csv(f"https://files.planning.data.gov.uk/dataset/{dataset}.csv", dtype="str")
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

    ca_gdf = get_pdp_gdf("conservation-area", "point")
    ca_gdf[["organisation_entity"]] = ca_gdf[["organisation_entity"]].astype(int)

    lpa_gdf = get_pdp_gdf("local-planning-authority", "geometry").rename(
        columns={"reference": "LPACD", "name": "lpa_name"}
    )

    lpa_live = lpa_gdf[["LPACD", "geometry"]].merge(
        org_lookup[org_lookup["end_date"].isnull()][["LPACD", "organisation", "organisation_name", "organisation_entity"]],
        how="inner",
        on="LPACD",
    )

    base = lpa_live[["LPACD", "organisation"]].merge(endpoint_issues, how="outer", on="organisation")

    ca_gdf = ca_gdf.merge(
        org_lookup[["organisation_entity", "organisation_name", "lpa_flag"]],
        how="left",
        on="organisation_entity",
    )

    lpa_ca_join = gpd.sjoin(
        lpa_live[["LPACD", "organisation", "organisation_name", "geometry"]],
        ca_gdf[["entity", "organisation_entity", "lpa_flag", "point"]],
        how="inner",
        predicate="intersects",
    )

    qual_prov = (
        lpa_ca_join.groupby(["LPACD", "organisation", "organisation_name"], as_index=False)
        .agg(prov_rank_max=("lpa_flag", "max"))
        .query("prov_rank_max == 0")
    )
    qual_prov[["collection", "pipeline"]] = "conservation-area"
    qual_prov["issue_type"] = "non_auth"
    qual_prov["quality_criteria"] = "1 - authoritative data from the LPA"
    qual_prov["quality_level"] = 1
    qual_prov = qual_prov[["LPACD", "collection", "pipeline", "organisation", "organisation_name", "issue_type", "quality_criteria", "quality_level"]]

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

    qual_all = pd.concat([qual_prov, qual_match, qual_bounds, qual_issues], ignore_index=True)

    level_map = {
        4: "4. data that is trustworthy",
        3: "3. data that is good for ODP",
        2: "2. authoritative data from the LPA",
        1: "1. some data",
    }

    qual_summary = (
        qual_all.groupby(["LPACD", "pipeline", "organisation", "organisation_name"], as_index=False, dropna=False)
        .agg(quality_level=("quality_level", "min"))
    )
    qual_summary["quality_level"] = qual_summary["quality_level"].replace(np.nan, 4)
    qual_summary["quality_level_label"] = qual_summary["quality_level"].map(level_map)

    odp_lpa_summary = qual_summary.merge(
        provision[["organisation", "pipeline", "cohort", "start_date"]],
        how="inner",
        on=["organisation", "pipeline"],
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
    odp_lpa_summary_wide.replace(np.nan, "0. no data", inplace=True)

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
        (ready["area_dataset_count"] == 5) & (ready["min_quality_level"] >= 2),
        "yes",
        "no",
    )
    odp_lpa_summary_wide = odp_lpa_summary_wide.merge(
        ready[["organisation", "ready_for_ODP_adoption"]],
        how="left",
        on="organisation",
    )

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

    odp_qual_summary = qual_cat_summary_wide[
        qual_cat_summary_wide["pipeline"].isin(ODP_DATASETS)
    ].copy()

    qual_criteria_cols = [c for c in odp_qual_summary.columns if c not in ["pipeline", "organisation", "organisation_name", "quality_level_label"]]
    flag_map = {True: "FALSE", False: "TRUE", 1: "FALSE", 0: "TRUE", 1.0: "FALSE", 0.0: "TRUE"}
    for col in qual_criteria_cols:
        odp_qual_summary[col] = odp_qual_summary[col].map(flag_map)

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

    # Add missing org+pipeline combos to detail CSV
    all_odp_org_pipeline = provision[["organisation", "pipeline"]].merge(
        org_lookup[["organisation", "organisation_name"]].drop_duplicates(),
        on="organisation",
        how="left"
    )[["organisation", "pipeline", "organisation_name"]].drop_duplicates()

    existing_org_pipeline = odp_qual_summary[["organisation", "pipeline"]].drop_duplicates()
    missing_org_pipeline = all_odp_org_pipeline[~all_odp_org_pipeline[["organisation", "pipeline"]].apply(tuple, axis=1).isin(
        existing_org_pipeline[["organisation", "pipeline"]].apply(tuple, axis=1)
    )]

    if len(missing_org_pipeline) > 0:
        missing_detail_rows = missing_org_pipeline.copy()
        # Get quality criteria columns
        qual_criteria = [col for col in odp_qual_summary.columns if col not in ["pipeline", "organisation", "organisation_name", "quality_level_label"]]
        for col in qual_criteria:
            missing_detail_rows[col] = np.nan
        missing_detail_rows["quality_level_label"] = "0. no data"
        odp_qual_summary = pd.concat([odp_qual_summary, missing_detail_rows], ignore_index=True)

    odp_qual_summary = odp_qual_summary.sort_values(["pipeline", "organisation"]).reset_index(drop=True)

    out_scores = os.path.join(output_dir, "quality_ODP_dataset_scores_by_LPA.csv")
    out_detail = os.path.join(output_dir, "quality_ODP_dataset_quality_detail.csv")

    odp_lpa_summary_wide.to_csv(out_scores, index=False)
    odp_qual_summary.to_csv(out_detail, index=False)

    print(f"Saved {out_scores} ({len(odp_lpa_summary_wide)} rows)")
    print(f"Saved {out_detail} ({len(odp_qual_summary)} rows)")


if __name__ == "__main__":
    main()
