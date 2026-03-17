
"""
Builds two ODP quality CSV reports by combining active endpoint issue data, provision/organisation lookups, and geospatial checks against LPA boundaries.
It maps issues to quality criteria, calculates provider-dataset quality levels plus criteria pass/fail detail, writes the two CSV outputs.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import urllib.parse
import urllib.request

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


def query_sqlite(db_path: str, sql: str) -> pd.DataFrame:
    with sqlite3.connect(db_path) as con:
        cur = con.execute(sql)
        cols = [c[0] for c in cur.description]
        return pd.DataFrame.from_records(cur.fetchall(), columns=cols)


def download_performance_db(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    urllib.request.urlretrieve("https://datasette.planning.data.gov.uk/performance.db", path)


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

    perf_db = os.path.join(output_dir, "_db_downloads", "performance.db")
    download_performance_db(perf_db)

    issue_lookup = datasette_query(
        "digital-land",
        """
        SELECT issue_type,
               quality_criteria_level || ' - ' || quality_criteria AS quality_criteria,
               quality_criteria_level AS quality_level
        FROM issue_type
        """,
    )

    provision = datasette_query(
        "digital-land",
        "SELECT * FROM provision WHERE project = 'open-digital-planning'",
    ).rename(columns={"dataset": "pipeline"})

    endpoint_issues = query_sqlite(
        perf_db,
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
        provision[["organisation", "pipeline", "cohort"]],
        how="inner",
        on=["organisation", "pipeline"],
    )

    odp_lpa_summary_wide = (
        odp_lpa_summary.pivot(
            columns="pipeline",
            values="quality_level_label",
            index=["cohort", "organisation", "organisation_name"],
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

    out_scores = os.path.join(output_dir, "quality_ODP_dataset_scores_by_LPA.csv")
    out_detail = os.path.join(output_dir, "quality_ODP_dataset_quality_detail.csv")

    odp_lpa_summary_wide.to_csv(out_scores, index=False)
    odp_qual_summary.to_csv(out_detail, index=False)

    print(f"Saved {out_scores} ({len(odp_lpa_summary_wide)} rows)")
    print(f"Saved {out_detail} ({len(odp_qual_summary)} rows)")

    # Clean up temporary database download.
    if os.path.exists(perf_db):
        os.remove(perf_db)

    perf_db_dir = os.path.dirname(perf_db)
    if os.path.isdir(perf_db_dir) and not os.listdir(perf_db_dir):
        os.rmdir(perf_db_dir)


if __name__ == "__main__":
    main()
