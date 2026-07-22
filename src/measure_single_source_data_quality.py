"""
Builds a single-source dataset quality CSV report by combining active endpoint issue data,
organisation lookups, and each dataset's own entity-level `quality` signal to determine
authoritative sourcing. Single-source datasets are everything that isn't ODP-scoped or
"mandated" (see measure_odp_quality.py, which covers those with slightly different checks).
It maps issues to quality criteria, calculates provider-dataset quality levels on a 0-6 scale
(authoritative axis x rung axis) plus criteria pass/fail detail, applies a staleness cap
(a criterion specific to single-source datasets, which have no alternative source to cross-check
freshness against), and writes the detail CSV output.
"""

from __future__ import annotations

import argparse
import os
import urllib.parse

import numpy as np
import pandas as pd
from utils import read_csv_with_retry

STALENESS_AGE_DAYS = 365


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
    # using the platform's own per-entity `quality` field. Aggregated leniently: if ANY entity
    # attributed to an organisation is quality-tier "authoritative" or better, the whole
    # organisation+pipeline provision counts as authoritative.

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
    # or encouraged-for-LPAs - see measure_odp_quality.py, which covers those with different
    # checks). Computed from every dataset in provision_rule, not just active ones.
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

    quality_lookup = datasette_query("digital-land", "SELECT quality, priority FROM quality")
    quality_priority = dict(zip(quality_lookup["quality"], quality_lookup["priority"]))

    # restricted to single-source pipelines at the query itself, so ODP/mandated data never
    # enters this report
    quoted_pipelines = ", ".join(f"'{p}'" for p in single_source_pipelines)
    endpoint_issues = datasette_query_paginated(
        "performance",
        f"""
        SELECT rhe.organisation,
               rhe.name AS organisation_name,
               rhe.collection,
               rhe.pipeline,
               rhe.endpoint,
               rhe.resource,
               CAST(JULIANDAY('now') - JULIANDAY(rhe.resource_start_date) AS int) AS resource_age_days,
               its.issue_type
        FROM reporting_historic_endpoints rhe
        LEFT JOIN endpoint_dataset_issue_type_summary its ON rhe.resource = its.resource
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

    # Authoritative-source signal: query each active pipeline's own entity table for its
    # platform-computed `quality` field, keyed by organisation_entity.
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

    # ISSUES TABLE - flagging when provisions have data quality issues (authoritative status
    # and staleness are separate axes, handled below, not concatenated in here)
    qual_all = endpoint_issues.merge(issue_lookup, how="left", on="issue_type")[[
        "collection", "pipeline", "organisation", "organisation_name", "issue_type", "quality_criteria", "quality_level",
    ]]

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
        qual_all.groupby(["collection", "pipeline", "organisation", "organisation_name"], as_index=False, dropna=False)
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

    # staleness acts as another criterion gating the top rung: a stale provision can't be
    # "trustworthy" and gets capped down to "usable" instead, but a provision already at
    # "usable" or "some data" isn't pushed down any further. Provisions already at 0 ("no
    # data") are left alone - there's nothing left to downgrade.
    stale = endpoint_issues[endpoint_issues["resource_age_days"] > STALENESS_AGE_DAYS][["pipeline", "organisation"]].drop_duplicates()
    stale["is_stale"] = True

    qual_summary = qual_summary.merge(stale, how="left", on=["pipeline", "organisation"])
    qual_summary["is_stale"] = qual_summary["is_stale"].fillna(False)

    cap_mask = qual_summary["is_stale"] & (qual_summary["quality_level"] > 0)
    rung = np.where(qual_summary["is_authoritative"], qual_summary["quality_level"] - 3, qual_summary["quality_level"])
    capped_rung = np.minimum(rung, 2)
    capped_quality_level = np.where(qual_summary["is_authoritative"], capped_rung + 3, capped_rung)
    qual_summary.loc[cap_mask, "quality_level"] = capped_quality_level[cap_mask]
    qual_summary.loc[cap_mask, "quality_level_label"] = qual_summary.loc[cap_mask, "quality_level"].map(level_map)

    # bring in resource age for the detail output (not just the pass/fail is_stale flag) -
    # max across resources if an org has more than one for a pipeline
    age = endpoint_issues.groupby(["pipeline", "organisation"], as_index=False).agg(resource_age_days=("resource_age_days", "max"))
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

    # bring in the authoritative-source, zero-entity, and staleness checks (separate axes,
    # not part of the severity quality_criteria pivot above)
    qual_cat_summary_wide = qual_cat_summary_wide.merge(
        qual_summary[[
            "organisation", "pipeline", "is_authoritative", "authoritative_check_available",
            "has_zero_entities", "resource_age_days", "is_stale",
        ]].drop_duplicates(),
        how="left",
        on=["organisation", "pipeline"],
    )

    # criteria columns come out of the pivot as issue_flag (True = no issue), which reads
    # backwards against a column literally named e.g. "2 - duplicate reference values" - invert
    # to TRUE/FALSE strings so TRUE means "yes, this issue occurred", matching measure_odp_quality.py
    non_criteria_cols = [
        "pipeline", "organisation", "organisation_name", "resource_age_days", "quality_level_label",
        "is_authoritative", "authoritative_check_available", "has_zero_entities", "is_stale",
    ]
    qual_criteria_cols = [c for c in qual_cat_summary_wide.columns if c not in non_criteria_cols]
    flag_map = {True: "FALSE", False: "TRUE", 1: "FALSE", 0: "TRUE", 1.0: "FALSE", 0.0: "TRUE"}
    for col in qual_criteria_cols:
        qual_cat_summary_wide[col] = qual_cat_summary_wide[col].map(flag_map)

    # these are already true/false in their natural sense (unlike the issue_flag-derived
    # criteria columns above, which invert), so map straight through to TRUE/FALSE strings
    # for consistent CSV formatting rather than leaving them as Python True/False/blank.
    bool_cols = ["is_authoritative", "authoritative_check_available", "has_zero_entities", "is_stale"]
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
