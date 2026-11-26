"""
Entities with facts from two or more active LPAs.

An entity should get its facts from one LPA. When facts from two active LPAs land on the
same entity it is usually our own config: a source registered to the wrong organisation,
or a redirect (old-entity.csv) joining two LPAs' entities. Facts from organisations with
an end_date are ignored, so councils merged by local government reorganisation don't count.

Facts are traced through fact_resource -> resource_organisation. fact_resource already has
redirects applied, so entities that were redirected into one another are included.

Only LPAs are compared. Counties, combined authorities and the GLA are left out: their
regional registers overlap LPA data by design, and all the cases found were historic.
"""

import argparse
import logging
import os
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from requests.exceptions import HTTPError

from utils import (
    DATASETTE_URL,
    datasette_query,
    fetch_datasette_csv_table,
    files_url,
    get_http_session,
    get_with_retry,
    read_csv_with_retry,
)

logger = logging.getLogger(__name__)

OUTPUT_FILE = "entities_conflicting_organisations.csv"
ENTITY_URL = "https://www.planning.data.gov.uk/entity/"

REDIRECT = "redirect from another organisation's entity"
REGISTRATION = "file registered to an organisation with no lookup for this entity"
BOTH_LOOKUP = "each organisation has a lookup for this entity"

# kept small so each query stays well inside datasette's SQL time limit, which
# utils.get_with_retry would otherwise retry with a long backoff
FILES_PER_QUERY = 40
IDS_PER_QUERY = 20
HASHES_PER_QUERY = 50
# runs alongside the other reports under make -j, so go easy on datasette
WORKERS = 4

OUTPUT_COLUMNS = [
    "dataset",
    "entity",
    "reference",
    "name",
    "organisations",
    "organisation_names",
    "current_organisation",
    "likely_cause",
    "organisations_without_lookup",
    "organisation_files",
    "redirected_from",
    "redirect_entry_date",
    "entity_url",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Report entities with facts from two or more active LPAs."
    )
    parser.add_argument("--output-dir", type=str, required=True, help="Directory to save the output CSV")
    return parser.parse_args()


def chunks(values, size):
    values = list(values)
    for i in range(0, len(values), size):
        yield values[i:i + size]


def quoted(values) -> str:
    return ",".join(f"'{v}'" for v in values)


def active_organisations(orgs: pd.DataFrame) -> pd.DataFrame:
    """Local authorities and LPAs without an end_date. is_lpa marks the LPAs (anything with a
    local_planning_authority code, which includes national parks and some development
    corporations), the only organisations compared. The other local authorities (counties,
    combined authorities, the GLA) are kept to find shared files. Government bodies are out of
    scope: their national compilations are expected to overlap with LPA data."""
    is_lpa = orgs["local_planning_authority"].fillna("") != ""
    in_scope = is_lpa | (orgs["dataset"] == "local-authority")
    active = orgs[in_scope & (orgs["end_date"].fillna("") == "")].copy()
    active["is_lpa"] = is_lpa[active.index]
    return active[["organisation", "entity", "name", "is_lpa"]].reset_index(drop=True)


def in_scope_files(
    resource_org: pd.DataFrame, resource_dataset: pd.DataFrame, active: pd.DataFrame, published: set
) -> pd.DataFrame:
    """Each active LPA's files, by dataset, for datasets with two or more LPAs.

    A file registered to several active organisations can't show which of them supplied a
    fact, so it is left out. That includes other local authorities: otherwise the GLA's
    London-wide file, also registered to Southwark, would count as Southwark's alone."""
    files = resource_org[["resource", "organisation"]].copy()
    files["organisation"] = files["organisation"].str.replace("-eng", "", regex=False)
    files = files[files["organisation"].isin(active["organisation"])].drop_duplicates()
    files = files[files.groupby("resource")["organisation"].transform("nunique") == 1]
    files = files[files["organisation"].isin(active.loc[active["is_lpa"], "organisation"])]
    files = files.merge(resource_dataset[["resource", "dataset"]], on="resource")
    files = files[files["dataset"].isin(published)].drop_duplicates()
    files = files[files.groupby("dataset")["organisation"].transform("nunique") > 1]
    return files[["dataset", "organisation", "resource"]].reset_index(drop=True)


def find_conflicts(entity_orgs: pd.DataFrame, active: pd.DataFrame) -> pd.DataFrame:
    """Entities with facts from more than one active LPA. entity_orgs has a row per dataset,
    entity and organisation with facts for it."""
    lpas = active.loc[active["is_lpa"], ["organisation", "name"]]
    d = entity_orgs.merge(lpas, on="organisation")
    d = d.drop_duplicates(["dataset", "entity", "organisation"]).sort_values("organisation")
    conflicts = d.groupby(["dataset", "entity"]).agg(
        n_orgs=("organisation", "size"),
        organisations=("organisation", "; ".join),
        organisation_names=("name", "; ".join),
    ).reset_index()
    conflicts = conflicts[conflicts["n_orgs"] > 1]
    return conflicts.drop(columns=["n_orgs"]).reset_index(drop=True)


def likely_cause(organisations, entity_lookup_orgs, old_entity_lookup_orgs) -> str:
    """Why facts from these organisations are on one entity, from which of them have a
    lookup.csv row for the entity, or only for an old entity redirected into it."""
    organisations = set(organisations)
    if organisations & (set(old_entity_lookup_orgs) - set(entity_lookup_orgs)):
        return REDIRECT
    if organisations - set(entity_lookup_orgs) - set(old_entity_lookup_orgs):
        return REGISTRATION
    return BOTH_LOOKUP


def build_report(
    conflicts: pd.DataFrame,
    entities: pd.DataFrame,
    org_files: pd.DataFrame,
    redirects: pd.DataFrame,
    lookup: pd.DataFrame,
    orgs: pd.DataFrame,
) -> pd.DataFrame:
    """
    conflicts: from find_conflicts
    entities: dataset, entity, name, reference, organisation_entity
    org_files: dataset, entity, organisation, start_date, end_date - each file an organisation
        supplied the entity's facts in
    redirects: dataset, old_entity, entity, entry_date - 301s into the entity
    lookup: dataset, organisation, entity - lookup.csv rows for the entities and their old entities
    orgs: the organisation table, for names
    """
    if conflicts.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    key = ["dataset", "entity"]
    names = orgs.set_index("organisation")["name"]
    report = conflicts.merge(entities, on=key, how="left")
    report["current_organisation"] = report["organisation_entity"].astype(str).map(
        orgs.astype({"entity": str}).set_index("entity")["name"]
    )
    report["entity_url"] = ENTITY_URL + report["entity"].astype(str)

    files = org_files.copy()
    files["current"] = files["end_date"].fillna("") == ""
    per_org = files.groupby(key + ["organisation"]).agg(
        latest=("start_date", "max"), current=("current", "any")
    ).reset_index()
    per_org["text"] = (
        per_org["organisation"].map(names) + ": latest file " + per_org["latest"].fillna("")
        + per_org["current"].map({True: " (current)", False: ""})
    )
    summary = per_org.sort_values("organisation").groupby(key).agg(
        organisation_files=("text", "; ".join)
    )
    report = report.merge(summary.reset_index(), on=key, how="left")

    redirects = redirects.sort_values("old_entity")
    old = redirects.groupby(key).agg(
        redirected_from=("old_entity", lambda s: "; ".join(map(str, s))),
        redirect_entry_date=("entry_date", "max"),
    ).reset_index()
    report = report.merge(old, on=key, how="left")

    by_entity = lookup.groupby(key)["organisation"].agg(set)
    old_orgs = redirects.merge(
        lookup.rename(columns={"entity": "old_entity"}), on=["dataset", "old_entity"]
    ).groupby(key)["organisation"].agg(set)

    causes, missing = [], []
    for row in report.itertuples():
        row_orgs = row.organisations.split("; ")
        mapped = by_entity.get((row.dataset, row.entity), set())
        old_mapped = old_orgs.get((row.dataset, row.entity), set())
        causes.append(likely_cause(row_orgs, mapped, old_mapped))
        missing.append("; ".join(names[o] for o in row_orgs if o not in mapped | old_mapped))
    report["likely_cause"] = causes
    report["organisations_without_lookup"] = missing

    report = report.sort_values(["dataset", "entity"])
    return report[OUTPUT_COLUMNS].reset_index(drop=True)


def published_datasets() -> set:
    response = get_with_retry(get_http_session(), f"{DATASETTE_URL}/-/databases.json")
    return {d["name"] for d in response.json()} - {"digital-land", "performance"}


def entities_for_files(dataset: str, resources: list) -> set:
    """Entities with facts from these files. A query that still times out after
    get_with_retry's retries is split in half."""
    sql = f"SELECT GROUP_CONCAT(DISTINCT entity) AS e FROM fact_resource WHERE resource IN ({quoted(resources)})"
    try:
        e = datasette_query(dataset, sql)["e"][0]
    except HTTPError:
        if len(resources) == 1:
            raise
        half = len(resources) // 2
        return entities_for_files(dataset, resources[:half]) | entities_for_files(dataset, resources[half:])
    return {int(i) for i in e.split(",")} if e else set()


def fetch_entity_orgs(files: pd.DataFrame) -> pd.DataFrame:
    jobs = [
        (dataset, organisation, batch)
        for (dataset, organisation), g in files.groupby(["dataset", "organisation"])
        for batch in chunks(sorted(g["resource"]), FILES_PER_QUERY)
    ]
    logger.info("Querying %d batches of files across %d datasets", len(jobs), files["dataset"].nunique())
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        results = list(pool.map(lambda job: entities_for_files(job[0], job[2]), jobs))
    rows = [(dataset, organisation, e) for (dataset, organisation, _), ents in zip(jobs, results) for e in ents]
    return pd.DataFrame(rows, columns=["dataset", "organisation", "entity"]).drop_duplicates()


def query_in_chunks(db: str, sql: str, values, size: int) -> pd.DataFrame:
    """Run sql, which has an {ids} placeholder, for each chunk of values."""
    frames = [datasette_query(db, sql.format(ids=",".join(map(str, chunk)))) for chunk in chunks(values, size)]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def fetch_details(conflicts: pd.DataFrame, files: pd.DataFrame, collections: pd.Series):
    entities, org_files, redirects, lookups = [], [], [], []
    for dataset, c in conflicts.groupby("dataset"):
        ids = sorted(c["entity"])
        entities.append(query_in_chunks(
            dataset,
            "SELECT entity, name, reference, organisation_entity FROM entity WHERE entity IN ({ids}) ORDER BY entity",
            ids, IDS_PER_QUERY,
        ).assign(dataset=dataset))
        # through fact, as fact_resource has no index on entity
        fr = query_in_chunks(
            dataset,
            "SELECT f.entity, fr.resource FROM fact f JOIN fact_resource fr ON fr.fact = f.fact "
            "WHERE f.entity IN ({ids}) GROUP BY f.entity, fr.resource ORDER BY f.entity, fr.resource",
            ids, IDS_PER_QUERY,
        )
        fr = fr.merge(files.loc[files["dataset"] == dataset, ["resource", "organisation"]], on="resource")
        org_files.append(fr.assign(dataset=dataset))
        old = query_in_chunks(
            dataset,
            "SELECT old_entity, entity, entry_date FROM old_entity WHERE status = '301' AND entity IN ({ids}) "
            "ORDER BY old_entity",
            ids, IDS_PER_QUERY,
        )
        if old.empty:
            old = pd.DataFrame(columns=["old_entity", "entity", "entry_date"])
        old["old_entity"] = old["old_entity"].astype(int)
        redirects.append(old.assign(dataset=dataset))

        lookup_url = files_url(f"config/pipeline/{collections[dataset]}/lookup.csv")
        try:
            lookup = read_csv_with_retry(lookup_url, dtype=str, keep_default_na=False,
                                         usecols=["prefix", "organisation", "entity"])
        except Exception as error:  # report the conflicts without causes rather than fail
            logger.warning("Couldn't read %s: %s", lookup_url, error)
            continue
        lookup = lookup[(lookup["prefix"] == dataset) & (lookup["entity"] != "")]
        lookup["entity"] = lookup["entity"].astype(int)
        lookup = lookup[lookup["entity"].isin(set(ids) | set(old["old_entity"]))]
        lookups.append(lookup.assign(dataset=dataset)[["dataset", "organisation", "entity"]])

    org_files = pd.concat(org_files, ignore_index=True)
    resources = sorted(org_files["resource"].unique())
    dates = pd.concat([
        datasette_query(
            "digital-land",
            f"SELECT resource, start_date, end_date FROM resource "
            f"WHERE resource IN ({quoted(chunk)}) ORDER BY resource",
        )
        for chunk in chunks(resources, HASHES_PER_QUERY)
    ], ignore_index=True)
    org_files = org_files.merge(dates, on="resource", how="left")

    lookup = pd.concat(lookups, ignore_index=True) if lookups else pd.DataFrame(
        columns=["dataset", "organisation", "entity"]
    )
    return pd.concat(entities, ignore_index=True), org_files, pd.concat(redirects, ignore_index=True), lookup


def main(output_dir):
    orgs = fetch_datasette_csv_table("organisation", dtype=str, keep_default_na=False)
    active = active_organisations(orgs)
    resource_org = fetch_datasette_csv_table("resource_organisation", dtype=str, keep_default_na=False)
    resource_dataset = fetch_datasette_csv_table("resource_dataset", dtype=str, keep_default_na=False)
    datasets = fetch_datasette_csv_table("dataset", dtype=str, keep_default_na=False)
    collections = datasets.set_index("dataset")["collection"]

    files = in_scope_files(resource_org, resource_dataset, active, published_datasets())
    entity_orgs = fetch_entity_orgs(files)
    conflicts = find_conflicts(entity_orgs, active)
    logger.info("%d entities checked, %d with conflicting organisations",
                entity_orgs[["dataset", "entity"]].drop_duplicates().shape[0], len(conflicts))

    if conflicts.empty:
        report = pd.DataFrame(columns=OUTPUT_COLUMNS)
    else:
        report = build_report(conflicts, *fetch_details(conflicts, files, collections), orgs)

    counts = report["dataset"].value_counts().to_dict() if not report.empty else {}
    print(f"Entities with facts from two active LPAs: {len(report)} {counts}")

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, OUTPUT_FILE)
    report.to_csv(output_path, index=False)
    print(f"CSV saved: {output_path}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    main(args.output_dir)
