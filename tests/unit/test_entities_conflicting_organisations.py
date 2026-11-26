import pandas as pd

from entities_conflicting_organisations import (
    BOTH_LOOKUP,
    OUTPUT_COLUMNS,
    REDIRECT,
    REGISTRATION,
    active_organisations,
    build_report,
    find_conflicts,
    in_scope_files,
    likely_cause,
)

ORG_COLUMNS = ["organisation", "entity", "name", "dataset", "local_planning_authority", "end_date"]
ORGS = pd.DataFrame([
    ["local-authority:MSS", "218", "Mid Sussex", "local-authority", "E60000218", ""],
    ["local-authority:MSU", "219", "Mid Suffolk", "local-authority", "E60000219", ""],
    ["local-authority:SCE", "282", "Scarborough", "local-authority", "E60000282", "2023-03-31"],
    ["local-authority:GLA", "174", "Greater London Authority", "local-authority", "", ""],
    ["government-organisation:PB1164", "16", "Historic England", "government-organisation", "", ""],
], columns=ORG_COLUMNS)
ACTIVE = active_organisations(ORGS)


def entity_orgs(*rows):
    return pd.DataFrame(list(rows), columns=["dataset", "entity", "organisation"])


def test_active_organisations_drops_ended_and_government_organisations_and_marks_lpas():
    assert ACTIVE["organisation"].tolist() == ["local-authority:MSS", "local-authority:MSU", "local-authority:GLA"]
    assert ACTIVE["is_lpa"].tolist() == [True, True, False]


def test_in_scope_files_keeps_unshared_lpa_files_in_datasets_with_two_lpas():
    resource_org = pd.DataFrame([
        ["r1", "local-authority:MSS"],
        ["r2", "local-authority:MSU-eng"],
        ["r3", "local-authority:MSS"],
        ["r3", "local-authority:MSU"],  # shared, can't tell who supplied the facts
        ["r4", "local-authority:SCE"],  # ended
        ["r5", "local-authority:MSS"],  # only organisation in its dataset
        ["r6", "local-authority:MSS"],  # not published
        ["r7", "local-authority:MSS"],
        ["r7", "local-authority:GLA"],  # shared with a non-LPA, still can't tell who supplied the facts
        ["r8", "local-authority:GLA"],  # not an LPA
    ], columns=["resource", "organisation"])
    resource_dataset = pd.DataFrame([
        ["r1", "brownfield-land"], ["r2", "brownfield-land"], ["r3", "brownfield-land"],
        ["r4", "brownfield-land"], ["r5", "tree"], ["r6", "unpublished"],
        ["r7", "brownfield-land"], ["r8", "brownfield-land"],
    ], columns=["resource", "dataset"])

    files = in_scope_files(resource_org, resource_dataset, ACTIVE, {"brownfield-land", "tree"})

    assert files.values.tolist() == [
        ["brownfield-land", "local-authority:MSS", "r1"],
        ["brownfield-land", "local-authority:MSU", "r2"],
    ]


def test_find_conflicts_flags_entities_with_two_active_lpas_only():
    conflicts = find_conflicts(entity_orgs(
        ["brownfield-land", 1, "local-authority:MSS"],
        ["brownfield-land", 1, "local-authority:MSU"],
        ["brownfield-land", 2, "local-authority:MSS"],
        ["brownfield-land", 2, "local-authority:GLA"],  # not an LPA, so not a conflict
        ["brownfield-land", 3, "local-authority:MSS"],
        ["brownfield-land", 3, "local-authority:SCE"],  # ended, so not a conflict
    ), ACTIVE)

    assert conflicts["entity"].tolist() == [1]
    assert conflicts["organisations"].iloc[0] == "local-authority:MSS; local-authority:MSU"
    assert conflicts["organisation_names"].iloc[0] == "Mid Sussex; Mid Suffolk"


def test_likely_cause():
    a, b = "local-authority:MSS", "local-authority:MSU"
    assert likely_cause([a, b], {a}, {b}) == REDIRECT
    assert likely_cause([a, b], {a}, set()) == REGISTRATION
    assert likely_cause([a, b], {a, b}, set()) == BOTH_LOOKUP


def test_build_report():
    conflicts = find_conflicts(entity_orgs(
        ["conservation-area", 44000002, "local-authority:MSS"],
        ["conservation-area", 44000002, "local-authority:MSU"],
        ["brownfield-land", 1700001, "local-authority:MSS"],
        ["brownfield-land", 1700001, "local-authority:MSU"],
    ), ACTIVE)
    entities = pd.DataFrame([
        ["conservation-area", 44000002, "Thame", "9970", "218"],
        ["brownfield-land", 1700001, "Site", "BF1", "218"],
    ], columns=["dataset", "entity", "name", "reference", "organisation_entity"])
    org_files = pd.DataFrame([
        ["conservation-area", 44000002, "local-authority:MSS", "2024-10-03", ""],
        ["conservation-area", 44000002, "local-authority:MSU", "2020-09-05", "2025-06-13"],
        ["conservation-area", 44000002, "local-authority:MSU", "2025-04-08", "2025-06-13"],
        ["brownfield-land", 1700001, "local-authority:MSS", "2025-01-07", ""],
        ["brownfield-land", 1700001, "local-authority:MSU", "2020-12-22", "2022-11-09"],
    ], columns=["dataset", "entity", "organisation", "start_date", "end_date"])
    redirects = pd.DataFrame([["conservation-area", 44000001, 44000002, "2025-11-10"]],
                             columns=["dataset", "old_entity", "entity", "entry_date"])
    lookup = pd.DataFrame([
        ["conservation-area", "local-authority:MSS", 44000002],
        ["conservation-area", "local-authority:MSU", 44000001],
        ["brownfield-land", "local-authority:MSS", 1700001],
    ], columns=["dataset", "organisation", "entity"])

    report = build_report(conflicts, entities, org_files, redirects, lookup, ORGS)

    assert report.columns.tolist() == OUTPUT_COLUMNS
    assert report["entity"].tolist() == [1700001, 44000002]
    bfl, ca = report.iloc[0], report.iloc[1]

    assert ca["likely_cause"] == REDIRECT
    assert ca["redirected_from"] == "44000001"
    assert ca["redirect_entry_date"] == "2025-11-10"
    assert ca["current_organisation"] == "Mid Sussex"
    assert ca["organisation_files"] == (
        "Mid Sussex: latest file 2024-10-03 (current); Mid Suffolk: latest file 2025-04-08"
    )
    assert ca["organisations_without_lookup"] == ""
    assert ca["entity_url"] == "https://www.planning.data.gov.uk/entity/44000002"

    assert bfl["likely_cause"] == REGISTRATION
    assert bfl["organisations_without_lookup"] == "Mid Suffolk"
    assert pd.isna(bfl["redirected_from"])


def test_build_report_with_no_conflicts_has_the_output_columns():
    empty = find_conflicts(entity_orgs(["tree", 1, "local-authority:MSS"]), ACTIVE)
    report = build_report(empty, None, None, None, None, ORGS)
    assert report.empty
    assert report.columns.tolist() == OUTPUT_COLUMNS
