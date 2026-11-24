"""
Builds an event-date history for every local plan's plan-timetable entities from authoritative
data, so users can track slippage (dates moving later) and confirm which plan events have held
steady.

A plan-timetable entity can be given several event-date values over time. Each value is a fact,
and fact_resource records every resource (snapshot of the publisher's data) the fact appeared in,
with the entry date it was given. The report replays those facts in order and writes the same
history in two layouts, so users can pick whichever suits them:

- long: one row per entity's first recorded event-date, plus one row per change
- wide: one row per entity - the FIRST event-date, then a REVISION column for each change

It also writes a plan-level report of how long each plan is taking from Gateway 1 to adoption.
"""

import argparse
import logging
import os
import re

import pandas as pd

from utils import datasette_query_paginated, fetch_datasette_csv_table

logger = logging.getLogger(__name__)

LONG_OUTPUT_FILE = "plan_timetable_event_date_history_long.csv"
WIDE_OUTPUT_FILE = "plan_timetable_event_date_history_wide.csv"
PLAN_DURATION_OUTPUT_FILE = "plan_timetable_plan_duration.csv"

# fact_resource.priority is 2 when the resource came from the organisation that owns the entity,
# and 1 (the default) otherwise - so priority 2 is the authoritative signal. It lines up with
# entity.quality: every priority 2 event-date fact belongs to an "authoritative" entity.
AUTHORITATIVE_PRIORITY = 2

# MHCLG owns plan-timetable entities it published on behalf of authorities, so its data passes
# the authoritative filter above but isn't an authority's own timetable - left out of both reports
EXCLUDED_ORGANISATIONS = ["government-organisation:D1342"]

# plan-timetable has no plan type of its own, but its plan field is a reference to a plan in one or
# more of these datasets
LOCAL_PLAN = "local-plan"
PLAN_DATASETS = [LOCAL_PLAN, "minerals-plan", "waste-plan", "supplementary-plan"]
INFERRED_LOCAL_PLAN = "local-plan (inferred)"
UNKNOWN_PLAN_TYPE = "unknown"

# county councils don't make local plans, only minerals and waste plans
COUNTY_ONS_PREFIX = "E10"

# plans that aren't local plans but which the plan datasets can't tell us about, either because the
# timetable's plan reference isn't in any of them or because a development plan document (DPD) is
# published as a local-plan. Core strategies count as local plans. Reviewed by hand, as plan names
# aren't a safe guide - e.g. a "MWLP" could be a combined minerals, waste and local plan.
EXCLUDED_PLANS = [
    # minerals and waste
    ("local-authority:MIK", "MWLP"),
    # supplementary plan
    ("local-authority:WFUA", "westmorland-and-furness-council-supplementary-plan"),
    # development plan documents
    ("local-authority:CON", "Cornwall-council-climate emergency DPD"),
    ("local-authority:CON", "Cornwall-council-site allocations DPD"),
    ("local-authority:CHI", "southbourne-allocation-DPD"),
    ("national-park-authority:Q72617988", "peak-district-national-park-authority-local-plan-dmp-2019"),
    ("local-authority:REI", "reigate-and-banstead-borough-council-local-plan-2012"),
    ("local-authority:SHR", "Adopted Site Allocations and Management of Development (SAMDev) Plan"),
]

# the wide layout always shows at least this many REVISION columns, adding more when an entity
# has been revised more often
MIN_REVISION_COLUMNS = 2

GATEWAY_1_EVENT = "gateway-1-self-assessment"
ADOPTION_EVENT = "adopted"

# plans are expected to be adopted within this long of Gateway 1 - counted in calendar months, as 30
# months isn't a fixed number of days
PLAN_MAKING_MONTHS = 30

EVENT_DATE_FACTS_SQL = f"""
SELECT fr.entity,
       fr.entry_date,
       fr.entry_number,
       fr.resource,
       f.value AS event_date
FROM fact_resource fr
JOIN fact f ON f.fact = fr.fact
WHERE fr.field = 'event-date'
  AND fr.priority = {AUTHORITATIVE_PRIORITY}
ORDER BY fr.entity, fr.entry_date, fr.resource, fr.entry_number
"""

# fields other than event-date, by line - joined to the event-date on the same line so they describe
# that version of the timetable (e.g. notes saying "consultation end date extended by 2 weeks"), and
# used on their own for the values the platform currently shows
LINE_FACTS_SQL = f"""
SELECT fr.entity,
       fr.resource,
       fr.entry_date,
       fr.entry_number,
       fr.field,
       f.value
FROM fact_resource fr
JOIN fact f ON f.fact = fr.fact
WHERE fr.field IN ('actual-date', 'notes')
  AND fr.priority = {AUTHORITATIVE_PRIORITY}
ORDER BY fr.entity, fr.resource, fr.entry_number, fr.field
"""

# every authoritative entity, including those with no event-date yet (often optional stages such
# as an examination pause that a council hasn't needed to schedule), with the entry date the
# platform shows for it
AUTHORITATIVE_ENTITIES_SQL = f"""
SELECT entity,
       max(entry_date) AS entry_date
FROM fact_resource
WHERE priority = {AUTHORITATIVE_PRIORITY}
GROUP BY entity
ORDER BY entity
"""

ENTITY_SQL = """
SELECT entity,
       reference,
       organisation_entity,
       json_extract(json, '$.plan') AS plan,
       json_extract(json, '$.plan-event') AS plan_event
FROM entity
ORDER BY entity
"""

PLANS_SQL = """
SELECT reference,
       name,
       organisation_entity
FROM entity
ORDER BY entity
"""

# start_date on the digital-land resource table is the date the resource was first collected.
# plan-timetable's own dataset_resource.start_date is empty, so it can't be used instead.
RESOURCE_COLLECTED_SQL = """
SELECT r.resource, r.start_date AS collected_date
FROM resource r
JOIN resource_dataset rd ON rd.resource = r.resource
WHERE rd.dataset = 'plan-timetable'
ORDER BY r.resource
"""

LONG_COLUMNS = [
    "entity",
    "reference",
    "organisation",
    "organisation_name",
    "ons_code",
    "plan",
    "plan_type",
    "plan_event",
    "entry_date",
    "previous_event_date",
    "new_event_date",
    "direction",
    "days_moved",
    "status",
    "actual_date",
    "notes",
]

FACT_ORDER = ["entity", "entry_date", "collected_date", "entry_number"]

NO_EVENT_DATE = "no event-date"


def ons_codes(organisations: pd.DataFrame) -> pd.Series:
    """
    The ONS code of each organisation. Joint planning groups have none of their own, so they get
    their members' codes separated by semicolons.
    """
    codes = organisations.set_index("organisation")["statistical_geography"].dropna()

    def ons_code(organisation):
        if pd.notna(organisation["statistical_geography"]):
            return organisation["statistical_geography"]
        members = organisation["organisations"].split(";") if pd.notna(organisation["organisations"]) else []
        return ";".join(codes[member] for member in members if member in codes) or None

    return organisations.apply(ons_code, axis=1)


def plan_key(value: str) -> str:
    """A plan reference or name without the case, spaces and punctuation publishers vary."""
    return re.sub(r"[^a-z0-9]", "", str(value).lower())


def plan_types(entities: pd.DataFrame, plans: pd.DataFrame) -> pd.Series:
    """
    The plan datasets each entity's plan appears in, separated by semicolons, as one plan can be a
    local, minerals and waste plan all at once. Where it isn't in any, a plan whose reference says
    it's a local plan and doesn't mention minerals or waste is an inferred local plan, and anything
    else is unknown.

    plans needs reference, name, organisation_entity and dataset. Publishers don't always use the
    plan's reference, so a plan is first looked for among the organisation's own by reference or
    name, ignoring case, spaces and punctuation. Joint plans may only be published by another of
    the authorities, so failing that the exact reference is looked for anywhere.
    """
    own_plans = {}
    any_plans = {}
    for plan in plans.itertuples():
        any_plans.setdefault(plan.reference, set()).add(plan.dataset)
        for value in (plan.reference, plan.name):
            if pd.notna(value):
                own_plans.setdefault((plan.organisation_entity, plan_key(value)), set()).add(plan.dataset)

    def plan_type(entity):
        if pd.isna(entity["plan"]):
            return UNKNOWN_PLAN_TYPE
        own = own_plans.get((entity["organisation_entity"], plan_key(entity["plan"])))
        datasets = own or any_plans.get(entity["plan"], set())
        if datasets:
            return ";".join(dataset for dataset in PLAN_DATASETS if dataset in datasets)
        key = plan_key(entity["plan"])
        if "localplan" in key and "mineral" not in key and "waste" not in key:
            return INFERRED_LOCAL_PLAN
        return UNKNOWN_PLAN_TYPE

    return entities.apply(plan_type, axis=1)


def exclusion_reasons(entities: pd.DataFrame) -> pd.Series:
    """
    Why each entity is left out of the reports, or blank if it belongs in them.

    entities needs organisation, ons_code, plan and plan_type. A plan of unknown type is kept, as
    most are local plans whose timetable doesn't use the plan's reference - a plan is never left
    out because of its name alone.
    """
    def exclusion_reason(entity):
        if entity["organisation"] in EXCLUDED_ORGANISATIONS:
            return "excluded organisation"
        if (entity["organisation"], entity["plan"]) in EXCLUDED_PLANS:
            return "excluded plan"
        if not {LOCAL_PLAN, INFERRED_LOCAL_PLAN, UNKNOWN_PLAN_TYPE} & set(entity["plan_type"].split(";")):
            return "not a local plan"
        if pd.notna(entity["ons_code"]) and all(
            code.startswith(COUNTY_ONS_PREFIX) for code in entity["ons_code"].split(";")
        ):
            return "county council"
        return ""

    return entities.apply(exclusion_reason, axis=1)


def order_facts(facts: pd.DataFrame) -> pd.DataFrame:
    """
    Order event-date observations into each entity's history.

    Ordered by entry_date, as the platform does when picking an entity's current value.
    entry_date is often supplied by the publisher rather than being the collection date, and
    publishers have changed an event-date without updating its entry-date, so ties are broken by
    when the resource was collected, then by line in the file.
    """
    return facts.sort_values(FACT_ORDER)


def resource_snapshots(facts: pd.DataFrame) -> pd.DataFrame:
    """
    One event-date per entity per resource, in history order.

    facts needs columns: entity, resource, entry_date, entry_number, collected_date, event_date,
    notes - one row per resource an event-date fact appeared in, with notes from the same line
    (or blank).

    Where a resource has two lines for the same entity, the one with the later entry date (then
    the later line) is kept, as the platform would.
    """
    return order_facts(order_facts(facts).drop_duplicates(["entity", "resource"], keep="last"))


def event_date_changes(facts: pd.DataFrame) -> pd.DataFrame:
    """
    Reduce event-date observations to each entity's first event-date and every change after it.

    Runs of the same value across consecutive resources collapse into one, so re-collecting
    unchanged data doesn't count as a change, but a value that changes and then changes back -
    even under the same entry date - is two changes. Each row keeps the notes from the resource
    where its event-date first appeared.
    """
    snapshots = resource_snapshots(facts).reset_index(drop=True)
    snapshots["previous_event_date"] = snapshots.groupby("entity")["event_date"].shift()
    is_first = snapshots["previous_event_date"].isna()
    return snapshots[is_first | (snapshots["event_date"] != snapshots["previous_event_date"])].copy()


def current_line_fields(line_facts: pd.DataFrame) -> pd.DataFrame:
    """
    The actual_date and notes the platform currently shows for each entity.

    line_facts needs entity, entry_date, collected_date, entry_number, actual_date, notes - one
    row per line with either, whether or not that line has an event-date, as the platform takes
    them from any line. last skips blanks, matching the platform: it keeps each field's latest
    value separately, and a later upload leaving notes or actual-date blank doesn't clear one
    given earlier.
    """
    return order_facts(line_facts).groupby("entity")[["actual_date", "notes"]].last()


def build_event_date_history(facts: pd.DataFrame, undated: pd.DataFrame = None) -> pd.DataFrame:
    """
    Long layout: a first recorded row per entity, plus one row per change, with its direction,
    how many days it moved and the notes from the upload that made it. status is "changed" on
    every row of an entity whose event-date has ever changed, so either set can be filtered.

    undated (entity, entry_date, notes) lists entities with no event-date at all; each gets a
    single row with status "no event-date", so every plan event appears.
    """
    columns = ["entity", "entry_date", "previous_event_date", "new_event_date",
               "direction", "days_moved", "status", "notes"]
    undated_rows = pd.DataFrame(columns=columns) if undated is None else (
        undated[["entity", "entry_date", "notes"]].assign(status=NO_EVENT_DATE).reindex(columns=columns)
    )
    if facts.empty:
        return undated_rows.reset_index(drop=True)

    history = event_date_changes(facts)

    # event-dates are all ISO YYYY-MM-DD, so string comparison orders them correctly
    history["direction"] = "first recorded"
    history.loc[history["event_date"] > history["previous_event_date"], "direction"] = "moved later"
    history.loc[history["event_date"] < history["previous_event_date"], "direction"] = "moved earlier"
    # signed, so slippage is positive and pulling a date forward is negative; blank when first recorded
    history["days_moved"] = (
        pd.to_datetime(history["event_date"]) - pd.to_datetime(history["previous_event_date"])
    ).dt.days.astype("Int64")

    changed_entities = history.loc[history["previous_event_date"].notna(), "entity"].unique()
    history["status"] = history["entity"].isin(changed_entities).map({True: "changed", False: "unchanged"})

    history = history.rename(columns={"event_date": "new_event_date"})[columns]
    if undated_rows.empty:
        return history.reset_index(drop=True)
    return pd.concat([history, undated_rows], ignore_index=True)


def revision_columns(count: int) -> list:
    return ["FIRST"] + [f"REVISION {n}" for n in range(1, max(count, MIN_REVISION_COLUMNS) + 1)]


def build_event_date_timetable(
    facts: pd.DataFrame, line_facts: pd.DataFrame, undated: pd.DataFrame = None
) -> pd.DataFrame:
    """
    Wide layout: one row per entity - its FIRST event-date, each REVISION after it, and the
    entry-date, actual-date and notes the platform currently shows.

    facts needs the columns event_date_changes does, and line_facts the columns
    current_line_fields does. undated (entity, entry_date) lists entities with no event-date at
    all; each gets a row with the event-date columns blank, so every plan event appears.
    """
    if facts.empty:
        timetable = pd.DataFrame(columns=revision_columns(0), index=pd.Index([], name="entity"))
        entry_date = pd.Series(dtype=object, name="entry-date")
    else:
        revisions = event_date_changes(facts)
        revisions["revision"] = revisions.groupby("entity").cumcount()

        timetable = revisions.pivot(index="entity", columns="revision", values="event_date")
        columns = revision_columns(int(revisions["revision"].max()))
        timetable = timetable.reindex(columns=range(len(columns)))
        timetable.columns = columns

        entry_date = resource_snapshots(facts).groupby("entity")["entry_date"].last().rename("entry-date")

    if undated is not None and not undated.empty:
        timetable = timetable.reindex(timetable.index.union(pd.Index(undated["entity"], name="entity")))
        entry_date = entry_date.combine_first(undated.set_index("entity")["entry_date"]).rename("entry-date")

    current = current_line_fields(line_facts).rename(columns={"actual_date": "actual-date"})
    return timetable.join(entry_date).join(current).reset_index()


def timetable_date_columns(timetable: pd.DataFrame) -> list:
    return [column for column in timetable.columns if column == "FIRST" or column.startswith("REVISION ")]


def build_plan_durations(timetable: pd.DataFrame) -> pd.DataFrame:
    """
    One row per organisation and plan with a Gateway 1 or adoption event: how long from Gateway 1
    to the adoption date currently predicted, both from when Gateway 1 actually happened and from
    when it was first predicted to - so moving Gateway 1 doesn't hide how long a plan is taking.

    timetable is the wide layout. Dates and durations are blank where the timetable doesn't have
    them. Whether a plan is within 30 months is also blank if adoption isn't after Gateway 1, as
    one of the dates must be wrong.
    """
    plan_columns = ["organisation", "organisation-name", "ons-code", "plan", "plan-type"]
    events = timetable[timetable["plan-event"].isin([GATEWAY_1_EVENT, ADOPTION_EVENT]) & timetable["plan"].notna()]
    events = events.assign(current=events[timetable_date_columns(events)].ffill(axis=1).iloc[:, -1])
    # an event given a new reference is a new entity, so a plan could have more than one: its first
    # prediction is the oldest entity's and its current dates are the newest's
    events = events.sort_values(["entry-date", "entity"])

    def event_dates(event, columns):
        dates = events[events["plan-event"] == event].groupby(["organisation", "plan"]).agg(**columns)
        return dates.reset_index()

    durations = events[plan_columns].drop_duplicates(["organisation", "plan"], keep="last")
    durations = durations.merge(
        event_dates(GATEWAY_1_EVENT, {
            "gateway-1-first-predicted": ("FIRST", "first"),
            "gateway-1-current-predicted": ("current", "last"),
            "gateway-1-actual": ("actual-date", "last"),
        }),
        on=["organisation", "plan"],
        how="left",
    ).merge(
        event_dates(ADOPTION_EVENT, {"adoption-current-predicted": ("current", "last")}),
        on=["organisation", "plan"],
        how="left",
    )

    adoption = pd.to_datetime(durations["adoption-current-predicted"])
    for start in ["gateway-1-actual", "gateway-1-first-predicted"]:
        gateway_1 = pd.to_datetime(durations[start])
        days = (adoption - gateway_1).dt.days.astype("Int64")
        durations[f"days-{start}-to-adoption"] = days
        within = (adoption <= gateway_1 + pd.DateOffset(months=PLAN_MAKING_MONTHS)).map({True: "yes", False: "no"})
        durations[f"{start}-within-{PLAN_MAKING_MONTHS}-months"] = within.where((days > 0).fillna(False), None)

    return durations.sort_values(["organisation-name", "plan"]).reset_index(drop=True)


def main(output_dir: str):
    logger.info("Fetching authoritative plan-timetable event-date facts...")
    facts = datasette_query_paginated("plan-timetable", EVENT_DATE_FACTS_SQL)
    logger.info(f"Loaded {len(facts)} event-date fact observations")

    resources = datasette_query_paginated("digital-land", RESOURCE_COLLECTED_SQL)
    facts = facts.merge(resources, on="resource", how="left")
    missing = facts["collected_date"].isna().sum()
    if missing:
        logger.warning(f"{missing} event-date observations have no resource collection date")

    line_facts = datasette_query_paginated("plan-timetable", LINE_FACTS_SQL)
    line_facts = (
        line_facts.pivot_table(index=["entity", "resource", "entry_date", "entry_number"], columns="field",
                               values="value", aggfunc="first")
        .reindex(columns=["actual-date", "notes"])
        .rename(columns={"actual-date": "actual_date"})
        .reset_index()
        .merge(resources, on="resource", how="left")
    )
    facts = facts.merge(
        line_facts[["entity", "resource", "entry_number", "actual_date", "notes"]],
        on=["entity", "resource", "entry_number"],
        how="left",
    )

    entities = datasette_query_paginated("plan-timetable", ENTITY_SQL)
    entities["organisation_entity"] = pd.to_numeric(entities["organisation_entity"], errors="coerce").astype("Int64")
    organisations = fetch_datasette_csv_table("organisation", low_memory=False)
    organisations["ons_code"] = ons_codes(organisations)
    organisations = organisations[["entity", "organisation", "name", "ons_code"]]
    organisations = organisations.rename(columns={"entity": "organisation_entity", "name": "organisation_name"})
    organisations["organisation_entity"] = organisations["organisation_entity"].astype("Int64")
    entities = entities.merge(organisations, on="organisation_entity", how="left")

    plans = pd.concat(
        [datasette_query_paginated(dataset, PLANS_SQL).assign(dataset=dataset) for dataset in PLAN_DATASETS],
        ignore_index=True,
    )
    plans["organisation_entity"] = pd.to_numeric(plans["organisation_entity"], errors="coerce").astype("Int64")

    authoritative = datasette_query_paginated("plan-timetable", AUTHORITATIVE_ENTITIES_SQL)
    entities = entities[entities["entity"].isin(authoritative["entity"])].copy()
    entities["plan_type"] = plan_types(entities, plans)

    reasons = exclusion_reasons(entities)
    for reason, count in reasons[reasons != ""].value_counts().items():
        logger.info(f"Excluding {count} plan events: {reason}")
    published_plans = set(zip(entities["organisation"], entities["plan"]))
    for excluded_plan in EXCLUDED_PLANS:
        if excluded_plan not in published_plans:
            logger.warning(f"Excluded plan {excluded_plan} no longer matches any plan events")

    entities = entities[reasons == ""]
    authoritative = authoritative[authoritative["entity"].isin(entities["entity"])]
    facts = facts[facts["entity"].isin(entities["entity"])]
    line_facts = line_facts[line_facts["entity"].isin(entities["entity"])]

    undated = authoritative[~authoritative["entity"].isin(facts["entity"])]
    undated = undated.merge(current_line_fields(line_facts)[["notes"]], left_on="entity", right_index=True, how="left")
    logger.info(f"{len(undated)} authoritative plan events have no event-date")

    os.makedirs(output_dir, exist_ok=True)

    entity_columns = [
        "entity", "reference", "organisation", "organisation_name", "ons_code", "plan", "plan_type", "plan_event"
    ]
    history = build_event_date_history(facts, undated).merge(entities[entity_columns], on="entity", how="left")
    history = history.merge(
        current_line_fields(line_facts)[["actual_date"]], left_on="entity", right_index=True, how="left"
    )
    # mergesort is stable, keeping the history's within-entry-date order
    history = history.sort_values(["entity", "entry_date"], kind="mergesort")[LONG_COLUMNS]
    long_file = os.path.join(output_dir, LONG_OUTPUT_FILE)
    history.to_csv(long_file, index=False)
    logger.info(
        f"Saved {long_file}: {len(history)} rows, {history['entity'].nunique()} plan events, "
        f"{history.loc[history['status'] == 'changed', 'entity'].nunique()} with a changed event-date, "
        f"{(history['status'] == NO_EVENT_DATE).sum()} with no event-date"
    )

    timetable = build_event_date_timetable(facts, line_facts, undated).merge(
        entities[entity_columns].rename(columns=lambda column: column.replace("_", "-")),
        on="entity",
        how="left",
    ).sort_values("entity")
    timetable = timetable[
        [column.replace("_", "-") for column in entity_columns]
        + timetable_date_columns(timetable)
        + ["entry-date", "actual-date", "notes"]
    ]
    wide_file = os.path.join(output_dir, WIDE_OUTPUT_FILE)
    timetable.to_csv(wide_file, index=False)
    logger.info(
        f"Saved {wide_file}: {len(timetable)} plan events, "
        f"{timetable['REVISION 1'].notna().sum()} with a revised event-date"
    )

    durations = build_plan_durations(timetable)
    durations_file = os.path.join(output_dir, PLAN_DURATION_OUTPUT_FILE)
    durations.to_csv(durations_file, index=False)
    logger.info(f"Saved {durations_file}: {len(durations)} plans")


def parse_args():
    parser = argparse.ArgumentParser(description="Event-date history for authoritative plan-timetable entities")
    parser.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Output directory for CSV files",
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    main(args.output_dir)
