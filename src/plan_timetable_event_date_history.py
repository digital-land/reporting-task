"""
Builds an event-date history for every plan-timetable entity from authoritative data, so users
can track slippage (dates moving later) and confirm which plan events have held steady.

A plan-timetable entity can be given several event-date values over time. Each value is a fact,
and fact_resource records every resource (snapshot of the publisher's data) the fact appeared in,
with the entry date it was given. The report replays those facts in order and writes the same
history in two layouts, so users can pick whichever suits them:

- long: one row per entity's first recorded event-date, plus one row per change
- wide: one row per entity - the FIRST event-date, then a REVISION column for each change
"""

import argparse
import logging
import os

import pandas as pd

from utils import datasette_query_paginated, fetch_datasette_csv_table

logger = logging.getLogger(__name__)

LONG_OUTPUT_FILE = "plan_timetable_event_date_history_long.csv"
WIDE_OUTPUT_FILE = "plan_timetable_event_date_history_wide.csv"

# fact_resource.priority is 2 when the resource came from the organisation that owns the entity,
# and 1 (the default) otherwise - so priority 2 is the authoritative signal. It lines up with
# entity.quality: every priority 2 event-date fact belongs to an "authoritative" entity.
AUTHORITATIVE_PRIORITY = 2

# MHCLG owns plan-timetable entities it published on behalf of authorities, so its data passes
# the authoritative filter above but isn't an authority's own timetable - left out of both reports
EXCLUDED_ORGANISATIONS = ["government-organisation:D1342"]

# the wide layout always shows at least this many REVISION columns, adding more when an entity
# has been revised more often
MIN_REVISION_COLUMNS = 2

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
    "plan",
    "plan_event",
    "entry_date",
    "previous_event_date",
    "new_event_date",
    "direction",
    "days_moved",
    "status",
    "notes",
]

FACT_ORDER = ["entity", "entry_date", "collected_date", "entry_number"]

NO_EVENT_DATE = "no event-date"


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
    organisations = fetch_datasette_csv_table("organisation", low_memory=False)[["entity", "organisation", "name"]]
    organisations = organisations.rename(columns={"entity": "organisation_entity", "name": "organisation_name"})
    organisations["organisation_entity"] = organisations["organisation_entity"].astype("Int64")
    entities = entities.merge(organisations, on="organisation_entity", how="left")

    authoritative = datasette_query_paginated("plan-timetable", AUTHORITATIVE_ENTITIES_SQL)
    excluded_entities = entities.loc[entities["organisation"].isin(EXCLUDED_ORGANISATIONS), "entity"]
    logger.info(
        f"Excluding {authoritative['entity'].isin(excluded_entities).sum()} plan events owned by "
        f"{EXCLUDED_ORGANISATIONS}"
    )
    authoritative = authoritative[~authoritative["entity"].isin(excluded_entities)]
    facts = facts[~facts["entity"].isin(excluded_entities)]
    line_facts = line_facts[~line_facts["entity"].isin(excluded_entities)]

    undated = authoritative[~authoritative["entity"].isin(facts["entity"])]
    undated = undated.merge(current_line_fields(line_facts)[["notes"]], left_on="entity", right_index=True, how="left")
    logger.info(f"{len(undated)} authoritative plan events have no event-date")

    os.makedirs(output_dir, exist_ok=True)

    history = build_event_date_history(facts, undated).merge(
        entities[["entity", "reference", "organisation", "organisation_name", "plan", "plan_event"]],
        on="entity",
        how="left",
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
        entities[["entity", "reference", "organisation", "organisation_name", "plan", "plan_event"]].rename(
            columns={"organisation_name": "organisation-name", "plan_event": "plan-event"}
        ),
        on="entity",
        how="left",
    ).sort_values("entity")
    revisions = [column for column in timetable.columns if column == "FIRST" or column.startswith("REVISION ")]
    timetable = timetable[
        ["entity", "reference", "organisation", "organisation-name", "plan", "plan-event"]
        + revisions
        + ["entry-date", "actual-date", "notes"]
    ]
    wide_file = os.path.join(output_dir, WIDE_OUTPUT_FILE)
    timetable.to_csv(wide_file, index=False)
    logger.info(
        f"Saved {wide_file}: {len(timetable)} plan events, "
        f"{timetable['REVISION 1'].notna().sum()} with a revised event-date"
    )


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
