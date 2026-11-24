import pandas as pd
import pytest

from plan_timetable_event_date_history import EXCLUDED_PLANS
from plan_timetable_event_date_history import build_event_date_history
from plan_timetable_event_date_history import build_event_date_timetable as build_timetable
from plan_timetable_event_date_history import build_plan_durations, exclusion_reasons, ons_codes, plan_types


def facts(rows, notes=None, actual_dates=None):
    df = pd.DataFrame(rows, columns=["entity", "entry_date", "entry_number", "collected_date", "event_date"])
    # each collection date stands for a separate resource
    df["resource"] = df["collected_date"]
    df["actual_date"] = actual_dates
    df["notes"] = notes
    return df


def build_event_date_timetable(facts, line_facts=None):
    """Lines in facts carry their own notes/actual-date unless separate line facts are given."""
    return build_timetable(facts, facts if line_facts is None else line_facts)


def history_values(history, column):
    return history[column].fillna("").tolist()


def timetable_dates(timetable, entity=1):
    row = timetable.set_index("entity").loc[entity]
    columns = [column for column in timetable.columns if column == "FIRST" or column.startswith("REVISION ")]
    return row[columns].fillna("").tolist()


def test_unchanged_event_date_has_only_first():
    """Re-collecting the same value in later resources must not add revisions."""
    timetable = build_event_date_timetable(facts([
        (1, "2026-01-01", 1, "2026-01-02", "2027-06-01"),
        (1, "2026-01-01", 1, "2026-02-02", "2027-06-01"),
        (1, "2026-03-01", 1, "2026-03-02", "2027-06-01"),
    ]))

    assert len(timetable) == 1
    assert timetable_dates(timetable) == ["2027-06-01", "", ""]
    assert timetable.loc[0, "entry-date"] == "2026-03-01"


def test_always_has_at_least_two_revision_columns():
    timetable = build_event_date_timetable(facts([
        (1, "2026-01-01", 1, "2026-01-02", "2027-06-01"),
    ]))

    assert [c for c in timetable.columns if c.startswith("REVISION ")] == ["REVISION 1", "REVISION 2"]


def test_each_change_is_a_revision_and_reverting_counts():
    timetable = build_event_date_timetable(facts([
        (1, "2026-01-01", 1, "2026-01-02", "2027-06-01"),
        (1, "2026-02-01", 1, "2026-02-02", "2027-09-01"),
        (1, "2026-03-01", 1, "2026-03-02", "2027-06-01"),
        (1, "2026-04-01", 1, "2026-04-02", "2027-08-01"),
    ]))

    assert timetable_dates(timetable) == ["2027-06-01", "2027-09-01", "2027-06-01", "2027-08-01"]


def test_revision_columns_grow_to_the_most_revised_entity():
    timetable = build_event_date_timetable(facts([
        (1, "2026-01-01", 1, "2026-01-02", "2027-06-01"),
        (2, "2026-01-01", 1, "2026-01-02", "2027-01-01"),
        (2, "2026-02-01", 1, "2026-02-02", "2027-02-01"),
        (2, "2026-03-01", 1, "2026-03-02", "2027-03-01"),
        (2, "2026-04-01", 1, "2026-04-02", "2027-04-01"),
    ]))

    assert timetable_dates(timetable, 1) == ["2027-06-01", "", "", ""]
    assert timetable_dates(timetable, 2) == ["2027-01-01", "2027-02-01", "2027-03-01", "2027-04-01"]


def test_ordered_by_entry_date_not_collection_date():
    """The platform picks an entity's current value by latest entry date, so a resource collected
    later but carrying an older entry date comes earlier in the timetable."""
    timetable = build_event_date_timetable(facts([
        (1, "2026-06-03", 2, "2026-08-05", "2026-06-03"),
        (1, "2026-03-06", 1, "2026-09-17", "2026-03-06"),
    ]))

    assert timetable_dates(timetable) == ["2026-03-06", "2026-06-03", ""]


def test_same_entry_date_changes_are_ordered_by_collection_date():
    """Publishers have changed an event-date without updating its entry-date, so the collection
    date decides which value came first."""
    timetable = build_event_date_timetable(facts([
        (1, "2026-04-17", 11, "2026-08-19", "2027-03-31"),
        (1, "2026-04-17", 9, "2026-04-29", "2026-03-31"),
        (1, "2026-04-17", 11, "2026-10-24", "2027-03-31"),
    ]))

    assert timetable_dates(timetable) == ["2026-03-31", "2027-03-31", ""]


def test_same_resource_with_two_values_keeps_the_later_entry_date_then_later_line():
    """A publisher's file with two lines for one entity mustn't flip between them every upload."""
    timetable = build_event_date_timetable(facts([
        (1, "2026-06-25", 5, "2026-06-26", "2026-08-17"),
        (1, "2026-06-25", 7, "2026-06-26", "2026-07-20"),
        (1, "2026-06-25", 5, "2026-08-04", "2026-08-17"),
        (1, "2026-07-20", 7, "2026-08-04", "2026-07-20"),
        (1, "2026-08-18", 5, "2026-08-26", "2026-08-17"),
        (1, "2026-07-20", 7, "2026-08-26", "2026-07-20"),
    ]))

    assert timetable_dates(timetable) == ["2026-07-20", "2026-08-17", ""]


def test_change_and_change_back_under_the_same_entry_date_are_both_revisions():
    """The latest revision must be the value the platform currently shows, even when a bad upload
    is corrected without the entry-date changing."""
    timetable = build_event_date_timetable(facts([
        (1, "2026-06-29", 5, "2026-07-01", "2027-07-01"),
        (1, "2026-06-29", 5, "2026-09-09", "2026-07-27"),
        (1, "2026-06-29", 5, "2026-09-10", "2027-07-01"),
    ]))

    assert timetable_dates(timetable) == ["2027-07-01", "2026-07-27", "2027-07-01"]


def test_entry_date_actual_date_and_notes_are_the_latest_non_blank_values():
    """Matches the platform, which keeps a note when a later upload leaves it blank."""
    timetable = build_event_date_timetable(facts(
        [
            (1, "2026-05-29", 4, "2026-06-01", "2026-10-01"),
            (1, "2026-09-29", 4, "2026-10-01", "2026-10-29"),
        ],
        notes=["1st of the month is a placeholder", None],
        actual_dates=[None, "2026-10-29"],
    ))

    row = timetable.iloc[0]
    assert row["entry-date"] == "2026-09-29"
    assert row["actual-date"] == "2026-10-29"
    assert row["notes"] == "1st of the month is a placeholder"


def test_current_notes_can_come_from_a_line_without_an_event_date():
    """The platform takes notes from any line, so the timetable must too."""
    event_dates = facts([(1, "2026-03-30", 20, "2026-04-01", "2026-09-25")])
    line_facts = facts(
        [(1, "2026-03-30", 21, "2026-04-02", None)],
        notes=["Placeholder to help the authority provide their data"],
    )

    timetable = build_event_date_timetable(event_dates, line_facts)

    assert timetable.loc[0, "notes"] == "Placeholder to help the authority provide their data"


def test_empty_input_returns_empty_timetable():
    timetable = build_event_date_timetable(facts([]))

    assert timetable.empty
    assert {"FIRST", "REVISION 1", "REVISION 2", "entry-date", "actual-date", "notes"} <= set(timetable.columns)


# long layout


def test_long_unchanged_event_date_is_a_single_first_recorded_row():
    history = build_event_date_history(facts([
        (1, "2026-01-01", 1, "2026-01-02", "2027-06-01"),
        (1, "2026-03-01", 1, "2026-03-02", "2027-06-01"),
    ]))

    assert len(history) == 1
    row = history.iloc[0]
    assert row["direction"] == "first recorded"
    assert row["status"] == "unchanged"
    assert row["entry_date"] == "2026-01-01"
    assert pd.isna(row["previous_event_date"])
    assert pd.isna(row["days_moved"])


def test_long_changes_produce_one_row_each_with_direction_and_days_moved():
    history = build_event_date_history(facts([
        (1, "2026-01-01", 1, "2026-01-02", "2027-06-01"),
        (1, "2026-02-01", 1, "2026-02-02", "2027-09-01"),
        (1, "2026-03-01", 1, "2026-03-02", "2027-08-01"),
    ]))

    assert history_values(history, "previous_event_date") == ["", "2027-06-01", "2027-09-01"]
    assert history_values(history, "new_event_date") == ["2027-06-01", "2027-09-01", "2027-08-01"]
    assert history_values(history, "direction") == ["first recorded", "moved later", "moved earlier"]
    assert history["days_moved"].tolist() == [pd.NA, 92, -31]
    assert set(history["status"]) == {"changed"}


def test_long_status_is_per_entity():
    history = build_event_date_history(facts([
        (2, "2026-02-01", 1, "2026-02-02", "2027-01-01"),
        (1, "2026-02-01", 1, "2026-02-02", "2027-09-01"),
        (1, "2026-01-01", 1, "2026-01-02", "2027-06-01"),
    ]))

    assert history["entity"].tolist() == [1, 1, 2]
    assert history["status"].tolist() == ["changed", "changed", "unchanged"]


def test_long_notes_come_from_the_upload_where_each_event_date_first_appeared():
    history = build_event_date_history(facts(
        [
            (1, "2026-07-01", 3, "2026-07-02", "2026-07-24"),
            (1, "2026-07-01", 3, "2026-07-20", "2026-07-24"),
            (1, "2026-07-24", 3, "2026-08-04", "2026-08-09"),
            (1, "2026-07-24", 3, "2026-08-19", "2026-08-09"),
        ],
        notes=["Consultation stage", "Consultation stage", "Extended by 2 weeks", "Later wording"],
    ))

    assert history_values(history, "notes") == ["Consultation stage", "Extended by 2 weeks"]


def test_long_empty_input_returns_empty_history():
    history = build_event_date_history(facts([]))

    assert history.empty
    assert "status" in history.columns


# plan events with no event-date


def test_undated_plan_events_appear_in_the_wide_layout_with_blank_dates():
    event_dates = facts([(1, "2026-05-14", 1, "2026-05-15", "2027-06-01")])
    line_facts = facts([(2, "2026-05-14", 2, "2026-05-15", None)], notes=["Only if needed"])
    undated = pd.DataFrame({"entity": [2], "entry_date": ["2026-05-14"]})

    timetable = build_timetable(event_dates, line_facts, undated)

    assert timetable["entity"].tolist() == [1, 2]
    assert timetable_dates(timetable, 2) == ["", "", ""]
    row = timetable.set_index("entity").loc[2]
    assert row["entry-date"] == "2026-05-14"
    assert row["notes"] == "Only if needed"


def test_undated_plan_events_get_a_single_no_event_date_row_in_the_long_layout():
    undated = pd.DataFrame({"entity": [2], "entry_date": ["2026-05-14"], "notes": ["Only if needed"]})

    history = build_event_date_history(facts([(1, "2026-01-01", 1, "2026-01-02", "2027-06-01")]), undated)

    row = history.set_index("entity").loc[2]
    assert row["status"] == "no event-date"
    assert row["entry_date"] == "2026-05-14"
    assert row["notes"] == "Only if needed"
    assert pd.isna(row["new_event_date"]) and pd.isna(row["direction"])
    assert history.set_index("entity").loc[1, "status"] == "unchanged"


# ONS codes


def test_ons_codes_for_joint_planning_groups_are_their_members_codes():
    organisations = pd.DataFrame(
        [
            ("local-authority:AAA", "E07000001", None),
            ("local-authority:BBB", "E07000002", None),
            ("local-authority:CCC", None, None),
            ("local-planning-group:AAA-BBB", None, "local-authority:AAA;local-authority:BBB;local-authority:CCC"),
        ],
        columns=["organisation", "statistical_geography", "organisations"],
    )

    assert ons_codes(organisations).tolist() == ["E07000001", "E07000002", None, "E07000001;E07000002"]


# plan types


def plan_type(plan, organisation_entity=1):
    plans = pd.DataFrame(
        [
            ("new-local-plan", "New Local Plan", 1, "local-plan"),
            ("combined-plan", "Combined Plan", 1, "waste-plan"),
            ("combined-plan", "Combined Plan", 1, "local-plan"),
            ("M01", "Minerals Plan", 1, "minerals-plan"),
            ("joint-plan", None, 2, "local-plan"),
        ],
        columns=["reference", "name", "organisation_entity", "dataset"],
    )
    entities = pd.DataFrame({"plan": [plan], "organisation_entity": [organisation_entity]})
    return plan_types(entities, plans).iloc[0]


@pytest.mark.parametrize("plan, expected", [
    ("new-local-plan", "local-plan"),
    ("New Local_Plan", "local-plan"),
    ("Minerals Plan", "minerals-plan"),
    ("combined-plan", "local-plan;waste-plan"),
    ("joint-plan", "local-plan"),
    ("a-council-local-plan-2044", "local-plan (inferred)"),
    ("A Council Local Plan", "local-plan (inferred)"),
    ("minerals-and-waste-local-plan", "unknown"),
    ("Mineral Local Plan", "unknown"),
    ("MWLP", "unknown"),
    ("something-else", "unknown"),
    (None, "unknown"),
])
def test_plan_type(plan, expected):
    assert plan_type(plan) == expected


def test_plan_names_only_match_within_the_same_organisation():
    """Plan names such as "Local Plan" aren't unique to one organisation."""
    assert plan_type("Minerals Plan", organisation_entity=2) == "unknown"


# exclusions


@pytest.mark.parametrize("organisation, ons_code, plan, plan_type, excluded", [
    ("local-authority:AAA", "E07000001", "a-plan", "local-plan", ""),
    ("local-authority:AAA", "E07000001", "a-plan", "unknown", ""),
    ("local-authority:AAA", "E07000001", "a-local-plan", "local-plan (inferred)", ""),
    ("local-authority:AAA", "E07000001", "a-plan", "local-plan;minerals-plan;waste-plan", ""),
    ("local-authority:AAA", "E07000001", "MWLP", "unknown", ""),
    ("local-authority:AAA", None, "a-plan", "unknown", ""),
    ("local-planning-group:AAA-BBB", "E10000001;E06000001", "a-plan", "local-plan", ""),
    ("local-authority:AAA", "E06000001", "a-plan", "minerals-plan;waste-plan", "not a local plan"),
    ("local-authority:AAA", "E07000001", "a-plan", "supplementary-plan", "not a local plan"),
    ("local-authority:CTY", "E10000001", "a-plan", "unknown", "county council"),
    ("local-authority:CTY", "E10000001", "a-local-plan", "local-plan (inferred)", "county council"),
    ("government-organisation:D1342", None, "a-plan", "local-plan", "excluded organisation"),
    (EXCLUDED_PLANS[0][0], "E06000001", EXCLUDED_PLANS[0][1], "local-plan", "excluded plan"),
])
def test_exclusion_reason(organisation, ons_code, plan, plan_type, excluded):
    entities = pd.DataFrame(
        [(organisation, ons_code, plan, plan_type)], columns=["organisation", "ons_code", "plan", "plan_type"]
    )

    assert exclusion_reasons(entities).iloc[0] == excluded


# plan durations


def plan_durations(rows):
    """rows are entity, plan-event, FIRST, REVISION 1, actual-date, with optional plan and entry-date."""
    timetable = pd.DataFrame([
        {
            "entity": row[0], "organisation": "local-authority:AAA", "organisation-name": "A Council",
            "ons-code": "E07000001", "plan": row[5] if len(row) > 5 else "a-plan", "plan-type": "local-plan",
            "plan-event": row[1], "FIRST": row[2], "REVISION 1": row[3], "REVISION 2": None,
            "entry-date": row[6] if len(row) > 6 else "2026-01-01", "actual-date": row[4],
        }
        for row in rows
    ])
    return build_plan_durations(timetable)


def test_plan_durations_use_first_and_actual_gateway_1_and_current_adoption():
    """Moving Gateway 1 later mustn't hide a plan taking more than 30 months since it was first predicted."""
    row = plan_durations([
        (1, "gateway-1-self-assessment", "2026-01-01", "2026-06-01", "2026-06-10"),
        (2, "adopted", "2028-06-01", "2028-12-01", None),
        (3, "gateway-2-advice-sought", "2027-01-01", None, None),
    ]).iloc[0]

    assert row["gateway-1-first-predicted"] == "2026-01-01"
    assert row["gateway-1-current-predicted"] == "2026-06-01"
    assert row["gateway-1-actual"] == "2026-06-10"
    assert row["adoption-current-predicted"] == "2028-12-01"
    assert row["days-gateway-1-actual-to-adoption"] == 905
    assert row["gateway-1-actual-within-30-months"] == "yes"
    assert row["days-gateway-1-first-predicted-to-adoption"] == 1065
    assert row["gateway-1-first-predicted-within-30-months"] == "no"


@pytest.mark.parametrize("adoption, within", [("2028-07-31", "yes"), ("2028-08-01", "no")])
def test_within_30_months_is_counted_in_calendar_months(adoption, within):
    row = plan_durations([
        (1, "gateway-1-self-assessment", "2026-01-31", None, None),
        (2, "adopted", adoption, None, None),
    ]).iloc[0]

    assert row["gateway-1-first-predicted-within-30-months"] == within


def test_plan_durations_are_blank_without_both_dates():
    durations = plan_durations([
        (1, "gateway-1-self-assessment", "2026-01-01", None, None, "no-actual-date"),
        (2, "adopted", "2028-01-01", None, None, "no-actual-date"),
        (3, "adopted", "2028-01-01", None, None, "no-gateway-1"),
        (4, "gateway-1-self-assessment", "2026-01-01", None, None, "no-adoption"),
    ]).set_index("plan")

    assert pd.isna(durations.loc["no-actual-date", "days-gateway-1-actual-to-adoption"])
    assert pd.isna(durations.loc["no-actual-date", "gateway-1-actual-within-30-months"])
    assert durations.loc["no-actual-date", "days-gateway-1-first-predicted-to-adoption"] == 730
    for plan in ["no-gateway-1", "no-adoption"]:
        assert pd.isna(durations.loc[plan, "days-gateway-1-first-predicted-to-adoption"])
        assert pd.isna(durations.loc[plan, "gateway-1-first-predicted-within-30-months"])


def test_within_30_months_is_blank_when_adoption_is_not_after_gateway_1():
    row = plan_durations([
        (1, "gateway-1-self-assessment", "2026-04-28", None, None),
        (2, "adopted", "2026-04-28", None, None),
    ]).iloc[0]

    assert row["days-gateway-1-first-predicted-to-adoption"] == 0
    assert pd.isna(row["gateway-1-first-predicted-within-30-months"])


def test_event_with_a_new_reference_keeps_the_oldest_first_prediction():
    durations = plan_durations([
        (9, "gateway-1-self-assessment", "2026-09-01", None, None, "a-plan", "2026-08-01"),
        (1, "gateway-1-self-assessment", "2026-01-01", None, None, "a-plan", "2026-02-01"),
        (2, "adopted", "2028-06-01", None, None),
    ])

    assert len(durations) == 1
    assert durations.loc[0, "gateway-1-first-predicted"] == "2026-01-01"
    assert durations.loc[0, "gateway-1-current-predicted"] == "2026-09-01"
