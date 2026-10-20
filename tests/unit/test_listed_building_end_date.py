import pandas as pd
import pytest

import listed_building_end_date


def test_main_raises_when_organisation_data_cannot_be_loaded(monkeypatch, tmp_path):
    """Without organisation data the output cannot be built, so the original error must surface
    rather than a KeyError on the missing organisation column."""
    listed_building = pd.DataFrame({"reference": [1], "end-date": ["2026-01-01"]})
    listed_building_outline = pd.DataFrame(
        {"listed-building": [1], "reference": ["LBO-1"], "entity": [100], "organisation-entity": [7]}
    )
    monkeypatch.setattr(
        listed_building_end_date,
        "read_csv_with_retry",
        lambda url, **kwargs: listed_building if "outline" not in url else listed_building_outline,
    )

    def fail_to_fetch(*args, **kwargs):
        raise ConnectionError("datasette unavailable")

    monkeypatch.setattr(listed_building_end_date, "fetch_datasette_csv_table", fail_to_fetch)

    with pytest.raises(ConnectionError, match="datasette unavailable"):
        listed_building_end_date.main(str(tmp_path))
