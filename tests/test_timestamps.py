import datetime
import warnings

import pandas as pd
import pytest

from caveclient.materializationengine import convert_timestamp
from caveclient.timestamps import to_utc

UTC = datetime.timezone.utc


class TestToUtc:
    def test_naive_datetime_assumed_utc(self):
        assert to_utc(datetime.datetime(2024, 1, 1, 12)) == datetime.datetime(
            2024, 1, 1, 12, tzinfo=UTC
        )

    def test_aware_datetime_converted_to_utc(self):
        est = datetime.timezone(datetime.timedelta(hours=-5))
        assert to_utc(
            datetime.datetime(2024, 1, 1, 7, tzinfo=est)
        ) == datetime.datetime(2024, 1, 1, 12, tzinfo=UTC)

    def test_epoch_seconds_are_utc(self):
        assert to_utc(0) == datetime.datetime(1970, 1, 1, tzinfo=UTC)
        assert to_utc(86400.5) == datetime.datetime(
            1970, 1, 2, 0, 0, 0, 500000, tzinfo=UTC
        )

    def test_iso_string_with_microseconds(self):
        assert to_utc("2020-09-01T08:10:01.497934") == datetime.datetime(
            2020, 9, 1, 8, 10, 1, 497934, tzinfo=UTC
        )

    def test_iso_string_without_microseconds_and_z_suffix(self):
        assert to_utc("2020-09-01T08:10:01Z") == datetime.datetime(
            2020, 9, 1, 8, 10, 1, tzinfo=UTC
        )

    def test_now(self):
        assert abs((to_utc("now") - datetime.datetime.now(UTC)).total_seconds()) < 5

    def test_unparseable_string_raises_value_error(self):
        with pytest.raises(ValueError):
            to_utc("not a timestamp")

    def test_unsupported_type_raises_type_error(self):
        with pytest.raises(TypeError):
            to_utc([1, 2, 3])


def test_convert_timestamp_none_returns_max():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        assert convert_timestamp(None) == pd.Timestamp.max.to_pydatetime()
