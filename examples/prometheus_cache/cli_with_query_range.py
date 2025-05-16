"""
Script to run sarc cli commands
with a version of get_job_time_series() based on Prometheus query range,
instead of default version based on Prometheus query with offset.

Prometheus query range allows to specify start and end time to find data,
while Prometheus query with offset uses an offset time relative to current time,
which we assume to be less precise.

We want to compare both implementations to check if they return (almost) same values.

QUERY WITH OFFSET VS QUERY RANGE
--------------------------------

1) Call sarc cli normally, to generate cache:
    uv run sarc -v acquire jobs ...
2) Call this script with same parameters and SARC_CACHE=check:
    SARC_CACHE=check uv run python cli_with_query_range.py -v acquire jobs ...

First call with generate cache with default get_job_time_series() based on query offset.

Second call will use a get_job_time_series() based on query range, then,
thanks to `SARC_CACHE=check`, will compare returned data with the ones
already in cache, and will raise an exception if data differs.

QUERY RANGE TESTING
-------------------

The script can also be used to test query range itself:
1) clear cache:
    rm -rf <sarc-cache-dir>/prometheus
2) Run this script once:
    uv run python cli_with_query_range.py -v acquire jobs ...
3) Run this script again with SARC_CACHE=check:
    SARC_CACHE=check uv run python cli_with_query_range.py -v acquire jobs ...

This will then compare live query range results with cached query range data,
allowing to check if query range is constant.

CURRENT REMARKS
---------------

CURRENT OBSERVATIONS:
- Query with offset and query range seems to return same results in most cases.
- Query range itself is not constant neither

CURRENT CONCLUSION:
It seems not worth to use query range, at least with current implementation.
"""

from unittest import mock

from series_with_query_range import _get_job_time_series_data_using_query_range

from sarc.cli import main


def patched_main():
    with mock.patch(
        "sarc.jobs.series._get_job_time_series_data",
        new=_get_job_time_series_data_using_query_range,
    ):
        check_mock()
        returncode = main()
        if returncode > 0:
            raise SystemExit(returncode)


def check_mock():
    from sarc.jobs.series import _get_job_time_series_data

    assert _get_job_time_series_data is _get_job_time_series_data_using_query_range


if __name__ == "__main__":
    patched_main()
