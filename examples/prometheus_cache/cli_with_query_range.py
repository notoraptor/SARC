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
