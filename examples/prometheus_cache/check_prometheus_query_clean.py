from unittest import mock

from series_with_query_range import _get_job_time_series_data_using_query_range

from sarc.cli import main


def patched_main():
    with mock.patch(
        "sarc.jobs.series._get_job_time_series_data",
        new=_get_job_time_series_data_using_query_range,
    ):
        returncode = main()
        if returncode > 0:
            raise SystemExit(returncode)


if __name__ == "__main__":
    patched_main()
