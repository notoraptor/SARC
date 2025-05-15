from unittest import mock

from check_many_metrics import main
from series_with_query_range import _get_job_time_series_data_using_query_range


def patched_main():
    with mock.patch(
        "sarc.jobs.series._get_job_time_series_data",
        new=_get_job_time_series_data_using_query_range,
    ):
        main()


if __name__ == "__main__":
    patched_main()
