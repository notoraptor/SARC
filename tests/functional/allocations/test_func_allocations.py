from datetime import date

import pytest

from sarc.allocations import get_allocations

parameters = {
    "name_only": {"cluster_name": "fromage"},
    "start_only": {"cluster_name": "fromage", "start": date(2018, 4, 1)},
    "end_only": {"cluster_name": "fromage", "end": date(2019, 4, 1)},
    "start_and_end": {
        "cluster_name": "fromage",
        "start": date(2018, 4, 1),
        "end": date(2020, 4, 1),
    },
    "name_list_start_and_end": {
        "cluster_name": ["fromage", "patate"],
        "start": date(2018, 4, 1),
        "end": date(2020, 4, 1),
    },
    "another_name": {"cluster_name": "patate"},
}


@pytest.mark.usefixtures("init_db_with_allocations")
@pytest.mark.parametrize("params,", parameters.values(), ids=parameters.keys())
def test_get_allocations(params, data_regression):
    data = get_allocations(**params)

    data_regression.check(
        [allocation.json(exclude={"id": True}) for allocation in data]
    )
