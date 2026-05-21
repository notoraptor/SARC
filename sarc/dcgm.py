"""NVIDIA DCGM sentinel values and helpers.

DCGM publishes special "BLANK" values when a metric is unavailable
(e.g. profiling paused, GPU not accessible, MIG slice not enabled). These
sentinels reach SARC via Prometheus exporters such as dcgm-exporter and
pollute statistics if not filtered before aggregation.

Constants and semantics mirror NVIDIA/DCGM ``testing/python3/dcgmvalue.py``:
https://github.com/NVIDIA/DCGM/blob/master/testing/python3/dcgmvalue.py
"""

from typing import overload

from pandas import Series

DCGM_INT64_BLANK = 0x7FFF_FFFF_FFFF_FFF0
DCGM_INT32_BLANK = 0x7FFF_FFF0
DCGM_FP64_BLANK = 140737488355328.0  # 2**47 == 0x8000_0000_0000

DCGM_FP64_NOT_FOUND = DCGM_FP64_BLANK + 1.0
DCGM_FP64_NOT_SUPPORTED = DCGM_FP64_BLANK + 2.0
DCGM_FP64_NOT_PERMISSIONED = DCGM_FP64_BLANK + 3.0


@overload
def is_blank_fp64(val: float) -> bool: ...
@overload
def is_blank_fp64(val: Series) -> Series: ...
def is_blank_fp64(val: float | Series) -> bool | Series:
    """Return True if ``val`` is a DCGM blank/error sentinel for FP64 metrics.

    Mirrors NVIDIA's ``DCGM_FP64_IS_BLANK``: any value >= ``DCGM_FP64_BLANK``
    is invalid, which also covers ``NOT_FOUND``, ``NOT_SUPPORTED`` and
    ``NOT_PERMISSIONED``. Polymorphic on scalars and Pandas Series.
    """
    return val >= DCGM_FP64_BLANK
