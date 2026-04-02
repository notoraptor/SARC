# test logging to loki service

import logging
import time

import pytest
from pytest_httpserver import HTTPServer

from sarc.config import config
from sarc.logging import getOpenTelemetryLoggingHandler


def wait_for_logs(httpserver, expected, timeout=5):
    deadline = time.time() + timeout
    while len(httpserver.log) < expected:
        if time.time() > deadline:
            raise AssertionError(
                f"Expected {expected} log requests, got {len(httpserver.log)}"
            )
        time.sleep(0.05)


@pytest.fixture
def httpserver(httpserver: HTTPServer):
    httpserver.expect_request("/otlp/v1/logs", method="POST").respond_with_json(
        {"status": "success", "data": {"result": []}}
    )
    return httpserver


@pytest.mark.usefixtures("base_config_with_logging")
def test_loki_logging_full_chain(httpserver):
    loki_url = httpserver.url_for("/otlp/v1/logs")
    log_conf = config().logging
    log_conf.OTLP_endpoint = loki_url

    ot_handler = getOpenTelemetryLoggingHandler(log_conf)

    test_logger = logging.getLogger("test_loki")
    test_logger.addHandler(ot_handler)
    test_logger.setLevel(logging.DEBUG)

    assert len(httpserver.log) == 0

    test_logger.error("test ERROR log")
    wait_for_logs(httpserver, 1)

    test_logger.warning("test WARNING log")
    wait_for_logs(httpserver, 2)
