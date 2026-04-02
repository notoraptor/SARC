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
def test_loki_logging_handler(httpserver):
    loki_url = httpserver.url_for("/otlp/v1/logs")
    log_conf = config().logging
    log_conf.OTLP_endpoint = loki_url

    ot_handler = getOpenTelemetryLoggingHandler(log_conf)

    assert len(httpserver.log) == 0

    ot_handler.emit(
        logging.LogRecord(
            "test_ERROR", logging.ERROR, "test_ERROR", 0, "test ERROR log", (), None
        )
    )
    wait_for_logs(httpserver, 1)

    ot_handler.emit(
        logging.LogRecord(
            "test_WARNING",
            logging.WARNING,
            "test_WARNING",
            0,
            "test WARNING log",
            (),
            None,
        )
    )
    wait_for_logs(httpserver, 2)
