# test logging to loki service

import logging
import time

import pytest
from opentelemetry._logs import get_logger_provider
from opentelemetry.sdk._logs import LoggingHandler
from pytest_httpserver import HTTPServer

from sarc.config import config
from sarc.logging import setupLogging


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
def test_setup_logging_configures_opentelemetry(httpserver):
    loki_url = httpserver.url_for("/otlp/v1/logs")
    config().logging.OTLP_endpoint = loki_url

    root_logger = logging.getLogger()
    original_handlers = root_logger.handlers[:]
    original_level = root_logger.level

    try:
        setupLogging()

        # Verify setupLogging installed an OpenTelemetry handler on root logger
        ot_handlers = [
            h for h in root_logger.handlers if isinstance(h, LoggingHandler)
        ]
        assert len(ot_handlers) == 1

        assert len(httpserver.log) == 0

        logger = logging.getLogger("test_loki")
        logger.error("test ERROR log")
        wait_for_logs(httpserver, 1)

        logger.warning("test WARNING log")
        wait_for_logs(httpserver, 2)
    finally:
        get_logger_provider().shutdown()
        root_logger.handlers = original_handlers
        root_logger.setLevel(original_level)
