# test logging to loki service

import logging

import gifnoc
import pytest
from opentelemetry._logs import get_logger_provider
from opentelemetry.sdk._logs import LoggingHandler
from pytest_httpserver import HTTPServer

from sarc.logging import setupLogging


@pytest.fixture
def httpserver(httpserver: HTTPServer):
    httpserver.expect_request("/otlp/v1/logs", method="POST").respond_with_json(
        {"status": "success", "data": {"result": []}}
    )
    return httpserver


@pytest.fixture
def config_with_logging(httpserver):
    with gifnoc.overlay(
        {
            "sarc": {
                "logging": {
                    "OTLP_endpoint": httpserver.url_for("/otlp/v1/logs"),
                    "service_name": "sarc-test",
                    "log_level": "WARNING",
                }
            }
        }
    ):
        yield


def test_setup_logging_configures_opentelemetry(httpserver, config_with_logging):
    root_logger = logging.getLogger()
    original_handlers = root_logger.handlers[:]
    original_level = root_logger.level

    try:
        # Prevent recursive logging: werkzeug logs each HTTP request it receives,
        # which would be sent to the OT handler on the root logger, triggering
        # another HTTP request, and so on infinitely.
        logging.getLogger("werkzeug").propagate = False

        setupLogging()
        provider = get_logger_provider()

        # Verify setupLogging installed an OpenTelemetry handler on root logger
        ot_handlers = [h for h in root_logger.handlers if isinstance(h, LoggingHandler)]
        assert len(ot_handlers) == 1

        provider.force_flush(timeout_millis=5000)
        assert len(httpserver.log) == 0

        logger = logging.getLogger("test_loki")
        logger.error("test ERROR log")
        provider.force_flush(timeout_millis=5000)
        assert len(httpserver.log) == 1

        logger.warning("test WARNING log")
        provider.force_flush(timeout_millis=5000)
        assert len(httpserver.log) == 2
    finally:
        get_logger_provider().shutdown()
        root_logger.handlers = original_handlers
        root_logger.setLevel(original_level)
