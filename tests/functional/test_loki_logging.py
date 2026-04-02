# test logging to loki service

import logging

import pytest
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import SimpleLogRecordProcessor
from opentelemetry.sdk.resources import Resource
from pytest_httpserver import HTTPServer


@pytest.fixture
def httpserver(httpserver: HTTPServer):
    httpserver.expect_request("/otlp/v1/logs", method="POST").respond_with_json(
        {"status": "success", "data": {"result": []}}
    )
    return httpserver


@pytest.fixture
def ot_handler(httpserver):
    """Build an OpenTelemetry logging handler with synchronous export."""
    loki_url = httpserver.url_for("/otlp/v1/logs")
    logger_provider = LoggerProvider(
        resource=Resource.create({"service.name": "sarc-test"})
    )
    set_logger_provider(logger_provider)
    otlp_exporter = OTLPLogExporter(loki_url)
    logger_provider.add_log_record_processor(SimpleLogRecordProcessor(otlp_exporter))
    handler = LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)
    yield handler
    logger_provider.shutdown()


def test_loki_logging_handler(httpserver, ot_handler):
    ot_handler.flush()
    assert len(httpserver.log) == 0

    ot_handler.emit(
        logging.LogRecord(
            "test_ERROR", logging.ERROR, "test_ERROR", 0, "test ERROR log", (), None
        )
    )
    ot_handler.flush()
    assert len(httpserver.log) == 1

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
    ot_handler.flush()
    assert len(httpserver.log) == 2
