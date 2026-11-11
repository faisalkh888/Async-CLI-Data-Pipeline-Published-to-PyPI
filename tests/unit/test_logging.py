import logging

from datapipe.cli import _configured_secrets
from datapipe.config.schema import PipelineConfig
from datapipe.observability.logging import SecretRedactionFilter


def test_secret_filter_redacts_configured_environment_secret(caplog, monkeypatch, tmp_path):
    secret = "example-token-do-not-log"
    monkeypatch.setenv("DATAPIPE_TEST_TOKEN", secret)
    config = PipelineConfig.model_validate(
        {
            "pipeline": {"name": "redaction", "output_dir": str(tmp_path / "output")},
            "sources": [
                {
                    "name": "api",
                    "type": "http",
                    "url": "https://example.test/records",
                    "auth_env": "DATAPIPE_TEST_TOKEN",
                }
            ],
            "sinks": [{"name": "out", "type": "jsonl", "path": "out.jsonl"}],
        }
    )
    logger = logging.getLogger("datapipe.test.secret-redaction")
    caplog.set_level(logging.ERROR, logger=logger.name)
    redactor = SecretRedactionFilter(_configured_secrets(config))
    caplog.handler.addFilter(redactor)
    try:
        logger.error("authorization=%s", secret)
        try:
            raise RuntimeError(f"request failed with credential {secret}")
        except RuntimeError:
            logger.exception("HTTP source failed")
    finally:
        caplog.handler.removeFilter(redactor)

    assert secret not in caplog.text
    assert caplog.text.count("[REDACTED]") >= 2
