import logging
from collections.abc import Iterable


class SecretRedactionFilter(logging.Filter):
    """Replace configured secret strings in messages and formatted exceptions."""

    def __init__(self, secrets: Iterable[str]) -> None:
        super().__init__()
        self.secrets = tuple(sorted({secret for secret in secrets if secret}, key=len, reverse=True))

    def _redact(self, value: str) -> str:
        for secret in self.secrets:
            value = value.replace(secret, "[REDACTED]")
        return value

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = self._redact(record.getMessage())
        record.args = ()
        if record.exc_info:
            record.exc_text = self._redact(logging.Formatter().formatException(record.exc_info))
            record.exc_info = None
        elif record.exc_text:
            record.exc_text = self._redact(record.exc_text)
        for name, value in tuple(record.__dict__.items()):
            if isinstance(value, str):
                setattr(record, name, self._redact(value))
        return True
