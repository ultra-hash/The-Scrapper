class ScrapperError(Exception):
    """Base exception for the scrapper framework."""


class PluginLoadError(ScrapperError):
    pass


class SchemaNotFoundError(ScrapperError):
    pass


class ValidationFailedError(ScrapperError):
    pass


class CircuitOpenError(ScrapperError):
    pass

class ScrapperError(Exception):
    """Base exception for scraper errors."""


class RetryableError(ScrapperError):
    """Errors that can be retried (network, transient HTTP)."""


class FatalError(ScrapperError):
    """Non-recoverable errors."""


class ValidationError(ScrapperError):
    """Raised when validation fails."""


class QuarantineError(ScrapperError):
    """Raised when an item is quarantined."""


class CircuitOpenError(ScrapperError):
    """Raised when a circuit breaker is open."""

