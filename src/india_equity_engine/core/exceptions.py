"""Project-specific exceptions."""


class EngineError(Exception):
    """Base exception for engine failures."""


class ConfigError(EngineError):
    """Configuration is missing or invalid."""


class ConnectorError(EngineError):
    """A source connector failed."""


class StorageError(EngineError):
    """A storage operation failed."""
