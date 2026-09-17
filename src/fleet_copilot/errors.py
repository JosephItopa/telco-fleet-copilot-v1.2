"""Domain errors for the fleet scanner."""

from __future__ import annotations


class FleetCopilotError(RuntimeError):
    """Base class for all service errors."""


class ConfigError(FleetCopilotError):
    """Raised when configuration is missing or invalid."""


class CollectionError(FleetCopilotError):
    """Raised when a cluster or Prometheus scan fails irrecoverably."""
