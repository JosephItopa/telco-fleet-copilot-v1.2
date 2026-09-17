"""Fleet Copilot: Kubernetes fleet metrics scanner that publishes to Kafka."""

from .models import AppMetric, AppRef

__all__ = ["AppMetric", "AppRef", "__version__"]

__version__ = "1.2.0"
