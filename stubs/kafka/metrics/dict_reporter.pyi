from kafka.metrics.metrics_reporter import AbstractMetricsReporter as AbstractMetricsReporter
from typing import Any

logger: Any

class DictReporter(AbstractMetricsReporter):
    def __init__(self, prefix: str = ...) -> None: ...
    def snapshot(self): ...
    def init(self, metrics: Any) -> None: ...
    def metric_change(self, metric: Any) -> None: ...
    def metric_removal(self, metric: Any): ...
    def get_category(self, metric: Any): ...
    def configure(self, configs: Any) -> None: ...
    def close(self) -> None: ...
