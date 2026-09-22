from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "app-performance-metrics"
    kafka_acks: str = "all"
    kafka_compression: str = "lz4"
    kafka_linger_ms: int = 20
    kafka_batch_size: int = 131072
    kafka_partitions: int = 24

    collect_interval_seconds: int = 15
    kubernetes_cluster_name: str = "cluster-01"
    kubernetes_namespace: str | None = None
    prometheus_timeout_seconds: float = 5.0
    max_concurrent_scrapes: int = 50

    collector_id: str = "collector-01"
    log_level: str = "INFO"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
