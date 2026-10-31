from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    node_id: str = "local"
    base_url: str
    pg_primary_dsn: str
    pg_replica_dsn: str
    redis_sentinels: str = ""          # "host:port,host:port"; empty -> redis_url
    redis_master_name: str = "mymaster"
    redis_url: str = "redis://redis-primary:6379/0"

    redis_timeout_s: float = 0.05      # 50 ms, then fall through to the DB
    db_read_timeout_s: float = 0.1     # read on the redirect path
    pool_size: int = 20
    redis_concurrency: int = 40
    acquire_timeout_s: float = 0.05

    l1_size: int = 10_000
    l1_ttl_s: int = 10
    l2_ttl_s: int = 86_400             # +-10% jitter applied on write
    l2_negative_ttl_s: int = 60
    l2_gone_ttl_s: int = 300
    url_blocklist: str = ""            # comma separated hosts


settings = Settings()
