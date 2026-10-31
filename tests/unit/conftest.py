import os

os.environ.setdefault("BASE_URL", "http://short.test")
os.environ.setdefault("PG_PRIMARY_DSN", "postgresql://u@db/x")
os.environ.setdefault("PG_REPLICA_DSN", "postgresql://u@db/x")
